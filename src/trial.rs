#[cfg(test)]
mod tests {
    use std::{
        collections::{HashSet, VecDeque},
        ffi::OsString,
        fs,
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::{Duration, Instant},
    };

    use async_trait::async_trait;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    use crate::{
        db::{Db, ProjectRepository},
        environment::ProjectAdmissionLock,
        execution_policy::{
            load_or_create_policy, PolicyLoadInput, ResolvedExecutionPolicy, StartupEnvironment,
        },
        models::{MetricDirection, NewProject, ObjectiveMetric, Project},
        pueue::{
            ControlledPueueFailure, ControlledPueueResult, PueueApi, PueueControlBoundary,
            PueueTask,
        },
        trial::{
            run_with, TrialCleanupStatus, TrialManifestClass, TrialOptions, TrialTerminalClass,
        },
        AppError,
    };

    struct NoopPueue {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl PueueApi for NoopPueue {
        async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }

        async fn add(&self, _args: &[OsString]) -> Result<i64, AppError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(0)
        }

        async fn kill(&self, _task_id: i64) -> Result<(), AppError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn status_json_controlled_before(
            &self,
            _deadline: std::time::Instant,
            _cancellation: CancellationToken,
        ) -> ControlledPueueResult<Vec<PueueTask>> {
            self.record_controlled_call()
        }

        async fn add_controlled_before(
            &self,
            _args: &[OsString],
            _deadline: std::time::Instant,
            _cancellation: CancellationToken,
        ) -> ControlledPueueResult<i64> {
            self.record_controlled_call()
        }

        async fn kill_controlled_before(
            &self,
            _task_id: i64,
            _deadline: std::time::Instant,
            _cancellation: CancellationToken,
        ) -> ControlledPueueResult<()> {
            self.record_controlled_call()
        }

        async fn remove_controlled_before(
            &self,
            _task_id: i64,
            _deadline: std::time::Instant,
            _cancellation: CancellationToken,
        ) -> ControlledPueueResult<()> {
            self.record_controlled_call()
        }

        async fn group_exists_controlled_before(
            &self,
            _group: &str,
            _deadline: std::time::Instant,
            _cancellation: CancellationToken,
        ) -> ControlledPueueResult<bool> {
            self.record_controlled_call()
        }

        async fn create_group_exclusive_controlled_before(
            &self,
            _group: &str,
            _deadline: std::time::Instant,
            _cancellation: CancellationToken,
        ) -> ControlledPueueResult<()> {
            self.record_controlled_call()
        }

        async fn remove_group_controlled_before(
            &self,
            _group: &str,
            _deadline: std::time::Instant,
            _cancellation: CancellationToken,
        ) -> ControlledPueueResult<()> {
            self.record_controlled_call()
        }
    }

    impl NoopPueue {
        fn record_controlled_call<T>(&self) -> ControlledPueueResult<T> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(failure(PueueControlBoundary::TargetNotReleased))
        }
    }

    #[derive(Clone, Copy)]
    enum AddBehavior {
        Success,
        SuccessWrongId,
        NotReleased,
        ReleasedZero,
        ReleasedOne,
        Duplicate,
        CleanupUncertain,
    }

    #[derive(Clone, Copy)]
    enum ManifestBehavior {
        Valid,
        Missing,
        Invalid,
        EmptyMetrics,
    }

    impl Default for ManifestBehavior {
        fn default() -> Self {
            Self::Valid
        }
    }

    #[derive(Default)]
    struct ScriptState {
        calls: Vec<String>,
        deadline_records: Vec<(String, Instant)>,
        snapshots: Vec<Vec<PueueTask>>,
        tasks: Vec<PueueTask>,
        groups: HashSet<String>,
        task_states: VecDeque<String>,
        add_behavior: Option<AddBehavior>,
        add_command: Option<String>,
        result_path: Option<String>,
        artifact_dir: Option<String>,
        extra_task_after_terminal: bool,
        extra_task_inserted: bool,
        ambiguous_extra_group_task: bool,
        ambiguous_extra_task_inserted: bool,
        late_enqueue_after_zero_snapshot: bool,
        late_enqueue_inserted: bool,
        late_enqueue_identity: Option<(String, String)>,
        duplicate_command_after_add: bool,
        duplicate_command_inserted: bool,
        mismatch_task_after_add: bool,
        mismatch_task_applied: bool,
        substitute_output_on_terminal: bool,
        output_substituted: bool,
        artifact_limit_on_terminal: bool,
        artifact_limit_injected: bool,
        manifest_behavior: ManifestBehavior,
        manifest_metric_name: String,
        status_failure: Option<(usize, PueueControlBoundary)>,
        group_create_failure: Option<PueueControlBoundary>,
        group_remove_failure: Option<PueueControlBoundary>,
        cancel_status_number: Option<usize>,
        remove_failures: usize,
        kill_failures: usize,
    }

    #[derive(Default)]
    struct ScriptPueue {
        state: Mutex<ScriptState>,
    }

    impl ScriptPueue {
        fn with_states(states: &[&str]) -> Self {
            let mut state = ScriptState::default();
            state.task_states = states.iter().map(|state| (*state).to_owned()).collect();
            Self {
                state: Mutex::new(state),
            }
        }

        fn set_add_behavior(&self, behavior: AddBehavior) {
            self.state.lock().unwrap().add_behavior = Some(behavior);
        }

        fn add_extra_group_task_on_terminal(&self) {
            self.state.lock().unwrap().extra_task_after_terminal = true;
        }

        fn add_extra_group_task_after_ambiguous_add(&self) {
            self.state.lock().unwrap().ambiguous_extra_group_task = true;
        }

        fn enqueue_after_zero_snapshot(&self) {
            self.state.lock().unwrap().late_enqueue_after_zero_snapshot = true;
        }

        fn add_duplicate_command_after_add(&self) {
            self.state.lock().unwrap().duplicate_command_after_add = true;
        }

        fn mismatch_task_after_add(&self) {
            self.state.lock().unwrap().mismatch_task_after_add = true;
        }

        fn substitute_output_on_terminal(&self) {
            self.state.lock().unwrap().substitute_output_on_terminal = true;
        }

        fn exceed_artifact_cleanup_limit_on_terminal(&self) {
            self.state.lock().unwrap().artifact_limit_on_terminal = true;
        }

        fn set_manifest(&self, behavior: ManifestBehavior, metric_name: &str) {
            let mut state = self.state.lock().unwrap();
            state.manifest_behavior = behavior;
            state.manifest_metric_name = metric_name.to_owned();
        }

        fn cancel_on_status(&self, number: usize) {
            self.state.lock().unwrap().cancel_status_number = Some(number);
        }

        fn fail_status_on(&self, number: usize, boundary: PueueControlBoundary) {
            self.state.lock().unwrap().status_failure = Some((number, boundary));
        }

        fn fail_group_create(&self, boundary: PueueControlBoundary) {
            self.state.lock().unwrap().group_create_failure = Some(boundary);
        }

        fn fail_group_remove(&self, boundary: PueueControlBoundary) {
            self.state.lock().unwrap().group_remove_failure = Some(boundary);
        }

        fn fail_removes(&self, count: usize) {
            self.state.lock().unwrap().remove_failures = count;
        }

        fn fail_kills(&self, count: usize) {
            self.state.lock().unwrap().kill_failures = count;
        }

        fn calls(&self) -> Vec<String> {
            self.state.lock().unwrap().calls.clone()
        }

        fn snapshots(&self) -> Vec<Vec<PueueTask>> {
            self.state.lock().unwrap().snapshots.clone()
        }

        fn deadlines(&self) -> Vec<(String, Instant)> {
            self.state.lock().unwrap().deadline_records.clone()
        }

        fn tasks(&self) -> Vec<PueueTask> {
            self.state.lock().unwrap().tasks.clone()
        }

        fn added_command(&self) -> String {
            self.state.lock().unwrap().add_command.clone().unwrap()
        }
    }

    fn failure(boundary: PueueControlBoundary) -> ControlledPueueFailure {
        let error = AppError::Runtime {
            operation: "scripted Pueue failure",
        };
        match boundary {
            PueueControlBoundary::TargetNotReleased => {
                ControlledPueueFailure::target_not_released(error)
            }
            PueueControlBoundary::TargetReleasedClientQuiescent => {
                ControlledPueueFailure::target_released_client_quiescent(error)
            }
            PueueControlBoundary::CleanupUncertain => {
                ControlledPueueFailure::cleanup_uncertain(error)
            }
        }
    }

    fn check_control(
        deadline: std::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ControlledPueueFailure> {
        if cancellation.is_cancelled() || std::time::Instant::now() >= deadline {
            Err(failure(PueueControlBoundary::TargetNotReleased))
        } else {
            Ok(())
        }
    }

    fn make_task(task_id: i64, group: String, command: String, state: &str) -> PueueTask {
        PueueTask {
            id: task_id,
            group,
            command,
            state: state.to_owned(),
            enqueued_at: None,
            started_at: None,
            ended_at: None,
            result: None,
        }
    }

    fn decode_add_identity(args: &[OsString]) -> (String, String, String, String, String) {
        let group = args
            .windows(2)
            .find(|pair| pair[0] == "-g")
            .and_then(|pair| pair[1].to_str())
            .unwrap()
            .to_owned();
        let separator = args.iter().position(|argument| argument == "--").unwrap();
        let runtime_argv = &args[separator + 1..];
        let command = crate::reconcile::try_canonical_command_display_os(runtime_argv).unwrap();
        let assignment = |name: &str| -> String {
            runtime_argv
                .iter()
                .filter_map(|argument| argument.to_str())
                .find_map(|argument| argument.strip_prefix(&format!("{name}=")))
                .unwrap()
                .to_owned()
        };
        (
            group,
            command,
            assignment("PUEUE_AGENT_EXPERIMENT_ID"),
            assignment("PUEUE_AGENT_RESULT_PATH"),
            assignment("PUEUE_AGENT_ARTIFACT_DIR"),
        )
    }

    #[async_trait]
    impl PueueApi for ScriptPueue {
        async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
            Ok(self.state.lock().unwrap().tasks.clone())
        }

        async fn add(&self, _args: &[OsString]) -> Result<i64, AppError> {
            Err(AppError::Runtime {
                operation: "legacy add should not be called by trial",
            })
        }

        async fn kill(&self, _task_id: i64) -> Result<(), AppError> {
            Err(AppError::Runtime {
                operation: "legacy kill should not be called by trial",
            })
        }

        async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
            Err(AppError::Runtime {
                operation: "legacy remove should not be called by trial",
            })
        }

        async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
            Err(AppError::Runtime {
                operation: "legacy group should not be called by trial",
            })
        }

        async fn status_json_controlled_before(
            &self,
            deadline: std::time::Instant,
            cancellation: CancellationToken,
        ) -> ControlledPueueResult<Vec<PueueTask>> {
            check_control(deadline, &cancellation)?;
            let mut state = self.state.lock().unwrap();
            state.calls.push("status".to_owned());
            state.deadline_records.push(("status".to_owned(), deadline));
            let next_snapshot = state.snapshots.len() + 1;
            if state
                .status_failure
                .is_some_and(|(number, _)| number == next_snapshot)
            {
                let (_, boundary) = state.status_failure.take().unwrap();
                return Err(failure(boundary));
            }
            if !state.tasks.is_empty() {
                if let Some(next_state) = state.task_states.pop_front() {
                    if let Some(task) = state.tasks.iter_mut().find(|task| task.id == 41) {
                        task.state = next_state.clone();
                    }
                    if next_state == "done"
                        && state.extra_task_after_terminal
                        && !state.extra_task_inserted
                    {
                        let group = state.tasks[0].group.clone();
                        state.tasks.push(make_task(
                            99,
                            group,
                            "other-command".to_owned(),
                            "queued",
                        ));
                        state.extra_task_inserted = true;
                    }
                    if next_state == "done"
                        && state.substitute_output_on_terminal
                        && !state.output_substituted
                    {
                        if let Some(result_path) = state.result_path.clone() {
                            substitute_output_generation(Path::new(&result_path));
                            state.output_substituted = true;
                        }
                    }
                    if next_state == "done"
                        && state.artifact_limit_on_terminal
                        && !state.artifact_limit_injected
                    {
                        if let Some(artifact_dir) = state.artifact_dir.clone() {
                            create_excess_artifact_entries(Path::new(&artifact_dir));
                            state.artifact_limit_injected = true;
                        }
                    }
                }
            }
            if state.mismatch_task_after_add
                && !state.mismatch_task_applied
                && !state.snapshots.is_empty()
            {
                if let Some(task) = state.tasks.iter_mut().find(|task| task.id == 41) {
                    task.group = "foreign-group".to_owned();
                    state.mismatch_task_applied = true;
                }
            }
            if state.duplicate_command_after_add
                && !state.duplicate_command_inserted
                && !state.snapshots.is_empty()
            {
                if let Some(task) = state.tasks.iter().find(|task| task.id == 41).cloned() {
                    state.tasks.push(make_task(
                        42,
                        "foreign-group".to_owned(),
                        task.command,
                        "queued",
                    ));
                    state.duplicate_command_inserted = true;
                }
            }
            let snapshot_number = state.snapshots.len() + 1;
            let snapshot = state.tasks.clone();
            state.snapshots.push(snapshot.clone());
            if state.late_enqueue_after_zero_snapshot
                && !state.late_enqueue_inserted
                && snapshot_number == 2
                && snapshot.is_empty()
            {
                if let Some((group, command)) = state.late_enqueue_identity.take() {
                    state.tasks.push(make_task(77, group, command, "queued"));
                    state.late_enqueue_inserted = true;
                }
            }
            if state.cancel_status_number == Some(snapshot_number) {
                cancellation.cancel();
            }
            Ok(snapshot)
        }

        async fn add_controlled_before(
            &self,
            args: &[OsString],
            deadline: std::time::Instant,
            cancellation: CancellationToken,
        ) -> ControlledPueueResult<i64> {
            check_control(deadline, &cancellation)?;
            let behavior = {
                let mut state = self.state.lock().unwrap();
                state.calls.push("add".to_owned());
                state.deadline_records.push(("add".to_owned(), deadline));
                state.add_behavior.take().unwrap_or(AddBehavior::Success)
            };
            let (group, command, experiment_id, result_path, artifact_dir) =
                decode_add_identity(args);
            {
                let mut state = self.state.lock().unwrap();
                state.add_command = Some(command.clone());
                state.result_path = Some(result_path.clone());
                state.artifact_dir = Some(artifact_dir);
            }
            let enqueue = |state: &mut ScriptState, task_id: i64| {
                state
                    .tasks
                    .push(make_task(task_id, group.clone(), command.clone(), "queued"));
                let manifest_behavior = state.manifest_behavior;
                let manifest_metric_name = if state.manifest_metric_name.is_empty() {
                    "score"
                } else {
                    state.manifest_metric_name.as_str()
                };
                match manifest_behavior {
                    ManifestBehavior::Missing => {}
                    ManifestBehavior::Invalid => {
                        fs::write(&result_path, b"not-json").unwrap();
                        set_private_file_mode(Path::new(&result_path));
                    }
                    ManifestBehavior::Valid | ManifestBehavior::EmptyMetrics => {
                        let metrics = if matches!(manifest_behavior, ManifestBehavior::EmptyMetrics)
                        {
                            json!({})
                        } else {
                            json!({ (manifest_metric_name): 1.25 })
                        };
                        let manifest = json!({
                            "schema_version": 1,
                            "experiment_id": experiment_id,
                            "metrics": metrics
                        });
                        fs::write(&result_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
                        set_private_file_mode(Path::new(&result_path));
                    }
                }
            };
            match behavior {
                AddBehavior::Success => {
                    enqueue(&mut self.state.lock().unwrap(), 41);
                    Ok(41)
                }
                AddBehavior::SuccessWrongId => {
                    enqueue(&mut self.state.lock().unwrap(), 42);
                    Ok(41)
                }
                AddBehavior::NotReleased => Err(failure(PueueControlBoundary::TargetNotReleased)),
                AddBehavior::ReleasedZero => {
                    let mut state = self.state.lock().unwrap();
                    if state.late_enqueue_after_zero_snapshot {
                        state.late_enqueue_identity = Some((group, command));
                    }
                    Err(failure(PueueControlBoundary::TargetReleasedClientQuiescent))
                }
                AddBehavior::ReleasedOne => {
                    let mut state = self.state.lock().unwrap();
                    enqueue(&mut state, 41);
                    if state.ambiguous_extra_group_task {
                        state.tasks.push(make_task(
                            99,
                            group,
                            "other-command".to_owned(),
                            "queued",
                        ));
                        state.ambiguous_extra_task_inserted = true;
                    }
                    Err(failure(PueueControlBoundary::TargetReleasedClientQuiescent))
                }
                AddBehavior::Duplicate => {
                    let mut state = self.state.lock().unwrap();
                    enqueue(&mut state, 41);
                    state.tasks.push(make_task(42, group, command, "queued"));
                    Err(failure(PueueControlBoundary::TargetReleasedClientQuiescent))
                }
                AddBehavior::CleanupUncertain => {
                    Err(failure(PueueControlBoundary::CleanupUncertain))
                }
            }
        }

        async fn kill_controlled_before(
            &self,
            task_id: i64,
            deadline: std::time::Instant,
            cancellation: CancellationToken,
        ) -> ControlledPueueResult<()> {
            check_control(deadline, &cancellation)?;
            let mut state = self.state.lock().unwrap();
            state.calls.push(format!("kill:{task_id}"));
            state.deadline_records.push(("kill".to_owned(), deadline));
            if state.kill_failures > 0 {
                state.kill_failures -= 1;
                return Err(failure(PueueControlBoundary::TargetReleasedClientQuiescent));
            }
            if let Some(task) = state.tasks.iter_mut().find(|task| task.id == task_id) {
                task.state = "killed".to_owned();
            }
            Ok(())
        }

        async fn remove_controlled_before(
            &self,
            task_id: i64,
            deadline: std::time::Instant,
            cancellation: CancellationToken,
        ) -> ControlledPueueResult<()> {
            check_control(deadline, &cancellation)?;
            let mut state = self.state.lock().unwrap();
            state.calls.push(format!("remove:{task_id}"));
            state.deadline_records.push(("remove".to_owned(), deadline));
            if state.remove_failures > 0 {
                state.remove_failures -= 1;
                return Err(failure(PueueControlBoundary::TargetReleasedClientQuiescent));
            }
            state.tasks.retain(|task| task.id != task_id);
            Ok(())
        }

        async fn group_exists_controlled_before(
            &self,
            group: &str,
            deadline: std::time::Instant,
            cancellation: CancellationToken,
        ) -> ControlledPueueResult<bool> {
            check_control(deadline, &cancellation)?;
            let mut state = self.state.lock().unwrap();
            state.calls.push("group_exists".to_owned());
            state
                .deadline_records
                .push(("group_exists".to_owned(), deadline));
            Ok(state.groups.contains(group))
        }

        async fn create_group_exclusive_controlled_before(
            &self,
            group: &str,
            deadline: std::time::Instant,
            cancellation: CancellationToken,
        ) -> ControlledPueueResult<()> {
            check_control(deadline, &cancellation)?;
            {
                let mut state = self.state.lock().unwrap();
                state.calls.push("group_exists".to_owned());
                state
                    .deadline_records
                    .push(("group_exists".to_owned(), deadline));
                if state.groups.contains(group) {
                    return Err(failure(PueueControlBoundary::TargetReleasedClientQuiescent));
                }
                if let Some(boundary) = state.group_create_failure.take() {
                    if boundary == PueueControlBoundary::TargetReleasedClientQuiescent {
                        state.groups.insert(group.to_owned());
                    }
                    return Err(failure(boundary));
                }
                state.calls.push("group_add".to_owned());
                state
                    .deadline_records
                    .push(("group_add".to_owned(), deadline));
                state.groups.insert(group.to_owned());
                state.calls.push("group_exists".to_owned());
                state
                    .deadline_records
                    .push(("group_exists".to_owned(), deadline));
                if !state.groups.contains(group) {
                    return Err(failure(PueueControlBoundary::TargetReleasedClientQuiescent));
                }
            }
            Ok(())
        }

        async fn remove_group_controlled_before(
            &self,
            group: &str,
            deadline: std::time::Instant,
            cancellation: CancellationToken,
        ) -> ControlledPueueResult<()> {
            check_control(deadline, &cancellation)?;
            let mut state = self.state.lock().unwrap();
            state.calls.push("group_remove".to_owned());
            state
                .deadline_records
                .push(("group_remove".to_owned(), deadline));
            if let Some(boundary) = state.group_remove_failure.take() {
                return Err(failure(boundary));
            }
            if state.tasks.iter().any(|task| task.group == group) {
                return Err(failure(PueueControlBoundary::TargetReleasedClientQuiescent));
            }
            state.groups.remove(group);
            Ok(())
        }
    }

    struct Fixture {
        _temporary: tempfile::TempDir,
        database_path: PathBuf,
        db: Db,
        project: Project,
        policy: Arc<ResolvedExecutionPolicy>,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_project(true, false)
        }

        fn with_project(enabled: bool, paused: bool) -> Self {
            let temporary = tempfile::tempdir().unwrap();
            let base = fs::canonicalize(temporary.path()).unwrap();
            let root = base.join("project");
            let state = base.join("state");
            let codex_home = base.join("codex-home");
            let trusted_bin = base.join("trusted-bin");
            for directory in [&root, &state, &codex_home, &trusted_bin] {
                fs::create_dir(directory).unwrap();
                set_private_mode(directory);
            }
            let service = root.join(".pueue-agent");
            fs::create_dir(&service).unwrap();
            set_private_mode(&service);
            fs::write(service.join("STATE.md"), "Improve the measured result.\n").unwrap();
            for name in ["codex", "pueue", "launcher"] {
                let path = trusted_bin.join(name);
                fs::write(&path, b"fixture").unwrap();
                set_executable_mode(&path);
            }
            let config = base.join("pueue.yml");
            fs::write(&config, b"fixture: true\n").unwrap();
            set_private_file_mode(&config);
            let policy = Arc::new(
                load_or_create_policy(&PolicyLoadInput {
                    state_dir: state.clone(),
                    project_roots: vec![root.clone()],
                    inherited_path: trusted_bin.clone().into_os_string(),
                    startup_environment: StartupEnvironment::from_pairs([
                        ("HOME", base.as_os_str()),
                        ("PUEUE_AGENT_STATE_DIR", state.as_os_str()),
                    ]),
                    codex_home,
                    pueue_config: config.clone(),
                    launcher_path: trusted_bin.join("launcher"),
                })
                .unwrap(),
            );

            let database_path = base.join("agent.sqlite3");
            let writable = Db::open(&database_path).unwrap();
            let mut new_project =
                NewProject::new("trial-project", &root, "registered-group", config, 1);
            new_project.enabled = enabled;
            new_project.paused = paused;
            let project = ProjectRepository::new(&writable)
                .register(&new_project)
                .unwrap();
            drop(writable);
            let db = Db::open_read_only(&database_path).unwrap();
            Self {
                _temporary: temporary,
                database_path,
                db,
                project,
                policy,
            }
        }

        fn trial_output_root(&self) -> PathBuf {
            self.project.root_path.join(".pueue-agent/trials")
        }
    }

    fn set_private_mode(path: &Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
    }

    fn set_private_file_mode(path: &Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    fn set_executable_mode(path: &Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
    }

    fn substitute_output_generation(result_path: &Path) {
        let generation = result_path.parent().unwrap();
        let retired = generation.with_file_name(format!(
            "{}-retired",
            generation.file_name().unwrap().to_string_lossy()
        ));
        fs::rename(generation, &retired).unwrap();
        fs::create_dir(generation).unwrap();
        set_private_mode(generation);
        let sentinel = generation.join("replacement-sentinel");
        fs::write(&sentinel, b"replacement generation").unwrap();
        set_private_file_mode(&sentinel);
    }

    fn create_excess_artifact_entries(artifact_dir: &Path) {
        fs::create_dir(artifact_dir).unwrap();
        set_private_mode(artifact_dir);
        for index in 0..=crate::environment::MAX_PRIVATE_TEMP_CLEANUP_ENTRIES {
            let entry = artifact_dir.join(format!("entry-{index:04}"));
            fs::write(&entry, b"x").unwrap();
            set_private_file_mode(&entry);
        }
    }

    fn options(argv: Vec<String>, timeout: Duration) -> TrialOptions {
        TrialOptions {
            argv,
            objective_metric: None,
            timeout,
        }
    }

    #[tokio::test]
    async fn invalid_timeout_has_no_output_or_pueue_mutation() {
        let fixture = Fixture::new();
        let pueue = NoopPueue {
            calls: AtomicUsize::new(0),
        };

        let result = run_with(
            &fixture.db,
            &fixture.project,
            Arc::clone(&fixture.policy),
            &pueue,
            &TrialOptions {
                argv: vec!["echo".to_owned(), "ok".to_owned()],
                objective_metric: None,
                timeout: Duration::ZERO,
            },
            CancellationToken::new(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(pueue.calls.load(Ordering::SeqCst), 0);
        assert!(!fixture.trial_output_root().exists());
    }

    #[tokio::test]
    async fn invalid_command_has_no_output_or_control_call() {
        let fixture = Fixture::new();
        let pueue = NoopPueue {
            calls: AtomicUsize::new(0),
        };
        let result = run_with(
            &fixture.db,
            &fixture.project,
            Arc::clone(&fixture.policy),
            &pueue,
            &options(Vec::new(), Duration::from_secs(1)),
            CancellationToken::new(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(pueue.calls.load(Ordering::SeqCst), 0);
        assert!(!fixture.trial_output_root().exists());
    }

    #[tokio::test]
    async fn disabled_registration_has_no_output_or_control_call() {
        let fixture = Fixture::with_project(false, false);
        let pueue = NoopPueue {
            calls: AtomicUsize::new(0),
        };

        let result = run_with(
            &fixture.db,
            &fixture.project,
            Arc::clone(&fixture.policy),
            &pueue,
            &options(vec!["benchmark".to_owned()], Duration::from_secs(1)),
            CancellationToken::new(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(pueue.calls.load(Ordering::SeqCst), 0);
        assert!(!fixture.trial_output_root().exists());
    }

    #[tokio::test]
    async fn invalid_objective_has_no_output_or_control_call() {
        let fixture = Fixture::new();
        fs::write(
            fixture.project.root_path.join(".pueue-agent/STATE.md"),
            "# Only a heading\n",
        )
        .unwrap();
        let pueue = NoopPueue {
            calls: AtomicUsize::new(0),
        };

        let result = run_with(
            &fixture.db,
            &fixture.project,
            Arc::clone(&fixture.policy),
            &pueue,
            &options(vec!["benchmark".to_owned()], Duration::from_secs(1)),
            CancellationToken::new(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(pueue.calls.load(Ordering::SeqCst), 0);
        assert!(!fixture.trial_output_root().exists());
    }

    #[tokio::test]
    async fn live_campaign_has_no_output_or_control_call() {
        let fixture = Fixture::new();
        let writable = Db::open(&fixture.database_path).unwrap();
        writable
            .connect()
            .unwrap()
            .execute(
                "INSERT INTO campaigns (
                    campaign_id, project_id, objective_text, objective_digest,
                    initial_argv_json, state, created_at, updated_at
                 ) VALUES ('trial-live-campaign', ?1, 'objective', 'digest',
                           '[\"benchmark\"]', 'active', 1, 1)",
                [&fixture.project.project_id],
            )
            .unwrap();
        drop(writable);
        let pueue = NoopPueue {
            calls: AtomicUsize::new(0),
        };

        let result = run_with(
            &fixture.db,
            &fixture.project,
            Arc::clone(&fixture.policy),
            &pueue,
            &options(vec!["benchmark".to_owned()], Duration::from_secs(1)),
            CancellationToken::new(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(pueue.calls.load(Ordering::SeqCst), 0);
        assert!(!fixture.trial_output_root().exists());
    }

    #[tokio::test]
    async fn admission_lock_contention_has_no_output_or_control_call() {
        let fixture = Fixture::new();
        let anchor = fixture
            .policy
            .project_root_anchor(&fixture.project.root_path)
            .unwrap();
        let verified = anchor.verify_identity().unwrap();
        let _held_lock = ProjectAdmissionLock::try_acquire(&verified)
            .unwrap()
            .unwrap();
        let pueue = NoopPueue {
            calls: AtomicUsize::new(0),
        };

        let result = run_with(
            &fixture.db,
            &fixture.project,
            Arc::clone(&fixture.policy),
            &pueue,
            &options(vec!["benchmark".to_owned()], Duration::from_secs(1)),
            CancellationToken::new(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(pueue.calls.load(Ordering::SeqCst), 0);
        assert!(!fixture.trial_output_root().exists());
    }

    #[tokio::test]
    async fn replaced_project_root_has_no_output_or_control_call() {
        let fixture = Fixture::new();
        let original = fixture.project.root_path.clone();
        let retired = original.with_file_name("project-retired");
        fs::rename(&original, &retired).unwrap();
        fs::create_dir(&original).unwrap();
        set_private_mode(&original);
        let service = original.join(".pueue-agent");
        fs::create_dir(&service).unwrap();
        set_private_mode(&service);
        fs::write(service.join("STATE.md"), "replacement objective\n").unwrap();
        let pueue = NoopPueue {
            calls: AtomicUsize::new(0),
        };

        let result = run_with(
            &fixture.db,
            &fixture.project,
            Arc::clone(&fixture.policy),
            &pueue,
            &options(vec!["benchmark".to_owned()], Duration::from_secs(1)),
            CancellationToken::new(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(pueue.calls.load(Ordering::SeqCst), 0);
        assert!(!fixture.trial_output_root().exists());
    }

    #[tokio::test]
    async fn successful_trial_removes_exact_task_group_and_private_output() {
        let fixture = Fixture::with_project(true, true);
        let pueue = ScriptPueue::with_states(&["queued", "running", "done"]);
        let report = run_with(
            &fixture.db,
            &fixture.project,
            Arc::clone(&fixture.policy),
            &pueue,
            &TrialOptions {
                argv: vec!["benchmark".to_owned(), "--small".to_owned()],
                objective_metric: Some(ObjectiveMetric {
                    name: "score".to_owned(),
                    direction: MetricDirection::Minimize,
                    min_delta: None,
                }),
                timeout: Duration::from_secs(3),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();

        assert!(report.is_success(), "{report:?}");
        assert_eq!(report.terminal, Some(TrialTerminalClass::Succeeded));
        assert_eq!(report.manifest, TrialManifestClass::Valid);
        assert_eq!(report.metric_count, Some(1));
        assert_eq!(report.selected_metric_name.as_deref(), Some("score"));
        assert_eq!(report.selected_metric_value, Some(1.25));
        assert_eq!(report.task_id, Some(41));
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Confirmed);
        assert!(!fixture
            .trial_output_root()
            .join(report.trial_id.to_string())
            .exists());

        let calls = pueue.calls();
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.as_str() == "group_remove")
                .count(),
            1
        );
        assert!(calls.contains(&"remove:41".to_owned()));
        assert!(!calls.iter().any(|call| call.starts_with("kill:")));
        let command = pueue.added_command();
        for task in pueue.snapshots().iter().flatten() {
            assert_eq!(task.id, 41);
            assert_eq!(task.group, report.group);
            assert_eq!(task.command, command);
        }
    }

    #[tokio::test]
    async fn extra_nonce_group_task_blocks_only_group_removal() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["done"]);
        pueue.add_extra_group_task_on_terminal();

        let report = run_with(
            &fixture.db,
            &fixture.project,
            Arc::clone(&fixture.policy),
            &pueue,
            &TrialOptions {
                argv: vec!["benchmark".to_owned()],
                objective_metric: None,
                timeout: Duration::from_secs(3),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();

        let calls = pueue.calls();
        assert!(calls.contains(&"remove:41".to_owned()));
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.as_str() == "group_remove")
                .count(),
            0
        );
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(report.outcome, super::TrialOutcome::CleanupUncertain);
        assert!(fixture
            .trial_output_root()
            .join(report.trial_id.to_string())
            .exists());
    }

    async fn run_scripted(
        fixture: &Fixture,
        pueue: &ScriptPueue,
        options: TrialOptions,
        cancellation: CancellationToken,
    ) -> super::TrialReport {
        run_with(
            &fixture.db,
            &fixture.project,
            Arc::clone(&fixture.policy),
            pueue,
            &options,
            cancellation,
        )
        .await
        .unwrap()
    }

    fn plain_options(timeout: Duration) -> TrialOptions {
        options(vec!["benchmark".to_owned()], timeout)
    }

    #[tokio::test]
    async fn add_boundaries_keep_their_distinct_cleanup_authority() {
        let fixture = Fixture::new();
        let not_released = ScriptPueue::default();
        not_released.set_add_behavior(AddBehavior::NotReleased);
        let clean = run_scripted(
            &fixture,
            &not_released,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(clean.outcome, super::TrialOutcome::AddUncertain);
        assert_eq!(clean.task_id, None);
        assert_eq!(clean.task_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(clean.group_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(clean.output_cleanup, TrialCleanupStatus::Confirmed);
        assert!(not_released.calls().contains(&"group_remove".to_owned()));

        let fixture = Fixture::new();
        let released_zero = ScriptPueue::default();
        released_zero.set_add_behavior(AddBehavior::ReleasedZero);
        let uncertain = run_scripted(
            &fixture,
            &released_zero,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(uncertain.outcome, super::TrialOutcome::AddUncertain);
        assert_eq!(uncertain.task_id, None);
        assert_eq!(uncertain.task_cleanup, TrialCleanupStatus::Uncertain);
        assert_eq!(uncertain.group_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(uncertain.output_cleanup, TrialCleanupStatus::Retained);
        let calls = released_zero.calls();
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.as_str() == "status")
                .count(),
            2
        );
        assert!(!calls.iter().any(|call| {
            call.starts_with("kill:") || call.starts_with("remove:") || call == "group_remove"
        }));
    }

    #[tokio::test]
    async fn unique_exact_task_after_quiescent_add_error_is_recovered() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::default();
        pueue.set_add_behavior(AddBehavior::ReleasedOne);

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.outcome, super::TrialOutcome::AddUncertain);
        assert_eq!(report.task_id, Some(41));
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Confirmed);
        assert!(pueue.calls().contains(&"remove:41".to_owned()));
    }

    #[tokio::test]
    async fn late_daemon_enqueue_after_zero_snapshot_remains_unresolved() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::default();
        pueue.set_add_behavior(AddBehavior::ReleasedZero);
        pueue.enqueue_after_zero_snapshot();

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        let snapshots = pueue.snapshots();
        let tasks_after_zero = pueue.tasks();
        assert_eq!(report.outcome, super::TrialOutcome::AddUncertain);
        assert_eq!(report.task_id, None);
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Uncertain);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(snapshots.len(), 2);
        assert!(snapshots[1].is_empty());
        assert_eq!(tasks_after_zero.len(), 1);
        assert_eq!(tasks_after_zero[0].id, 77);
        assert_eq!(tasks_after_zero[0].group, report.group);
        assert_eq!(tasks_after_zero[0].command, pueue.added_command());
        let calls = pueue.calls();
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.as_str() == "status")
                .count(),
            2
        );
        assert!(!calls.iter().any(|call| {
            call.starts_with("kill:") || call.starts_with("remove:") || call == "group_remove"
        }));
    }

    #[tokio::test]
    async fn unexpected_group_task_blocks_ambiguous_add_recovery() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::default();
        pueue.set_add_behavior(AddBehavior::ReleasedOne);
        pueue.add_extra_group_task_after_ambiguous_add();

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.outcome, super::TrialOutcome::AddUncertain);
        assert_eq!(report.task_id, None);
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Uncertain);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Retained);
        let calls = pueue.calls();
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.as_str() == "status")
                .count(),
            2
        );
        assert!(!calls.iter().any(|call| {
            call.starts_with("kill:") || call.starts_with("remove:") || call == "group_remove"
        }));
    }

    #[tokio::test]
    async fn duplicate_add_identities_are_not_recovered() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::default();
        pueue.set_add_behavior(AddBehavior::Duplicate);

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.outcome, super::TrialOutcome::IdentityUncertain);
        assert_eq!(report.task_id, None);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Retained);
        assert!(!pueue.calls().iter().any(|call| {
            call.starts_with("kill:") || call.starts_with("remove:") || call == "group_remove"
        }));
    }

    #[tokio::test]
    async fn cleanup_uncertain_add_grants_no_followup_mutation_authority() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::default();
        pueue.set_add_behavior(AddBehavior::CleanupUncertain);

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.outcome, super::TrialOutcome::CleanupUncertain);
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Uncertain);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Retained);
        let calls = pueue.calls();
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.as_str() == "status")
                .count(),
            1
        );
        assert!(!calls.iter().any(|call| {
            call.starts_with("kill:") || call.starts_with("remove:") || call == "group_remove"
        }));
    }

    #[tokio::test]
    async fn group_creation_failure_never_adopts_an_uncertain_group() {
        let fixture = Fixture::new();
        let not_released = ScriptPueue::default();
        not_released.fail_group_create(PueueControlBoundary::TargetNotReleased);

        let clean = run_scripted(
            &fixture,
            &not_released,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(clean.outcome, super::TrialOutcome::GroupCreationFailed);
        assert_eq!(clean.group_cleanup, TrialCleanupStatus::NotOwned);
        assert_eq!(clean.output_cleanup, TrialCleanupStatus::Confirmed);
        let calls = not_released.calls();
        assert!(!calls.iter().any(|call| {
            call == "group_add" || call == "group_remove" || call == "add" || call == "status"
        }));

        let fixture = Fixture::new();
        let quiescent = ScriptPueue::default();
        quiescent.fail_group_create(PueueControlBoundary::TargetReleasedClientQuiescent);
        let uncertain = run_scripted(
            &fixture,
            &quiescent,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(uncertain.outcome, super::TrialOutcome::GroupCreationFailed);
        assert_eq!(uncertain.group_cleanup, TrialCleanupStatus::Uncertain);
        assert_eq!(uncertain.output_cleanup, TrialCleanupStatus::Confirmed);
        assert!(!quiescent
            .calls()
            .iter()
            .any(|call| { call == "group_remove" || call == "add" || call == "status" }));
    }

    #[tokio::test]
    async fn acknowledged_task_identity_mismatch_is_never_killed_or_removed() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["done"]);
        pueue.mismatch_task_after_add();

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.outcome, super::TrialOutcome::IdentityUncertain);
        assert_eq!(report.task_id, Some(41));
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Uncertain);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Retained);
        assert!(!pueue.calls().iter().any(|call| {
            call.starts_with("kill:") || call.starts_with("remove:") || call == "group_remove"
        }));
    }

    #[tokio::test]
    async fn duplicate_canonical_command_in_another_group_is_identity_failure() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["done"]);
        pueue.add_duplicate_command_after_add();

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.outcome, super::TrialOutcome::IdentityUncertain);
        assert_eq!(report.task_id, Some(41));
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Retained);
        assert!(!pueue.calls().iter().any(|call| {
            call.starts_with("kill:") || call.starts_with("remove:") || call == "group_remove"
        }));
    }

    #[tokio::test]
    async fn returned_task_id_must_match_the_exact_snapshot_task() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["done"]);
        pueue.set_add_behavior(AddBehavior::SuccessWrongId);

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.outcome, super::TrialOutcome::IdentityUncertain);
        assert_eq!(report.task_id, Some(41));
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Retained);
        assert!(!pueue.calls().iter().any(|call| {
            call.starts_with("kill:") || call.starts_with("remove:") || call == "group_remove"
        }));
    }

    #[tokio::test]
    async fn status_error_after_add_uses_controlled_cleanup_and_exact_removal() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&[]);
        pueue.fail_status_on(2, PueueControlBoundary::TargetNotReleased);

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.outcome, super::TrialOutcome::ControlFailed);
        assert_eq!(report.task_id, Some(41));
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Confirmed);
        assert!(pueue.calls().contains(&"remove:41".to_owned()));
    }

    #[tokio::test]
    async fn status_error_during_ambiguous_add_recovery_retains_without_mutation() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::default();
        pueue.set_add_behavior(AddBehavior::ReleasedOne);
        pueue.fail_status_on(2, PueueControlBoundary::TargetNotReleased);

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.outcome, super::TrialOutcome::AddUncertain);
        assert_eq!(report.task_id, None);
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Uncertain);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Retained);
        assert!(!pueue.calls().iter().any(|call| {
            call.starts_with("kill:") || call.starts_with("remove:") || call == "group_remove"
        }));
    }

    #[tokio::test]
    async fn terminal_command_failure_is_reported_after_confirmed_cleanup() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["failed"]);
        pueue.fail_removes(1);

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.outcome, super::TrialOutcome::CommandFailed);
        assert_eq!(report.terminal, Some(TrialTerminalClass::Failed));
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(
            pueue
                .calls()
                .iter()
                .filter(|call| call.as_str() == "remove:41")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn queued_and_running_timeouts_use_remove_and_kill_cleanup_paths() {
        let fixture = Fixture::new();
        let queued = ScriptPueue::with_states(&[]);
        let queued_report = run_scripted(
            &fixture,
            &queued,
            plain_options(Duration::from_secs(1)),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(queued_report.outcome, super::TrialOutcome::TimedOut);
        assert_eq!(queued_report.task_cleanup, TrialCleanupStatus::Confirmed);
        assert!(queued.calls().contains(&"remove:41".to_owned()));
        assert!(!queued.calls().iter().any(|call| call.starts_with("kill:")));

        let fixture = Fixture::new();
        let running = ScriptPueue::with_states(&["running"]);
        running.fail_kills(1);
        let running_report = run_scripted(
            &fixture,
            &running,
            plain_options(Duration::from_secs(1)),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(running_report.outcome, super::TrialOutcome::TimedOut);
        assert_eq!(running_report.task_cleanup, TrialCleanupStatus::Confirmed);
        assert!(running.calls().contains(&"kill:41".to_owned()));
        assert!(running.calls().contains(&"remove:41".to_owned()));
        assert_eq!(
            running
                .calls()
                .iter()
                .filter(|call| call.as_str() == "kill:41")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn cancellation_uses_a_fresh_token_for_task_cleanup() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["queued"]);
        pueue.cancel_on_status(2);

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.outcome, super::TrialOutcome::Cancelled);
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Confirmed);
        assert!(pueue.calls().contains(&"remove:41".to_owned()));
    }

    #[tokio::test]
    async fn manifest_and_selected_metric_failures_are_closed_report_values() {
        for behavior in [
            ManifestBehavior::Missing,
            ManifestBehavior::Invalid,
            ManifestBehavior::EmptyMetrics,
        ] {
            let fixture = Fixture::new();
            let pueue = ScriptPueue::with_states(&["done"]);
            pueue.set_manifest(behavior, "score");
            let report = run_scripted(
                &fixture,
                &pueue,
                plain_options(Duration::from_secs(3)),
                CancellationToken::new(),
            )
            .await;
            assert_eq!(report.outcome, super::TrialOutcome::ManifestInvalid);
            assert_eq!(report.manifest, TrialManifestClass::Invalid);
            assert_eq!(report.task_cleanup, TrialCleanupStatus::Confirmed);
            assert_eq!(report.group_cleanup, TrialCleanupStatus::Confirmed);
            assert_eq!(report.output_cleanup, TrialCleanupStatus::Confirmed);
        }

        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["done"]);
        let mut options = plain_options(Duration::from_secs(3));
        options.objective_metric = Some(ObjectiveMetric {
            name: "missing-score".to_owned(),
            direction: MetricDirection::Minimize,
            min_delta: None,
        });
        let report = run_scripted(&fixture, &pueue, options, CancellationToken::new()).await;
        assert_eq!(report.outcome, super::TrialOutcome::SelectedMetricMissing);
        assert_eq!(report.manifest, TrialManifestClass::Valid);
        assert_eq!(report.metric_count, Some(1));
        assert_eq!(report.selected_metric_name, None);
        assert_eq!(report.selected_metric_value, None);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Confirmed);
    }

    #[tokio::test]
    async fn selected_metric_lookup_precedes_bounded_redacted_display() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["done"]);
        let raw_name = format!("path={} ", "x".repeat(400)).repeat(2);
        pueue.set_manifest(ManifestBehavior::Valid, &raw_name);
        let mut options = plain_options(Duration::from_secs(3));
        options.objective_metric = Some(ObjectiveMetric {
            name: raw_name.clone(),
            direction: MetricDirection::Minimize,
            min_delta: None,
        });

        let report = run_scripted(&fixture, &pueue, options, CancellationToken::new()).await;

        assert_eq!(report.outcome, super::TrialOutcome::Succeeded);
        assert_eq!(report.selected_metric_value, Some(1.25));
        let displayed_name = report.selected_metric_name.as_deref().unwrap();
        assert!(displayed_name.len() <= 240);
        assert_ne!(displayed_name, raw_name);
        assert!(!serde_json::to_string(&report).unwrap().contains(&raw_name));
    }

    #[tokio::test]
    async fn distinct_metric_keys_with_same_bounded_display_do_not_match() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["done"]);
        let shared_prefix = format!("metric-{}", "x".repeat(300));
        let selected_name = format!("{shared_prefix}a");
        let manifest_name = format!("{shared_prefix}b");
        assert_eq!(
            crate::output::bounded_redacted_text(&selected_name),
            crate::output::bounded_redacted_text(&manifest_name)
        );
        pueue.set_manifest(ManifestBehavior::Valid, &manifest_name);
        let mut options = plain_options(Duration::from_secs(3));
        options.objective_metric = Some(ObjectiveMetric {
            name: selected_name.clone(),
            direction: MetricDirection::Minimize,
            min_delta: None,
        });

        let report = run_scripted(&fixture, &pueue, options, CancellationToken::new()).await;

        assert_eq!(report.outcome, super::TrialOutcome::SelectedMetricMissing);
        assert_eq!(report.manifest, TrialManifestClass::Valid);
        assert_eq!(report.metric_count, Some(1));
        assert_eq!(report.selected_metric_name, None);
        assert_eq!(report.selected_metric_value, None);
        assert!(!report.is_success());
        let serialized = serde_json::to_string(&report).unwrap();
        assert!(!serialized.contains(&selected_name));
        assert!(!serialized.contains(&manifest_name));
    }

    #[tokio::test]
    async fn output_generation_substitution_is_preserved_for_recovery() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["done"]);
        pueue.substitute_output_on_terminal();

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        let replacement = fixture
            .trial_output_root()
            .join(report.trial_id.to_string());
        assert_eq!(report.manifest, TrialManifestClass::Invalid);
        assert_eq!(report.outcome, super::TrialOutcome::CleanupUncertain);
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Uncertain);
        assert_eq!(
            fs::read(replacement.join("replacement-sentinel")).unwrap(),
            b"replacement generation"
        );
    }

    #[tokio::test]
    async fn uncertain_group_removal_retains_private_output() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["done"]);
        pueue.fail_group_remove(PueueControlBoundary::TargetReleasedClientQuiescent);

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;

        assert_eq!(report.outcome, super::TrialOutcome::CleanupUncertain);
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Retained);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Retained);
        let calls = pueue.calls();
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.as_str() == "group_remove")
                .count(),
            1
        );
        assert!(calls.contains(&"group_exists".to_owned()));
        assert!(fixture
            .trial_output_root()
            .join(report.trial_id.to_string())
            .exists());
    }

    #[tokio::test]
    async fn artifact_entry_limit_failure_prevents_trial_success() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["done"]);
        pueue.exceed_artifact_cleanup_limit_on_terminal();

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(30)),
            CancellationToken::new(),
        )
        .await;

        let generation = fixture
            .trial_output_root()
            .join(report.trial_id.to_string());
        let artifact_dir = generation.join("artifacts");
        assert_eq!(
            report.terminal,
            Some(TrialTerminalClass::Succeeded),
            "{report:?}"
        );
        assert_eq!(report.manifest, TrialManifestClass::Valid);
        assert_eq!(report.outcome, super::TrialOutcome::CleanupUncertain);
        assert_eq!(report.task_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.group_cleanup, TrialCleanupStatus::Confirmed);
        assert_eq!(report.output_cleanup, TrialCleanupStatus::Uncertain);
        assert_eq!(fs::read_dir(&artifact_dir).unwrap().count(), 4097);
        assert!(generation.join("result.json").exists());
    }

    #[tokio::test]
    async fn execution_and_cleanup_calls_each_share_one_absolute_deadline() {
        let fixture = Fixture::new();
        let pueue = ScriptPueue::with_states(&["done"]);

        let report = run_scripted(
            &fixture,
            &pueue,
            plain_options(Duration::from_secs(3)),
            CancellationToken::new(),
        )
        .await;
        assert!(report.is_success());

        let deadlines = pueue.deadlines();
        let execution = deadlines[0].1;
        let cleanup_start = deadlines
            .iter()
            .position(|(name, _)| name == "remove")
            .unwrap();
        assert!(deadlines[..cleanup_start]
            .iter()
            .all(|(_, deadline)| *deadline == execution));
        let cleanup = deadlines[cleanup_start].1;
        assert!(deadlines[cleanup_start..]
            .iter()
            .all(|(_, deadline)| *deadline == cleanup));
        assert_ne!(execution, cleanup);
    }

    #[tokio::test]
    async fn timeout_above_maximum_has_no_output_or_pueue_mutation() {
        let fixture = Fixture::new();
        let pueue = NoopPueue {
            calls: AtomicUsize::new(0),
        };
        let result = run_with(
            &fixture.db,
            &fixture.project,
            Arc::clone(&fixture.policy),
            &pueue,
            &options(vec!["benchmark".to_owned()], Duration::from_secs(301)),
            CancellationToken::new(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(pueue.calls.load(Ordering::SeqCst), 0);
        assert!(!fixture.trial_output_root().exists());
    }
}
use std::{
    ffi::OsString,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    db::{CampaignRepository, Db, ProjectRepository},
    environment::{
        experiment_runtime_argv_with_outputs, private_trial_output_paths, PrivateTrialOutput,
        ProjectAdmissionLock,
    },
    events::result_is_failure,
    execution_policy::ResolvedExecutionPolicy,
    models::{ObjectiveMetric, Project, ProposalKind},
    output::bounded_redacted_text,
    proposals::{self, ProposalInput},
    pueue::{ControlledPueueFailure, PueueApi, PueueControlBoundary, PueueTask},
    pueue_security::validate_group,
    reconcile::try_canonical_command_display_os,
    result_manifest::{classify_manifest_bytes, ClassifiedManifest},
    state, AppError,
};

const DEFAULT_TRIAL_TIMEOUT: Duration = Duration::from_secs(60);
const MIN_TRIAL_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_TRIAL_TIMEOUT: Duration = Duration::from_secs(300);
const TRIAL_CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
const TRIAL_POLL_INTERVAL: Duration = Duration::from_millis(250);
const TRIAL_GROUP_PREFIX: &str = "pueue-agent-trial-";

#[derive(Debug, Clone)]
pub struct TrialOptions {
    pub argv: Vec<String>,
    pub objective_metric: Option<ObjectiveMetric>,
    pub timeout: Duration,
}

impl TrialOptions {
    pub const DEFAULT_TIMEOUT: Duration = DEFAULT_TRIAL_TIMEOUT;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TrialOutcome {
    Succeeded,
    CommandFailed,
    TimedOut,
    Cancelled,
    AddUncertain,
    GroupCreationFailed,
    ControlFailed,
    IdentityUncertain,
    ManifestInvalid,
    SelectedMetricMissing,
    CleanupUncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TrialTerminalClass {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TrialManifestClass {
    NotRead,
    Valid,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TrialCleanupStatus {
    NotStarted,
    NotRequired,
    Confirmed,
    NotOwned,
    Retained,
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TrialReport {
    pub schema_version: u8,
    pub trial_id: Uuid,
    pub task_id: Option<i64>,
    pub group: String,
    pub outcome: TrialOutcome,
    pub terminal: Option<TrialTerminalClass>,
    pub manifest: TrialManifestClass,
    pub metric_count: Option<usize>,
    pub selected_metric_name: Option<String>,
    pub selected_metric_value: Option<f64>,
    pub task_cleanup: TrialCleanupStatus,
    pub group_cleanup: TrialCleanupStatus,
    pub output_cleanup: TrialCleanupStatus,
}

impl TrialReport {
    pub fn is_success(&self) -> bool {
        self.outcome == TrialOutcome::Succeeded
            && self.task_cleanup == TrialCleanupStatus::Confirmed
            && self.group_cleanup == TrialCleanupStatus::Confirmed
            && self.output_cleanup == TrialCleanupStatus::Confirmed
    }
}

/// Run one bounded, non-campaign trial while holding the registered project
/// admission lock. Preflight failures return before creating private output or
/// issuing any Pueue operation; post-admission failures are represented by a
/// bounded report so recovery identifiers remain available to the caller.
pub async fn run_with<P: PueueApi + ?Sized>(
    db: &Db,
    project: &Project,
    policy: Arc<ResolvedExecutionPolicy>,
    pueue: &P,
    options: &TrialOptions,
    cancellation: CancellationToken,
) -> Result<TrialReport, AppError> {
    validate_options(options)?;

    let registered = refresh_registered_project(db, project)?;
    let root_anchor = policy
        .project_root_anchor(&registered.root_path)
        .map_err(AppError::from)?;
    let initial_root = root_anchor.verify_identity().map_err(AppError::from)?;
    let _admission = ProjectAdmissionLock::try_acquire(&initial_root)
        .map_err(AppError::from)?
        .ok_or(AppError::Runtime {
            operation: "acquire project trial admission lock",
        })?;

    let registered = refresh_registered_project(db, &registered)?;
    let verified_root = root_anchor.verify_identity().map_err(AppError::from)?;
    if CampaignRepository::new(db)
        .find_live_by_project(&registered.project_id)?
        .is_some()
    {
        return Err(AppError::Validation {
            field: "trial",
            message: "a managed campaign is active",
        });
    }

    let objective = state::load_objective(&verified_root.anchor.canonical_path)?;
    let baseline = proposals::validate_initial_baseline(
        ProposalInput {
            kind: ProposalKind::Experiment,
            hypothesis: "Non-campaign trial".to_owned(),
            source_experiment_id: None,
            argv: options.argv.clone(),
            working_directory: ".".to_owned(),
            expected_evidence: Vec::new(),
        },
        &objective.digest,
    )?;

    let trial_id = Uuid::new_v4();
    let campaign_id = Uuid::new_v4().to_string();
    let experiment_id = Uuid::new_v4().to_string();
    let group = format!("{TRIAL_GROUP_PREFIX}{}", trial_id.simple());
    validate_group(&group)?;

    let (result_path, artifact_dir) =
        private_trial_output_paths(&verified_root.anchor.canonical_path, trial_id);
    let runtime_argv = experiment_runtime_argv_with_outputs(
        &campaign_id,
        &experiment_id,
        &result_path,
        &artifact_dir,
        baseline.argv(),
    );
    let canonical_command = try_canonical_command_display_os(&runtime_argv)?;
    let add_args = trial_add_args(&group, &verified_root.anchor.canonical_path, &runtime_argv);
    crate::pueue::validate_add_argv(&add_args)?;

    let mut session = TrialSession::new(trial_id, group, canonical_command);
    if cancellation.is_cancelled() {
        session.report.outcome = TrialOutcome::Cancelled;
        return Ok(session.report);
    }

    session.output = Some(PrivateTrialOutput::create(&verified_root, trial_id)?);
    session.report.output_cleanup = TrialCleanupStatus::NotStarted;
    if cancellation.is_cancelled() {
        session.report.outcome = TrialOutcome::Cancelled;
        session
            .cleanup_output_only(Instant::now() + TRIAL_CLEANUP_TIMEOUT)
            .await;
        return Ok(session.report);
    }

    let execution_deadline = Instant::now() + options.timeout;
    match pueue
        .create_group_exclusive_controlled_before(
            &session.report.group,
            execution_deadline,
            cancellation.clone(),
        )
        .await
    {
        Ok(()) => {
            session.group_owned = true;
            session.report.group_cleanup = TrialCleanupStatus::NotStarted;
        }
        Err(failure) => {
            let boundary = take_boundary(failure);
            session.report.outcome = if cancellation.is_cancelled() {
                TrialOutcome::Cancelled
            } else if boundary == PueueControlBoundary::CleanupUncertain {
                TrialOutcome::CleanupUncertain
            } else {
                TrialOutcome::GroupCreationFailed
            };
            session.report.group_cleanup = match boundary {
                PueueControlBoundary::TargetNotReleased => TrialCleanupStatus::NotOwned,
                PueueControlBoundary::TargetReleasedClientQuiescent
                | PueueControlBoundary::CleanupUncertain => TrialCleanupStatus::Uncertain,
            };
            if boundary == PueueControlBoundary::CleanupUncertain {
                session.report.output_cleanup = TrialCleanupStatus::Retained;
            } else {
                session
                    .cleanup_output_only(Instant::now() + TRIAL_CLEANUP_TIMEOUT)
                    .await;
            }
            return Ok(session.report);
        }
    }

    if cancellation.is_cancelled() {
        session.report.outcome = TrialOutcome::Cancelled;
        cleanup_session(
            &mut session,
            pueue,
            None,
            None,
            Instant::now() + TRIAL_CLEANUP_TIMEOUT,
        )
        .await;
        return Ok(session.report);
    }

    match controlled_status(pueue, execution_deadline, cancellation.clone()).await {
        Ok(tasks)
            if scan_add_candidates(&tasks, &session.report.group, &session.command).is_empty() => {}
        Ok(_) => {
            session.retain_uncertain_as(TrialOutcome::IdentityUncertain);
            return Ok(session.report);
        }
        Err(boundary) => {
            session.report.outcome = if cancellation.is_cancelled() {
                TrialOutcome::Cancelled
            } else if boundary == PueueControlBoundary::CleanupUncertain {
                TrialOutcome::CleanupUncertain
            } else {
                TrialOutcome::ControlFailed
            };
            if boundary == PueueControlBoundary::CleanupUncertain {
                session.retain_uncertain();
            } else {
                cleanup_session(
                    &mut session,
                    pueue,
                    None,
                    None,
                    Instant::now() + TRIAL_CLEANUP_TIMEOUT,
                )
                .await;
            }
            return Ok(session.report);
        }
    }

    match pueue
        .add_controlled_before(&add_args, execution_deadline, cancellation.clone())
        .await
    {
        Ok(task_id) => {
            session.report.task_id = Some(task_id);
            session.report.task_cleanup = TrialCleanupStatus::NotStarted;
            let stop = observe_task(
                &mut session,
                pueue,
                task_id,
                &experiment_id,
                options.objective_metric.as_ref(),
                execution_deadline,
                cancellation,
            )
            .await;
            match stop {
                ExecutionStop::Retain(outcome) => session.retain_uncertain_as(outcome),
                ExecutionStop::Cleanup { outcome } => {
                    session.report.outcome = outcome;
                    cleanup_session(
                        &mut session,
                        pueue,
                        Some(task_id),
                        None,
                        Instant::now() + TRIAL_CLEANUP_TIMEOUT,
                    )
                    .await;
                }
                ExecutionStop::Terminal { outcome, task } => {
                    session.report.outcome = outcome;
                    cleanup_session(
                        &mut session,
                        pueue,
                        Some(task_id),
                        Some(task),
                        Instant::now() + TRIAL_CLEANUP_TIMEOUT,
                    )
                    .await;
                }
            }
        }
        Err(failure) => {
            let boundary = take_boundary(failure);
            if cancellation.is_cancelled() {
                session.report.outcome = TrialOutcome::Cancelled;
            } else {
                session.report.outcome = TrialOutcome::AddUncertain;
            }
            match boundary {
                PueueControlBoundary::CleanupUncertain => session.retain_uncertain(),
                PueueControlBoundary::TargetNotReleased => {
                    cleanup_session(
                        &mut session,
                        pueue,
                        None,
                        None,
                        Instant::now() + TRIAL_CLEANUP_TIMEOUT,
                    )
                    .await;
                }
                PueueControlBoundary::TargetReleasedClientQuiescent => {
                    recover_ambiguous_add(
                        &mut session,
                        pueue,
                        Instant::now() + TRIAL_CLEANUP_TIMEOUT,
                    )
                    .await;
                }
            }
        }
    }

    Ok(session.report)
}

fn validate_options(options: &TrialOptions) -> Result<(), AppError> {
    if !(MIN_TRIAL_TIMEOUT..=MAX_TRIAL_TIMEOUT).contains(&options.timeout) {
        return Err(AppError::Validation {
            field: "trial.timeout",
            message: "must be between 1 and 300 seconds",
        });
    }
    if options.argv.is_empty() || options.argv.first().is_some_and(String::is_empty) {
        return Err(AppError::Validation {
            field: "trial.command",
            message: "must contain a command",
        });
    }
    if let Some(metric) = &options.objective_metric {
        metric.validate()?;
    }
    Ok(())
}

fn refresh_registered_project(db: &Db, expected: &Project) -> Result<Project, AppError> {
    let current = ProjectRepository::new(db)
        .find_by_id(&expected.project_id)?
        .ok_or(AppError::Validation {
            field: "trial.project",
            message: "must be a registered project",
        })?;
    if current.root_path != expected.root_path || current.pueue_group != expected.pueue_group {
        return Err(AppError::Validation {
            field: "trial.project",
            message: "root and group identity changed",
        });
    }
    if !current.enabled {
        return Err(AppError::Validation {
            field: "trial.project",
            message: "project is disabled",
        });
    }
    Ok(current)
}

fn trial_add_args(group: &str, project_root: &Path, argv: &[std::ffi::OsString]) -> Vec<OsString> {
    let mut args = Vec::with_capacity(argv.len() + 5);
    args.extend([
        OsString::from("-g"),
        OsString::from(group),
        OsString::from("--working-directory"),
        project_root.as_os_str().to_os_string(),
        OsString::from("--"),
    ]);
    args.extend(argv.iter().cloned());
    args
}

struct TrialSession {
    report: TrialReport,
    command: String,
    output: Option<PrivateTrialOutput>,
    group_owned: bool,
}

impl TrialSession {
    fn new(trial_id: Uuid, group: String, command: String) -> Self {
        Self {
            report: TrialReport {
                schema_version: 1,
                trial_id,
                task_id: None,
                group,
                outcome: TrialOutcome::ControlFailed,
                terminal: None,
                manifest: TrialManifestClass::NotRead,
                metric_count: None,
                selected_metric_name: None,
                selected_metric_value: None,
                task_cleanup: TrialCleanupStatus::NotStarted,
                group_cleanup: TrialCleanupStatus::NotStarted,
                output_cleanup: TrialCleanupStatus::NotStarted,
            },
            command,
            output: None,
            group_owned: false,
        }
    }

    fn retain_uncertain(&mut self) {
        self.retain_uncertain_as(TrialOutcome::CleanupUncertain);
    }

    fn retain_uncertain_as(&mut self, outcome: TrialOutcome) {
        self.report.outcome = outcome;
        if self.report.task_cleanup == TrialCleanupStatus::NotStarted {
            self.report.task_cleanup = TrialCleanupStatus::Uncertain;
        }
        if self.group_owned && self.report.group_cleanup != TrialCleanupStatus::Confirmed {
            self.report.group_cleanup = TrialCleanupStatus::Retained;
        }
        if self.output.is_some() && self.report.output_cleanup != TrialCleanupStatus::Confirmed {
            self.report.output_cleanup = TrialCleanupStatus::Retained;
        }
    }

    async fn cleanup_output_only(&mut self, deadline: Instant) {
        let Some(output) = self.output.take() else {
            self.report.output_cleanup = TrialCleanupStatus::NotRequired;
            return;
        };
        let (retained, cleaned) = cleanup_private_output(output, deadline).await;
        self.output = retained;
        self.report.output_cleanup = if cleaned {
            TrialCleanupStatus::Confirmed
        } else {
            TrialCleanupStatus::Uncertain
        };
        if !cleaned {
            self.report.outcome = TrialOutcome::CleanupUncertain;
        }
    }
}

fn take_boundary(failure: ControlledPueueFailure) -> PueueControlBoundary {
    let boundary = failure.boundary();
    let _ = failure.into_error();
    boundary
}

async fn controlled_status<P: PueueApi + ?Sized>(
    pueue: &P,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<Vec<PueueTask>, PueueControlBoundary> {
    if Instant::now() >= deadline {
        return Err(PueueControlBoundary::TargetNotReleased);
    }
    pueue
        .status_json_controlled_before(deadline, cancellation)
        .await
        .map_err(take_boundary)
}

#[derive(Debug)]
enum ExpectedTaskScan {
    Exact(PueueTask),
    Absent,
    Ambiguous,
}

fn scan_add_candidates<'a>(
    tasks: &'a [PueueTask],
    group: &str,
    command: &str,
) -> Vec<&'a PueueTask> {
    tasks
        .iter()
        .filter(|task| task.group == group || task.command == command)
        .collect()
}

fn scan_expected_task(
    tasks: &[PueueTask],
    task_id: i64,
    group: &str,
    command: &str,
) -> ExpectedTaskScan {
    let id_matches = tasks.iter().filter(|task| task.id == task_id).count();
    let command_matches = tasks
        .iter()
        .filter(|task| task.command == command)
        .collect::<Vec<_>>();
    if id_matches == 0 && command_matches.is_empty() {
        return ExpectedTaskScan::Absent;
    }
    if id_matches == 1 && command_matches.len() == 1 {
        let task = command_matches[0];
        if task.id == task_id && task.group == group && task.command == command {
            return ExpectedTaskScan::Exact(task.clone());
        }
    }
    ExpectedTaskScan::Ambiguous
}

async fn observe_task<P: PueueApi + ?Sized>(
    session: &mut TrialSession,
    pueue: &P,
    task_id: i64,
    experiment_id: &str,
    objective_metric: Option<&ObjectiveMetric>,
    deadline: Instant,
    cancellation: CancellationToken,
) -> ExecutionStop {
    loop {
        if cancellation.is_cancelled() {
            return ExecutionStop::Cleanup {
                outcome: TrialOutcome::Cancelled,
            };
        }
        if Instant::now() >= deadline {
            return ExecutionStop::Cleanup {
                outcome: TrialOutcome::TimedOut,
            };
        }

        let tasks = match controlled_status(pueue, deadline, cancellation.clone()).await {
            Ok(tasks) => tasks,
            Err(PueueControlBoundary::CleanupUncertain) => {
                return ExecutionStop::Retain(TrialOutcome::CleanupUncertain)
            }
            Err(_) => {
                return ExecutionStop::Cleanup {
                    outcome: if cancellation.is_cancelled() {
                        TrialOutcome::Cancelled
                    } else if Instant::now() >= deadline {
                        TrialOutcome::TimedOut
                    } else {
                        TrialOutcome::ControlFailed
                    },
                }
            }
        };
        if cancellation.is_cancelled() {
            return ExecutionStop::Cleanup {
                outcome: TrialOutcome::Cancelled,
            };
        }
        if Instant::now() >= deadline {
            return ExecutionStop::Cleanup {
                outcome: TrialOutcome::TimedOut,
            };
        }

        match scan_expected_task(&tasks, task_id, &session.report.group, &session.command) {
            ExpectedTaskScan::Exact(task) if task.is_terminal() => {
                let failed = task.state.eq_ignore_ascii_case("failed")
                    || task.state.eq_ignore_ascii_case("killed")
                    || task.result.as_ref().is_some_and(result_is_failure);
                session.report.terminal = Some(if failed {
                    TrialTerminalClass::Failed
                } else {
                    TrialTerminalClass::Succeeded
                });
                let outcome = if failed {
                    TrialOutcome::CommandFailed
                } else {
                    classify_trial_manifest(session, experiment_id, objective_metric)
                };
                return ExecutionStop::Terminal { outcome, task };
            }
            ExpectedTaskScan::Exact(_) => {}
            ExpectedTaskScan::Absent | ExpectedTaskScan::Ambiguous => {
                return ExecutionStop::Retain(TrialOutcome::IdentityUncertain)
            }
        }

        match execution_poll_wait(deadline, &cancellation).await {
            PollWait::Poll => {}
            PollWait::Deadline => {
                return ExecutionStop::Cleanup {
                    outcome: TrialOutcome::TimedOut,
                }
            }
            PollWait::Cancelled => {
                return ExecutionStop::Cleanup {
                    outcome: TrialOutcome::Cancelled,
                }
            }
        }
    }
}

fn classify_trial_manifest(
    session: &mut TrialSession,
    experiment_id: &str,
    objective_metric: Option<&ObjectiveMetric>,
) -> TrialOutcome {
    let Some(output) = session.output.as_ref() else {
        session.report.manifest = TrialManifestClass::Invalid;
        return TrialOutcome::ManifestInvalid;
    };
    let bytes = match output.read_result_bounded() {
        Ok(bytes) => bytes,
        Err(_) => {
            session.report.manifest = TrialManifestClass::Invalid;
            return TrialOutcome::ManifestInvalid;
        }
    };
    let classified = match classify_manifest_bytes(&bytes, experiment_id) {
        Ok(classified) => classified,
        Err(_) => ClassifiedManifest::Invalid,
    };
    let ClassifiedManifest::Valid { metrics } = classified else {
        session.report.manifest = TrialManifestClass::Invalid;
        return TrialOutcome::ManifestInvalid;
    };
    if metrics.is_empty() {
        session.report.manifest = TrialManifestClass::Invalid;
        return TrialOutcome::ManifestInvalid;
    }

    session.report.manifest = TrialManifestClass::Valid;
    session.report.metric_count = Some(metrics.len());
    if let Some(metric) = objective_metric {
        let Some(value) = metrics.get(&metric.name).copied() else {
            return TrialOutcome::SelectedMetricMissing;
        };
        session.report.selected_metric_name = Some(bounded_redacted_text(&metric.name));
        session.report.selected_metric_value = Some(value);
    }
    TrialOutcome::Succeeded
}

enum ExecutionStop {
    Terminal {
        outcome: TrialOutcome,
        task: PueueTask,
    },
    Cleanup {
        outcome: TrialOutcome,
    },
    Retain(TrialOutcome),
}

enum PollWait {
    Poll,
    Deadline,
    Cancelled,
}

async fn execution_poll_wait(deadline: Instant, cancellation: &CancellationToken) -> PollWait {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return PollWait::Deadline;
    }
    let delay = remaining.min(TRIAL_POLL_INTERVAL);
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => PollWait::Cancelled,
        _ = tokio::time::sleep(delay) => {
            if Instant::now() >= deadline {
                PollWait::Deadline
            } else {
                PollWait::Poll
            }
        }
    }
}

async fn cleanup_poll_wait(deadline: Instant) -> bool {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return false;
    }
    tokio::time::sleep(remaining.min(TRIAL_POLL_INTERVAL)).await;
    Instant::now() < deadline
}

async fn recover_ambiguous_add<P: PueueApi + ?Sized>(
    session: &mut TrialSession,
    pueue: &P,
    cleanup_deadline: Instant,
) {
    let cancellation = CancellationToken::new();
    let tasks = match controlled_status(pueue, cleanup_deadline, cancellation).await {
        Ok(tasks) => tasks,
        Err(boundary) => {
            session.retain_uncertain_as(if boundary == PueueControlBoundary::CleanupUncertain {
                TrialOutcome::CleanupUncertain
            } else {
                TrialOutcome::AddUncertain
            });
            return;
        }
    };
    let candidates = scan_add_candidates(&tasks, &session.report.group, &session.command);
    if candidates.is_empty() {
        session.retain_uncertain_as(TrialOutcome::AddUncertain);
        return;
    }
    if candidates.len() != 1 {
        let matching_commands = tasks
            .iter()
            .filter(|task| task.command == session.command)
            .count();
        session.retain_uncertain_as(if matching_commands > 1 {
            TrialOutcome::IdentityUncertain
        } else {
            TrialOutcome::AddUncertain
        });
        return;
    }
    let task = candidates[0];
    if task.group != session.report.group || task.command != session.command {
        session.retain_uncertain_as(TrialOutcome::IdentityUncertain);
        return;
    }
    let task_id = task.id;
    session.report.task_id = Some(task_id);
    session.report.task_cleanup = TrialCleanupStatus::NotStarted;
    cleanup_session(
        session,
        pueue,
        Some(task_id),
        Some(task.clone()),
        cleanup_deadline,
    )
    .await;
}

async fn cleanup_session<P: PueueApi + ?Sized>(
    session: &mut TrialSession,
    pueue: &P,
    task_id: Option<i64>,
    task_hint: Option<PueueTask>,
    cleanup_deadline: Instant,
) {
    let cancellation = CancellationToken::new();
    let task_absent = if let Some(task_id) = task_id {
        cleanup_task(
            pueue,
            task_id,
            &session.report.group,
            &session.command,
            task_hint,
            cleanup_deadline,
            &cancellation,
        )
        .await
    } else {
        confirm_trial_task_absent(
            pueue,
            &session.report.group,
            &session.command,
            cleanup_deadline,
            cancellation.clone(),
        )
        .await
    };
    if !task_absent {
        session.retain_uncertain();
        return;
    }
    session.report.task_cleanup = TrialCleanupStatus::Confirmed;
    if !session.group_owned {
        session.report.group_cleanup = TrialCleanupStatus::NotOwned;
        session.cleanup_output_only(cleanup_deadline).await;
        return;
    }

    let fresh_tasks = match controlled_status(pueue, cleanup_deadline, cancellation.clone()).await {
        Ok(tasks) => tasks,
        Err(_) => {
            session.retain_uncertain();
            return;
        }
    };
    if !scan_add_candidates(&fresh_tasks, &session.report.group, &session.command).is_empty() {
        session.retain_uncertain();
        return;
    }

    let remove_result = if Instant::now() < cleanup_deadline {
        Some(
            pueue
                .remove_group_controlled_before(
                    &session.report.group,
                    cleanup_deadline,
                    cancellation.clone(),
                )
                .await,
        )
    } else {
        None
    };
    if let Some(Err(failure)) = remove_result.as_ref() {
        if failure.boundary() == PueueControlBoundary::CleanupUncertain {
            let _ = failure.boundary();
            session.retain_uncertain();
            return;
        }
    }

    if Instant::now() >= cleanup_deadline {
        session.report.group_cleanup = TrialCleanupStatus::Retained;
        session.report.output_cleanup = TrialCleanupStatus::Retained;
        session.report.outcome = TrialOutcome::CleanupUncertain;
        return;
    }
    let group_absent = match pueue
        .group_exists_controlled_before(&session.report.group, cleanup_deadline, cancellation)
        .await
    {
        Ok(false) => true,
        Ok(true) | Err(_) => false,
    };
    if !group_absent {
        session.report.group_cleanup = TrialCleanupStatus::Retained;
        session.report.output_cleanup = TrialCleanupStatus::Retained;
        session.report.outcome = TrialOutcome::CleanupUncertain;
        return;
    }
    session.report.group_cleanup = TrialCleanupStatus::Confirmed;
    session.cleanup_output_only(cleanup_deadline).await;
}

async fn confirm_trial_task_absent<P: PueueApi + ?Sized>(
    pueue: &P,
    group: &str,
    command: &str,
    deadline: Instant,
    cancellation: CancellationToken,
) -> bool {
    let tasks = match controlled_status(pueue, deadline, cancellation).await {
        Ok(tasks) => tasks,
        Err(_) => return false,
    };
    scan_add_candidates(&tasks, group, command).is_empty()
}

async fn cleanup_task<P: PueueApi + ?Sized>(
    pueue: &P,
    task_id: i64,
    group: &str,
    command: &str,
    initial_hint: Option<PueueTask>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> bool {
    let mut task_hint = initial_hint;
    loop {
        if Instant::now() >= deadline {
            return false;
        }
        let task = if let Some(task) = task_hint.take() {
            if task.id != task_id || task.group != group || task.command != command {
                return false;
            }
            task
        } else {
            let tasks = match controlled_status(pueue, deadline, cancellation.clone()).await {
                Ok(tasks) => tasks,
                Err(PueueControlBoundary::CleanupUncertain) => return false,
                Err(_) => {
                    if !cleanup_poll_wait(deadline).await {
                        return false;
                    }
                    continue;
                }
            };
            match scan_expected_task(&tasks, task_id, group, command) {
                ExpectedTaskScan::Exact(task) => task,
                ExpectedTaskScan::Absent => return true,
                ExpectedTaskScan::Ambiguous => return false,
            }
        };

        let action = if task.is_running() {
            pueue
                .kill_controlled_before(task_id, deadline, cancellation.clone())
                .await
        } else {
            pueue
                .remove_controlled_before(task_id, deadline, cancellation.clone())
                .await
        };
        if let Err(failure) = action {
            if failure.boundary() == PueueControlBoundary::CleanupUncertain {
                let _ = failure.into_error();
                return false;
            }
            let _ = failure.into_error();
        }

        if Instant::now() >= deadline {
            return false;
        }
        let tasks = match controlled_status(pueue, deadline, cancellation.clone()).await {
            Ok(tasks) => tasks,
            Err(PueueControlBoundary::CleanupUncertain) => return false,
            Err(_) => {
                if !cleanup_poll_wait(deadline).await {
                    return false;
                }
                continue;
            }
        };
        match scan_expected_task(&tasks, task_id, group, command) {
            ExpectedTaskScan::Absent => return true,
            ExpectedTaskScan::Exact(task) => {
                task_hint = Some(task);
                if !cleanup_poll_wait(deadline).await {
                    return false;
                }
            }
            ExpectedTaskScan::Ambiguous => return false,
        }
    }
}

async fn cleanup_private_output(
    mut output: PrivateTrialOutput,
    deadline: Instant,
) -> (Option<PrivateTrialOutput>, bool) {
    match tokio::task::spawn_blocking(move || {
        let result = output.cleanup_before(deadline);
        (output, result)
    })
    .await
    {
        Ok((output, Ok(_))) => (Some(output), true),
        Ok((output, Err(_))) => (Some(output), false),
        Err(_) => (None, false),
    }
}
