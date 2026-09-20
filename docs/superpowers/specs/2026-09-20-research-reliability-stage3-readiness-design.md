# 自律研究の信頼性改善：第3段階 setup/readiness 設計

日付: 2026-09-20

状態: Draft（Stage 2 受入後の実装候補）

調査基準: Stage 2 worktree `9e29636`（Stage 2 完了commitを実装開始点とする）

上位方針: [第1段階の設計](./2026-09-15-research-reliability-stage1-design.md)

## 1. 目的

Stage 1 のロードマップが約束した「初回submit前の導入支援」を、既存のtrusted-native境界内で実装する。利用者は次の3点をcampaign作成前に確認できる。

1. `STATE.md`が実際にobjectiveとして受理される。
2. immutable policyに固定されたinstalled Codexが、decision/research roleに必要な実capabilityを持つ。
3. 利用者が指定した短いexperiment commandがreal Pueue上で実行され、本番と同じ環境変数名から有効なresult manifestを生成できる。

doctorは読み取り専用を維持する。trialは利用者が明示的に起動した1回だけを実行し、campaign、proposal、experiment、submission、reservation、研究予算を作成・消費しない。

## 2. 対象外

- Stage 2 のresearch continuity、復旧、cleanup authorityの変更
- campaignの自動開始、trial成功後の自動submit、daemonからの自動trial
- 新しい設定項目、SQLite schema、依存crate、汎用command runner
- arbitrary commandのOS隔離、credential継承変更、GPU/resource budget（Stage 5）
- trial出力の保持option、過去trial一覧、trial再開、CLI crash後の自動回収
- active campaign中のtrial、候補worktree上のtrial
- 本設計・実装中に利用者の実projectでlive trialを起動すること

## 3. 現行コードから再利用する境界

- `state::load_objective`は16 KiB上限、UTF-8、制御文字、placeholder、意味のある行を検証し、digestを返す。objective parserを追加しない。
- `codex_command::probe_installed_codex_capabilities`はpinned Codexに対する`--version`、`--help`、`exec --help`を各2秒・256 KiB・空environment・process-group cleanup付きで実行する。
- `CodexCapabilities::supports_research_policy`はdecision契約に加えてexact `--json`を要求する。
- `proposals::validate_initial_baseline`、experiment runtime environment builder、`validate_add_argv`、canonical command displayが本番baselineのargv契約を持つ。
- `PueueApi`がadd/status/kill/removeをshellなしで実行し、各control commandを30秒に制限する。
- `result_manifest`のclassifierは16 KiB、schema version 1、exact experiment ID、finite numeric metricsを検証する。
- `ProjectAdmissionLock`とpinned `ProjectRootAnchor`がproject単位の実行開始を直列化する。
- private temp実装がowner/mode/mount/descriptor identityを検証し、bounded descriptor-relative cleanupを行う既存primitiveを持つ。

## 4. Objective readiness doctor

`build_doctor_report_with_policy_and_roots`はlive campaignが0件の場合に、1件だけ`state.objective` checkを追加する。

- `state::load_objective(&project.root_path)`成功は`ok`。
- missing、placeholder、oversize、invalid UTF-8、control character、heading/tableだけの入力は`error`。
- summary/remediationにobjective本文やraw I/O errorを入れない。
- live campaignがある場合は新しいcheckを出さず、既存の`campaign.objective_digest`のok/warning/error契約をそのまま使う。active snapshotと現在のファイルが異なるだけで起動不能へ格上げしない。
- `DoctorReport::has_errors`による既存exit 1を使う。新しいexit codeやreadiness commandは作らない。

doctorはobjectiveを修正せず、SQLite、project file、policy、Pueueを変更しない。

## 5. Installed Codex capability doctor

同期のreport builderからsubprocessを起動しない。`commands::doctor`がpolicy load後に非同期probeを1回実行し、boundedなprojectionだけを`DoctorExternal`へ渡す。

```rust
pub enum DoctorAgentRuntime {
    Supported,
    Blocked { code: PolicyViolationCode },
    SkippedPolicyUnavailable,
}

pub async fn probe_doctor_agent_runtime(
    policy: &Result<ResolvedExecutionPolicy, PolicyViolation>,
) -> DoctorAgentRuntime;
```

- policy成功時は必ずglobal `ResolvedExecutionPolicy::codex_anchor`をprobeする。standard project agentがcustom executableでも置き換えない。
- probe成功かつ`supports_research_policy()`だけを`Supported`とする。decision capabilityだけのCLIやexact `--json`欠落は`UnsafeCodexArgument`相当の`Blocked`。
- policy load失敗時はambient `codex`を探索せず`SkippedPolicyUnavailable`。
- `execution.agent_runtime` checkは`execution.anchors`の直後に出す。Supportedは`ok`、Blockedは`error`、Skippedは`warning`。
- reportにはtyped policy codeだけを出し、version/help stdout、path、environment値を出さない。
- 実際のagent launch時のprobeは残す。doctor結果をSQLiteへcacheせず、後の起動authorityとして扱わない。

Linuxが正式な実行対象である。non-Linuxでは既存の`UnsupportedPlatform`をbounded errorとして表示し、fallback executableを起動しない。

## 6. Non-campaign trial CLI

CLIは次に固定する。

```text
pueue-agent trial [--timeout-seconds 60] \
  [--metric-name NAME --metric-direction minimize|maximize [--metric-min-delta DELTA]] \
  [--json] -- COMMAND...
```

- `COMMAND`は必須で、submitと同じくUTF-8のargvとして扱う。shell文字列を新しく解釈しない。
- `--timeout-seconds`は1〜300、既定60。trial taskの実行deadlineであり、Pueue control commandの既存30秒上限とは別である。
- cleanup confirmationはtask deadline後も最大30秒だけ行う。status pollは250 ms、すべて1つのabsolute deadlineから残時間を計算する。
- metric flagsはsubmitと同じ組合せ・validationを使う。指定時はexact metric名のfinite値を要求する。未指定時もvalidかつnonemptyなmetric mapを要求する。
- command invocation自体を実行承認とみなし、追加のpreview/confirmation promptは挟まない。

成功は次をすべて満たす場合だけである。

1. project/objective/policy/command preflightが成功した。
2. 専用Pueue groupとtaskのidentityが一貫した。
3. taskが成功terminalになった。
4. exact trial experiment IDのmanifestが有効だった。
5. task、専用group、private outputのcleanupを確認した。

## 7. AdmissionとDB境界

trialはcurrent directoryから既存のregistered/enabled projectを解決し、read-only DBを使う。

1. policy、pinned project root、configured Pueueを解決する。
2. commandをString argvへ変換し、`state::load_objective`と`proposals::validate_initial_baseline`を行う。
3. `ProjectAdmissionLock`を取得し、root identity、enabled registration、live campaign不在を再検証する。
4. lockをtask/group/output cleanupの完了まで保持する。短いtrial中にcampaign baselineやnative agentが同じprojectへ新規admitされないようにする。

preflight失敗ではfilesystemとPueueを変更しない。trial経路はwriteable `Db`、campaign coordinator、repositories、callback writerを呼ばない。

trialはcampaign/proposal/experiment/submission/reservation/agent-run/research-review/Event/task-observationを作らない。後述する未登録Pueue groupへのglobal callbackは既存どおり`integration_events(kind=unknown_callback_group)`を1件だけ冪等記録し得る。これは通常Eventではなくscheduler/agent起動へ接続されない。

## 8. 専用Pueue groupによる隔離

registered project groupは使わない。そこへtrialを入れると既存callback/reconcilerが通常Eventを作り、agent起動へ接続し得るためである。

- group名は`pueue-agent-trial-<uuid simple>`。128 bytes以内のASCIIで、毎回新規生成する。
- `PueueApi`へ薄い`group_exists`、`create_group_exclusive`、`remove_group`操作を追加する。既存`CommandPueue::execute`と`validate_group`を使い、新runnerを作らない。
- ownership proofは同じadapterによる`create前に不存在`、direct `group add`の成功応答、`create後に存在`の3点すべてとする。既存groupを成功扱いで採用しない。add失敗後にgroupが現れてもownership不明として削除しない。実Pueueの既存group応答はisolated Linux gateで固定する。
- taskをexact identityでterminal/removed/absentまで確認した後に限り、group内taskが0件であることを再確認して`group remove`する。
- remove後はgroup不存在を確認する。削除失敗や残存はtrial失敗として、group名とtask IDだけをbounded reportに残す。
- `group remove`は残存taskをdefaultへ移すPueue操作なので、空確認前には絶対に呼ばない。

reconcilerは未登録groupを`unknown_groups`として無視し、task observation、Event、agentを作らない。callbackが未知groupをintegration eventとして記録する既存挙動は維持する。

## 9. Private trial output

`environment.rs`にtrial専用の小さいcapabilityを置き、既存private-tempのopenat/mount/identity/cleanup primitiveを再利用する。

```rust
pub struct PrivateTrialOutput { /* retained root descriptors and identities */ }

impl PrivateTrialOutput {
    pub fn create(root: &VerifiedProjectRoot, trial_id: Uuid) -> Result<Self, PolicyViolation>;
    pub fn result_path(&self) -> &Path;
    pub fn artifact_dir(&self) -> &Path;
    pub fn read_result_bounded(&self) -> Result<Vec<u8>, PolicyViolation>;
    pub fn cleanup(&mut self) -> Result<TempCleanupReport, PolicyViolation>;
}
```

- pathは`.pueue-agent/trials/<uuid>/result.json`と同階層の`artifacts/`。
- service/trials/generationはowner-only directory。productionと同じくresult fileとartifact directoryはchildが作るため、trial側で事前作成しない。
- 作成前後とread/cleanup前後にproject root、mount、service、parent、generationのidentityを再検証する。
- terminal後にretained generation descriptorから`result.json`をno-followで開き、regular file、owner、mode、link count、mountを検証して16 KiB + 1 byteまで読む。childによる同一generation内のatomic renameは受理するが、symlink、hardlink、special file、parent/generation substitutionは受理しない。
- cleanupはtask absence確認後だけ、既存のdepth/entry/allocated-byte上限内でdescriptor-relativeに実行する。pathnameでreplacement generationを削除しない。
- explicit cleanup成功をtrial成功条件にする。Dropのbest-effortだけで成功としない。

本番campaignの既存output pathは変更しない。runtime environment builderの共通coreだけを次の形で切り出す。

```rust
pub(crate) fn experiment_runtime_argv_with_outputs(
    campaign_id: &str,
    experiment_id: &str,
    result_path: &Path,
    artifact_dir: &Path,
    user_argv: &[String],
) -> Vec<OsString>;
```

既存`campaign_experiment_runtime_argv`は従来pathsを計算してこのcoreを呼ぶため、campaign argv/env契約は変わらない。trialも同じ4変数名を使う。

## 10. Task identity、timeout、cleanup

trial ID、campaign ID、experiment IDはmemory-only UUIDである。runtime argvにはunique IDsとdedicated output pathsが入るため、canonical commandもtrialごとに一意になる。

Pueue add argsは専用group、pinned project rootの`--working-directory`、`--`、runtime argvで構成し、`validate_add_argv`を通す。add前に同じgroup/commandのtaskが0件であることを確認する。

add後の各snapshotは次を満たす必要がある。

- returned task IDが1件だけ存在する、またはcleanup済みとして不存在である。
- 存在時はgroupとcanonical commandがexact一致する。
- 同じunique canonical commandが別IDに存在しない。

addがerrorになっても「何も投入されなかった」と仮定しない。bounded status scanでunique group/commandを探し、exact 1件ならそのIDをcleanup対象として回収する。0件を安定して確認できればoutput/group cleanupへ進み、複数・status error・identity mismatchならPueue mutationを止めて残存を報告する。

通常終了とcleanupの順序は次である。

1. terminal taskを観測し、既存`PueueTask::is_terminal`と`events::result_is_failure`を使い、`failed`/`killed`/failure result以外だけを成功に分類する。
2. 成功taskだけmanifestをdescriptor-readしてproduction classifierで分類する。
3. terminal taskをremoveし、task IDとunique commandの不存在を確認する。
4. dedicated groupが空であることを再一覧で確認し、groupをremoveして不存在を確認する。
5. private outputをidentity-checkしてcleanupする。

deadline時はqueued/stashed等のnon-running taskをremoveする。running taskはkillし、同じidentityのterminalまたはabsenceを確認してからremoveへ進む。kill/remove errorもstatusで最終状態を確認する。identity disagreement、status不明、kill後もrunning、group残存、output identity/cleanup失敗は成功にしない。

## 11. Manifest classifierと表示

`result_manifest::classify_manifest`のpure部分を`pub(crate)`のbounded projectionとして再利用する。trialから`ExperimentMetricsRow`やDB persist APIは呼ばない。

```rust
pub(crate) enum ClassifiedManifest {
    Invalid,
    Valid { metrics: BTreeMap<String, f64> },
}

pub(crate) fn classify_manifest_bytes(
    bytes: &[u8],
    expected_experiment_id: &str,
) -> Result<ClassifiedManifest, AppError>;
```

既存campaign ingestionはこの関数から従来のrowを作る。trialはempty mapを追加で拒否し、metric指定時はexact keyを要求する。

human/JSON reportはschema version、trial ID、Pueue task ID、dedicated group、terminal classification、manifest classification、metric count、選択metric名/value、task/group/output cleanup状態だけを含む。user command、environment値、manifest bytes、Codex help output、credentialを表示しない。JSON schemaは固定fieldを持ち、失敗理由はbounded codeにする。

taskが投入された後のcommand failureやinvalid manifestでも、cleanupを確認できた場合はreportを出してexit 1にする。cleanupが不明ならcleanup uncertaintyを優先してexit 1にする。

## 12. 最小導入順

Stage 3Aと3Bは同じStage 3内の独立checkpointにする。

- **3A doctor readiness:** objectiveとCodex capability。read-onlyでtrial private filesystem/Pueue変更に依存しないため先に実装・reviewできる。
- **3B bounded trial:** private output、ephemeral group、task state machine、CLI/E2E。3Aのobjective checkと既存probeの変更には依存しないが、Stage 2受入後の同じbaseへ積む。

3Aだけを最終Stage 3完了とは呼ばない。3Bはsecure output/group cleanupの独立reviewを通してから統合する。

## 13. 受入条件

1. no-campaign doctorは実際のsubmitと同じobjective validatorを使い、invalid objectiveでerrorになる。
2. active campaignの既存objective digest warning契約は変わらない。
3. doctorはglobal pinned Codexをbounded probeし、research capability不足を事前にerror表示する。
4. doctorはraw help/version/objectiveを出さず、どのcheckでもDB/files/policy/Pueueを変更しない。
5. trial preflight失敗はPueue/filesystemを変更しない。
6. trialは専用nonce groupだけを作り、registered project groupへtaskを入れない。
7. real taskがproject rootで実argvを実行し、4つのenvironment変数とinput data accessを確認してvalid manifestを生成する。
8. success/failure/queued timeout/running timeout/add ambiguityで、exact task identityを使いkill/removeを判断する。
9. 成功時はtask、group、private outputがすべて消え、置換されたoutputや別group/taskを削除しない。
10. campaign/proposal/experiment/submission/reservation/agent-run/research-review/Event/task-observation行とbudgetは増えない。global callback由来のbounded unknown-group integration eventだけは許容する。
11. trialを2回実行してID/path/groupが衝突せず、timeout後にrunning trial taskを残さない。
12. Linux isolated gateは専用state、Pueue profile、disposable projectで行い、既存service/experimentや利用者projectを操作しない。

## 14. 既知の限界

CLI processがSIGKILLやhost crashで消えると、non-durable trialは自動再開・自動cleanupされない。今回DB rowを作らない契約と両立してdurable crash recoveryを加えることは別機能である。通常完了、validation failure、Pueue error、task timeout、捕捉したinterruptではbounded cleanupを行う。identityが不明な場合は安全のため残し、trial ID/group/task IDを表示する。

専用groupへのglobal callbackがunknown-group integration eventを残す場合がある。これはagent起動authorityではなく、既存の外部integration監査記録である。これも完全にzero-writeにするにはcallbackへspoof不能なtransient identityを追加する必要があり、本Stage 3の最小scopeには含めない。
