# Stage 2 検証報告

作成日: 2026-09-22

## 判定

最終検証対象は Stage 1 baseline `7868455472371688deb6151589658690c26033d8` から Task 8 を経た commit `2f49d3b60b52d93d79a62c1ed998347742a5be9d` と、レビュー承認済み Task 9 の9 pathを含む `13c32b3c3be6acd53a6bcab30d9820423c580a53` である。`13c32b3` の Linux runner は exit 0、`accepted=true` を記録し、26 gateのうち24 gateは exit 0、2つの formatter check は診断を出して exit 1 となった。したがって機能・統合受入パケットは受理されたが、「26 gateすべて成功」とは記載しない。書式診断は残存課題として以下に定量化する。

| 項目 | 結果 |
| --- | --- |
| 最終検証 source | `13c32b3c3be6acd53a6bcab30d9820423c580a53`、親 `2f49d3b60b52d93d79a62c1ed998347742a5be9d` |
| Linux acceptance runner | `runner_exit=0`, `accepted=true`, `remote_clean=true`; 24 gate exit 0、formatter 2 gate exit 1 |
| Task 8 | native fake 1 test、Bats 25 tests、15 real-Pueue research cases、legacy Rust E2E の18 gateすべて exit 0 |
| Task 9 | 8 Linux gatesを実行。6 gate exit 0、formatter 2 gate exit 1。Rust 32 Cargo targets: `1957 passed; 0 failed; 15 ignored` |
| 後片付け | 15 research WORK directories、legacy WORK、runner/legacy/Pueue daemon の捕捉 identity はすべて absent と照合済み |
| formatter | Stage 1継承ファイルとStage 2追加ファイルの両方に診断あり。包括的な再整形は行っていない |

最終 Linux evidence は SHA-256 `f389d56d194e151d182022f26114f132acfdc688d49e6f27c32b2965c698ad40`、runner log は `13497542fcd9fda0693786e51492dc14573290c82be65591ae06278dbdd436a6`。Task 8 case marker evidence は `160eab3b8434450b4dedc1a2887843b3394a0c7743f0dd6fc70025083d135924`、cleanup evidence は `1f00b82fcaf45f6d459307f0cf300ec175f3953b21231e01d293580e89feb702` である。

## 対象と変更範囲

Task 9 commit `13c32b3` は Task 8 accepted source を親とし、レビュー済み deferred-minor と Task 4 report の EOF 修正を含む。Task 8 parent `2f49d3b60b52d93d79a62c1ed998347742a5be9d` から Task 9 candidate `13c32b3c3be6acd53a6bcab30d9820423c580a53` への差分は `+93/-65`、9 path。コード・テスト8 pathは `src/db/research.rs`、`src/agent.rs`、`tests/integration/database.rs`、`tests/integration/research_actions.rs`、`tests/integration/pueue_adapter.rs`、`src/native_launcher.rs`、`src/campaign.rs`、`src/code_change.rs`。残る1 path `.superpowers/sdd/2026-09-17-research-reliability-stage2/task-4-report.md` は末尾LF修正のみで、本文変更はない。Composition review は APPROVE（SHA-256 `c7b8d16bee044a1949795ddde03408055e1e1ff53b31436c4c788f767ba68050`）。

Task 9 の変更は、既に due time が確定している場合に不要な timestamp overflow 計算を避ける修正、未使用 `finish_agent_run` Boolean 引数の削除、ordinary successor integration test の `owns_successor` assertion、macOS の descriptor-based bounded directory traversal と fixture-root canonicalization である。`owns_successor` はテスト assertion で製品動作の変更ではない。Task 9 は checkpoint phase predicate、unknown-add quarantine、termination、promotion authority を変更していない。

Linux acceptance は isolated test root `/home/romanohu/project/pueueAgent-reliability-stage2-test.AuXm0B` の source と専用 HOME/state/Pueue profile を使用した。real Pueue daemon、trainer、callback、result manifest、evaluation、promotion をこの隔離環境で検証した。fake agent は protocol/action と harness lifecycle を検査するもので、実LLMの品質・性能を測るものではない。runner は source の期待 SHA と tracked clean status を境界で確認し、最終 Linux evidence でも clean を記録した。別の local source boundary evidence（SHA-256 `ec271e315bf593d55da0e116d4d06fb82b401f7b2014b216fc8bb6a081c8bd0c`）は Stage 2 HEAD `13c32b3` と outer HEAD `60e003a290655ef10973d61b0d944aa6aded840e` が tracked-clean だったことを示す。untracked inventory は行っていない。元 remote checkout の `4193bbce` 時点の dirty 記録は歴史的情報であり、今回再確認した状態ではない。

## 8つの受入要件群

| 要件群 | 最終 source で確認した内容 |
| --- | --- |
| 1. Due scheduling、healthy launch、disable/stop、exclusive claims、budgets | Task 1/4/5/7 の scheduler、claim、pause、health、budget の既存 coverage を保持し、最終 Linux all-target check/test を通過。Task 9 は確定済み due 状態で不要な timestamp overflow を評価しない回帰を加えた。 |
| 2. Session continuation/separation、safe reconstruction、unsafe block、finite generations/budgets | `continue` で同一 session を継続し、`missing_session` で失われた session から別 ID・新 generation を構成し、notes と budget 消費を保持した。unsafe owned session では fresh fallback を行わず block する。 |
| 3. Bounded context/evidence、privacy、observation-only metrics | native fake protocol test、Bats と real-Pueue cases を実行。bounded context、redaction、callback境界、manifest/artifactに基づく observation-only metric ingestion を含む既存テストを最終 Rust suite で再検証した。 |
| 4. Continue と confirmed stop-to-next | `continue` は exact same session、review 2件、proposal/experiment/submission各1件を確認。`stop_and_next` は source terminal、checkpoint step 180、confirmed kill 1、cycle 1、DIRECT successor 1 と durable task ID、successor checkpoint step 8、manifest metric を確認した。中断 baseline に有効 metric がないため promotion outcome は `SkippedNoMetric` であり、positive promotion とは呼ばない。 |
| 5. Stale-action refusal と failure isolation | `failure_malformed`、`failure_timeout`、`failure_cap`、`failure_unsafe_session`、`failure_unknown_kill` はすべて pass。unconfirmed kill では request failure/timeout を確認し、source を稼働のまま保って successor と cycle を作らない。 |
| 6. Verified checkpoint continuation | source checkpoint step 1 と digest `40dba0ba2e1c5789bd8e21b6851d2712adc6e0ba4f18ef766a6148d92468fd6f` を保持し、successor が checkpoint を実際にロードして step 240 まで進んだ。source digest不変、cold startでないこと、lineage、manifest loss `6.593644805093327e-06` と独立再計算 loss を確認した。中断 baseline に metric がないため promotion は `SkippedNoMetric`。 |
| 7. Restart と duplicate prevention | restart cases は各停止点で daemon の hard crash/reap と recovery を確認。`restart_stop_confirmed` は confirmed handoff 後かつ generic decision admission 前の crash を、run-ID lock による scheduler defer と600秒 retry lease を介して再開し、1 cycle/confirmed kill/successorを確認した。`stop_and_next` と `restart_stop_confirmed` では exact campaign/source の DIRECT successor が1件で、durable `pueue_task_id` が non-NULL になるまで待ってから task ID を読む。`add_reconcile` は real add の結果を抑制した後も durable `Unreconciled` quarantine を維持し、restart 後の正確な外部task観測を確認した。managed task ID/signature は NULL のままで、自動 binding や duplicate add は行わない。 |
| 8. Schema/role visibility、docs、promotion、legacy behavior | Task 7 の schema/docs/instruction bytes と visibility coverage を保持。Task 8 legacy Rust E2E では計測済み positive promotion を確認し、interrupted-baseline の `SkippedNoMetric` と区別した。Task 9 macOS portability と EOF 修正を bounded checks と Linux gates で検証した。 |

Task 8 15 research cases の pass marker は `task-9-final-case-markers.json` に記録される。Case logs のSHAは同JSONに含まれる。Interrupted-baseline時に successor metric が有効でも `current_best_experiment_id` が NULL のため promotion はされず、別の legacy scenario が positive measured promotion を検証する。

## Task 9 ローカル検証

`task-9-combined-local-evidence.json` は同じ9-path manifest と全 hash を束縛し、9 gateを記録した。Evidence SHA-256 `02c4c26abae77df05a163a34f87519f9f1c21d239fa954f169d9f7c9e42d71b0`。実行内容は `cargo clean --package pueue-agent`、`cargo check --locked --offline --all-targets`、`cargo test --locked --offline --lib --no-run`、macOS named 15-test set、recursive-cap、owned-due、session、ordinary-successor、genuine-overflow regressions である。macOS bounded set は libtest binary の直接起動で、外側の libtest 集計 `15 passed; 0 failed` を採用し、子プロセスの個別集計は加算しない。対象15 test は `agent::tests::{bound_cleanup_termination_error_retains_child_and_original_post_marker_intent,ownership_loss_remains_sticky_across_timeout_paths,poll_reap_ownership_loss_remains_sticky_across_timeout_paths,terminal_group_cleanup_error_retains_db_state_and_same_handle_for_retry,timeout_termination_error_retains_db_state_and_same_handle_for_retry,wait_reap_ownership_loss_remains_sticky_across_timeout_paths}`、`campaign::tests::candidate_submission_cwd_is_descriptor_bound_to_the_candidate_root`、`code_change::tests::{bound_terminal_result_rejects_in_place_manifest_mutation,bound_terminal_result_rejects_same_status_manifest_replacement,git_config_proof_rejects_execution_channels_and_wrong_worktree,git_metadata_reads_are_offset_independent_and_nonempty,git_pointer_target_types_are_enforced,ignored_status_walk_does_not_follow_symlinked_directories,terminal_result_output_ingestion_classifies_missing_and_invalid_results,terminal_result_outputs_allow_only_bound_manifest_and_artifacts}`。5 focused regressions は `ignored_status_walk_fails_closed_on_recursive_bounds`、`research_schedule_ignores_overflow_when_due_state_is_already_owned`、`research_finish_agent_run_rejects_context_replacement_after_confirmation`、`ordinary_research_proposal_applies_one_successor_and_replays_without_duplication`、`due_time_reports_timestamp_overflow`。UTC `Z` と `+00:00` 表記差の `code_change_candidate_commit_uses_fixed_identity_and_message` は最終 Linux Rust suite で pass した。最初の sandbox 実行では native process-group probe の EPERM により14/15だったが、同じ binary の許可された native 実行では15/15。これは full macOS all-target test pass を意味しない。

## 統合候補の Linux gates

Runner scripts は `task9-final-linux.sh`（SHA-256 `3985da580cc5b3e75019ca3e420713082b852080dfb5da225d738bd9f63de2f7`）と `task8-acceptance-linux.sh`（SHA-256 `68651e7539c5d2a4a474c1064eaa0a43aeb76e18df01ed2ac3e5ea6ba78b1843`）。各 gateのコマンドと終了値:

| Gate | 実行内容 | 結果 |
| --- | --- | --- |
| Task 9 `fmt-all` | `cargo fmt --all -- --check` | exit 1、format diagnostics |
| Task 9 `fmt-focused` | Stage 1 baselineからcandidateまでのchanged Rust pathsに `rustfmt --edition 2021 --check` | exit 1、format diagnostics |
| Task 9 `check` | `cargo check --locked --offline --all-targets` | exit 0 |
| Task 9 `check-release` | `cargo check --locked --offline --all-targets --release` | exit 0 |
| Task 9 `rust-all` | `cargo test --locked --offline --all-targets --no-fail-fast -- --test-threads=1` | exit 0; 32 targets, 1957 passed, 0 failed, 15 ignored |
| Task 9 `bash` | `bash -n install.sh bin/pueue-agent tests/e2e/run.sh tests/e2e/rust_supervisor.sh tests/e2e/research_supervisor.sh tests/support/fake_agent.sh tests/support/fake_codex.sh` | exit 0 |
| Task 9 `python` | `python3 -B -m unittest discover -s tests/e2e/research_experiment -p 'test_*.py'` | exit 0; 10 tests passed |
| Task 9 `diff` | `git diff --check 7868455472371688deb6151589658690c26033d8 13c32b3c3be6acd53a6bcab30d9820423c580a53 --` | exit 0 |
| Task 8 `native-fake` | `cargo test --locked --offline --test research_agent research_fake_protocol_parses_all_actions_from_real_available_support -- --exact --nocapture --test-threads=1` | exit 0; 1 passed |
| Task 8 `bats` | `"$TASK_ROOT/bats-core/bin/bats" tests/test_shell_entrypoints.bats` | exit 0; 25 passed |
| Task 8 `continue` | `bash tests/e2e/research_supervisor.sh --case continue` | exit 0 |
| Task 8 `stop_and_next` | `bash tests/e2e/research_supervisor.sh --case stop_and_next` | exit 0 |
| Task 8 `checkpoint` | `bash tests/e2e/research_supervisor.sh --case checkpoint` | exit 0 |
| Task 8 `missing_session` | `bash tests/e2e/research_supervisor.sh --case missing_session` | exit 0 |
| Task 8 `restart_review_running` | `bash tests/e2e/research_supervisor.sh --case restart_review_running` | exit 0 |
| Task 8 `restart_answer_ready` | `bash tests/e2e/research_supervisor.sh --case restart_answer_ready` | exit 0 |
| Task 8 `restart_stop_pending` | `bash tests/e2e/research_supervisor.sh --case restart_stop_pending` | exit 0 |
| Task 8 `restart_stop_confirmed` | `bash tests/e2e/research_supervisor.sh --case restart_stop_confirmed` | exit 0 |
| Task 8 `restart_successor_submitting` | `bash tests/e2e/research_supervisor.sh --case restart_successor_submitting` | exit 0 |
| Task 8 `add_reconcile` | `bash tests/e2e/research_supervisor.sh --case add_reconcile` | exit 0 |
| Task 8 `failure_malformed` | `bash tests/e2e/research_supervisor.sh --case failure_malformed` | exit 0 |
| Task 8 `failure_timeout` | `bash tests/e2e/research_supervisor.sh --case failure_timeout` | exit 0 |
| Task 8 `failure_cap` | `bash tests/e2e/research_supervisor.sh --case failure_cap` | exit 0 |
| Task 8 `failure_unsafe_session` | `bash tests/e2e/research_supervisor.sh --case failure_unsafe_session` | exit 0 |
| Task 8 `failure_unknown_kill` | `bash tests/e2e/research_supervisor.sh --case failure_unknown_kill` | exit 0 |
| Task 8 `legacy` | `bash tests/e2e/rust_supervisor.sh` | exit 0; `Rust E2E PASS` |

Total: 26 gate invocations. Twenty-four return exit 0; the two formatter checks return exit 1 and are recorded as diagnostics, not successful gates. Linux evidence records `runner_exit=0`, `accepted=true`, and clean source at the runner boundary.

Rust summary evidence SHA-256 `66df12aa58358c11915ac78ad770013f9f6717203a98b8de429e8432177daf36`. It reports 32 Cargo-framed targets, `1957 passed; 0 failed; 15 ignored` (13 lib fixtures and 2 daemon fixtures; count audit SHA-256 `265eda570b340dd6a1b88279ee076403f956bc632fa970ebd462ef86166ef03c`). A raw scan saw 46 summaries / 1971 passes including 14 nested one-test summaries; those nested values are not added to the Cargo total. UTC regression and final Rust log SHA-256 `54c655b4ffc07530f67a94cb9162f080f4f3332c104cf2c6b6d3840ee1f7147a`.

Formatter summary distinguishes output counts from changed-file counts. `fmt-all` exit 1 emitted 2738 raw diagnostic sections over 88 path spellings, with diagnostics in 49 changed files and 9 added files (474 raw added-file sections). `fmt-focused` exit 1 emitted 4636 raw sections over 75 path spellings, again intersecting 49 changed and 9 added files (1032 raw added-file sections). Raw sections may repeat across recursive module checks; they are not unique hunks. Path spellings are not changed-file counts. Both inherited and newly added files have formatting diagnostics; no broad reformat was applied.

## Linux 環境と証拠

最終 Linux tool versions evidence SHA-256 `4664a266ab5f673bbcec7d51b6a766692a169fc37a3be46b8ef9bd54b655e39d`: Linux host `roko` (hostname evidence SHA-256 `4eb7d6f72c06f335bfb7fcb3a8c7a3d1c126aea892fba2c2ce91ad7f827e113d`), kernel `6.18.7-76061807-generic`; rustc `1.97.1 (8bab26f4f 2026-07-14)`; cargo `1.97.0 (c980f4866 2026-06-30)`; rustfmt `1.9.0`; pueue/pueued `4.0.4`; Python `3.12.3`; pytest `9.1.1`; SQLite `3.53.3`; Bats `1.14.0`; Git `2.50.1`; jq `1.7.1`.

Cleanup evidence confirms all 15 exact research WORK roots absent, legacy WORK `/tmp/pa-rust-e2e.ICK7we` absent, and captured runner/legacy/pueued process identities absent. This is scoped to those exact roots and identities; it is not a host-wide process claim. Final Linux evidence, final summary, version record, cleanup record, case markers, local source boundary record, and runner log hashes are recorded above. Artifact collection and cleanup did not run tests against the original Pueue service or the original remote checkout.

## 制限

- Full macOS all-target suite は実行しておらず、pass と主張しない。macOS evidence は bounded named 15-test set と focused checks。
- `PUEUE_AGENT_HEALTH_E2E=1` full optional health scenario、GPU OOM/実GPU health は未実行。
- fake agent は protocol/action と lifecycle を検査し、実LLM品質・性能を測定しない。
- Interrupted-baseline `SkippedNoMetric` は正しい promotion policy outcome であり、別の legacy positive-promotion scenario と区別する。
- `add_reconcile` の unknown add は durable `Unreconciled` quarantine のまま。managed task ID/signature は NULLで、自動 binding されない。
- 外側/main checkout の未追跡 `tests/e2e/learning_experiment/` は読まず、編集していない。Linux isolated runner では、事前承認された4つの committed fixture のみを実行した。
- 元 remote checkout の `4193bbce` 時点の dirty 状態は歴史的記録のみで、再確認していない。Linux run の clean claim は isolated test source の期待 SHA/runner境界に限定される。
