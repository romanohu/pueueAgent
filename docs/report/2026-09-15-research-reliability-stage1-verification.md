# 自律研究の信頼性改善・第1段階の検証記録

更新日: 2026-09-17。第1段階の実装・コードレビューとLinux実学習E2Eを完了した。全テスト成功という意味ではなく、下記の既知失敗と検証範囲を留保する。

対象設計: [第1段階](../superpowers/specs/2026-09-15-research-reliability-stage1-design.md)。
対象branch: `codex/research-reliability-stage1`。merge/pushは行っていない。

## 最終結果

- role指示の整合、preview/token/apply/backup付き指示更新、実CPU学習の回帰テストを実装した。第2段階以降には着手していない。
- 全体E2Eは`4f77222`で`Rust E2E PASS`、終了0。実測lossは`0.97334875433718737`から`3.0985984047920148e-06`へ改善し、候補・bestの対応と再起動後の重複防止も成功した。
- 最終実装修正を含む`e6821cc`ではdebug/release check成功、全Rust1485成功・1失敗・14ignore、Bats21/21成功。失敗1件は基準commitでも確認したUTC表記の比較で、未修正。全体fmtにも既存差分が残る。
- 最終レビューのCritical/Importantは解消。既存backup再利用時の専用回帰テストを復元するMinorを1件残した。詳細は末尾に記載。
- E2Eは最終コミットより前の固定スナップショットで実行した。E2E本体・fixtureは同一で、その後の製品差分は指示更新の親directory同期処理のみ。最終コミットの指示更新は別のLinux単体・統合テストで確認した。

以下は途中の失敗も含む時系列の検証記録。

## 検証環境と保護範囲

- 正式な実行対象はLinux。roko上の独立コピー
  `/home/romanohu/project/pueueAgent-reliability-stage1-test.iW9UBT/source`と`source-task2`を使い、ビルド先を分離して検証した。
- 既存の`/home/romanohu/project/pueueAgent`は変更・未追跡ファイルが59件あったため、上書き・clean・branch変更を行っていない。
- リモートのテストは`umask 077`で実行する。既存サービス・実験は操作せず、E2Eが作る専用Pueue profileとプロセスだけを対象とする。
- Linux: Cargo1.97.0、Rust1.97.1、Pueue/pueued4.0.4、Python3.12.3、Git2.50.1、Bats1.14.0。
- pytest9.1.1と依存パッケージは独立テスト領域の専用venvに導入した。既存E2Eによる実行ファイルのコピー後もvenvを保持するため、固定の専用Pythonをexecするネイティブランチャーをテストホスト側に配置した。実際のPython/pytestを使い、結果やmetricsを生成する代用品ではない。製品への追加ではない。

## 変更前から存在する検証上の問題

基準commit `3b80ad9`のLinux全Rustテスト:

```text
cargo test --locked --offline --quiet --all-targets --no-fail-fast -- --test-threads=1
1448 passed / 2 failed / 14 ignored（28 targets、終了コード101）
```

失敗した2件:

1. `phase_2_campaign_documentation_covers_autonomous_terminal_loop_and_phase_3_boundary`:
   READMEの更新前のquickstartを期待していた。Task1で現行ドキュメントに合わせた。
2. `code_change_candidate_commit_uses_fixed_identity_and_message`:
   固定UTC時刻の表示が`2000-01-01T00:00:00Z`で、期待値は同じ時刻の`2000-01-01T00:00:00+00:00`。
   この既存失敗は未修正。Gitのバージョンが原因だとは確認していない。

`cargo fmt --check`も既存ファイル多数のformat差分で失敗する。無関係な一括formatは行わない。
`environment.rs`の`OptionalOpenStep::EntryMountPrecheck`に既存のdead-code warningがある。

macOSでは既存cli_helpの3件が`policy_blocked:anchor_missing`、initの1件が`policy_blocked:policy_unreadable`で失敗する。
これら4件は、安全なpermissionsを持つLinuxテストコピーでは再現しない。安全チェックは緩和していない。

## Task1: 役割別指示

実装commit: `656950c`、レビュー修正: `69d1189`。

Linux `656950c`で次を確認した（すべて終了コード0）:

| コマンド（先頭は`cargo test --locked --offline`） | 結果 |
| --- | --- |
| `--no-fail-fast --test init --test config --test cli_help -- --test-threads=1` | init23 / config37 / cli_help45件成功 |
| `--test scheduler prompt -- --test-threads=1` | 10件成功 |
| `--lib launch_prompt -- --test-threads=1` | 2件成功 |
| `--lib editor_prompt -- --test-threads=1` | 1件成功 |

レビューで共通Dispatch modeの修正指示とmanaged Standardの編集禁止との矛盾を指摘された。
`69d1189`で修正を非managed Standardに限定し、initが生成する指示本文の関係を検査する回帰テストを追加した。
再レビューは2件とも解消、新しい問題なし。
同commitのLinuxで`--test init instructions`が5件、`--test cli_help phase_2_campaign_documentation`が1件成功した。

ローカルでも対象のRED/GREEN、`cargo check --all-targets`、`git diff --check`、旧版テンプレートと`5d1a8e0`のbyte一致を確認した。
非managed promptの全文一致テストを維持している。

ここで検証したのは生成された指示とproduction promptの契約であり、実LLMが必ず指示に従うという検証ではない。

同じ`69d1189`のLinuxでBatsは15件成功した。
最初の既存E2Eは`policy_blocked:anchor_replaced`で、MLシナリオ前に停止した。
この試行中にBats内のCargoテストを並行実行したため、検証中のlauncherが再リンクされた可能性を切り分けている。
製品の退行とも環境原因とも断定せず、E2E単独で再試行した。以降はE2E中に同じtargetのbuild/testやsource更新を行わない。
単独再試行では前回の停止地点を通過し、既存のコード変更・実pytest・候補実験・promotionの成功ケースが完了した。
最初に差し替わったanchorのidentityは初回cleanup後には確認できないため、並行ビルドが原因であることの断定は避ける。
既存の`PUEUE_AGENT_HEALTH_E2E=1`で有効になる長時間healthシナリオは、この通常E2E実行には含まれない。

単独再試行の最終結果は終了コード1。コード変更の成功・check失敗・実行時OOM相当/内部エラー、goal、repairの根拠有無、finite wait、不正decisionの各ケースを通過した後、callback903の復旧確認付近のdaemon停止で`policy_blocked:native_gate_failed`となった。
この試行はTask1反映後の`69d1189`で行ったもので、第1段階着手前の`3b80ad9`ではない。
したがって「Task3追加前にも発生」は確認済みだが、第1段階全体から見た既存不具合か退行かは未確定。
最後のrunは`post_marker`段階の失敗で、実行ファイルのinodeは実行中に確認した値と同じだった。
初回のanchor差し替えとは分けて原因調査中であり、既存E2E全体の成功はまだ確認できていない。
`post_marker`は保守的な失敗分類で、実際に対象プロセス作成やmarker公開が完了した証拠ではない。
既存markerによる起動前拒否も含まれる。同一eventの同一秒内再試行によるlog/marker名の再利用は調査候補だが、失敗時の一時領域がcleanup済みのため原因は断定できない。TERMとの因果関係も未確認。

## 残る受入ゲート

- Task2: 残るテスト品質上の保留事項を最終レビューで確認。
- Task3: 実測学習lossを使うreal-Pueue E2E、再起動時の一意性、保護対象の不変性。
- 最終commitでのLinux build/test/Bats/E2Eと全branchレビュー。

長期sessionを使った定期判断、初回導入支援、製品としての評価条件固定、OS隔離・時間予算は後続段階であり、本段階の検証済み機能には含めない。

## Task2: 指示更新コマンド

初回実装commit: `4ee61b7`。
E2E中の実行ファイルを差し替えないよう、Linux検証は同じ独立テスト領域の`source-task2` cloneと別targetで実施した。

```text
cargo test --locked --offline --no-fail-fast --test instructions --test init --test cli_help -- --test-threads=1
instructions12 / init23 / cli_help45件成功

cargo test --locked --offline --lib instructions -- --test-threads=1
9件成功

cargo check --locked --offline --all-targets
成功（既存のEntryMountPrecheck warningあり）
```

これらのテストは成功したが、独立レビューで以下の不足が見つかった。

- previewに実際の追加・削除内容が含まれていない。
- 未知版や壊れた追加markerを一部見逃す。
- 既存backup再利用時にfile/directoryの同期を再確認しない。
- 一時ファイルが差し替わった場合のcleanupが、別のファイルを削除し得る。
- 待機中に差し替わったlockのidentityを、取得後に再検証しない。

`0e0589a`で5件を修正した。Linuxでinstructions統合テスト14件、関連libraryテスト14件が単一threadで成功し、`cargo check --locked --offline --all-targets`も成功した。差分限定の再レビューは5件すべて解消と判定した。
一部library回帰テストの初回REDは未実装hookによるコンパイルエラーで、修正前の誤動作を実行で示したものではない。

追加検証では、同じcommitの並列実行で新しい問題を確認した:

```text
timeout 30s cargo test --locked --offline --lib instructions -- --test-threads=4
12 passed / 2 failed（終了コード101、timeoutではない）
```

`candidate_cleanup_retains_a_replacement_at_the_named_path`と
`publication_sync_fault_reports_uncertainty_and_retains_backup`が失敗した。
process-globalなテスト用差し替えhookを別テストが消費し、後者の候補内容が`replacement candidate`になっていた。
製品ビルドにはないテストhookの干渉だが、通常の並列テストを壊す退行のため、Task2を再開して修正する。

`34bb409`で両hookをthread-localにし、lock通知は実際のupdater worker内で設定するよう修正した。
他threadにhookが漏れないことを確かめる2つの回帰テストで修正前の失敗を確認し、修正後はローカル・Linux双方で16件成功した。
Linuxの正確なコマンドは`cargo test --locked --offline --lib instructions -- --test-threads=4`と`--test-threads=1`で、両方終了コード0。
差分限定再レビューも解消・新規問題なしと判定した。

ほかにbackup symlinkテストのparent permission明示と、lock待機テストのjoin時間上限がレビュー保留事項として残る。最終レビューで扱う。

## Task3: 実測学習 E2Eの実装と修正履歴

実装commit: `cb2e828`。
固定データの小さな線形モデルを CPU で学習し、学習率 `0.001` から `0.05` への変更による held-out MSE 改善を検査する fixture を追加した。
E2E は通常の submit から候補実験・評価・best 更新を通し、再起動前後の実験 ID、タスク数、promotion 数と best SHA を比較する。
LLM の判断・編集案だけが制御された test double で、学習 loss を固定値として注入する方式ではない。

同commitでコントローラが次を実行し、成功を確認した:

```text
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests/e2e/learning_experiment -p 'test_*.py'
6 tests / OK
bash -n tests/e2e/rust_supervisor.sh tests/support/fake_codex.sh
git diff --check
両方終了コード0
```

初回実装時点では、実装担当はBats16件とcargo check成功を報告し、Linux E2Eは未実行だった。
Linuxの専用Python/pytestでもbaseline fixtureの6件が成功した（`34bb409`）。
担当者のmacOS全対象Cargo実行では7targetが失敗したが、事前に確認した4件以外まで既存失敗と断定できる比較証拠はないため、すべてを「既知の環境問題」とは分類しない。
Linux正式環境での最終結果と分けて扱う。

レビューで、再起動後の照合処理を正に観測していない点と、subprocessの不変性テストがmodel.pyしか保護していない点を指摘された。
追加で、candidateの学習率変更後はテストのbaseline比較側も同じ学習率になり、必ず比較が失敗する問題を確認した。
この3点を修正・再レビューした後にLinux E2Eへ進んだ。

`c000fc0`で3点を修正し、差分限定再レビューはすべて解消と判定した。
比較テストはbaseline学習率を明示的に固定し、実際に学習率だけを変更した候補コピーでも6件成功した。
再起動時には両タスクの`task_observations.observed_at`が進んだことを待ち、daemon停止の完了後に不変条件を確認する。

## Linux 受入検証（`c000fc0`）

| 検査 | 結果 |
| --- | --- |
| `cargo check --locked --offline --all-targets` | 成功 |
| 同 `--release` | 成功 |
| `cargo test --locked --offline --quiet --all-targets --no-fail-fast -- --test-threads=1` | 29 targets、1485成功 / 1失敗 / 14ignore、終了101 |
| Bats `tests/test_shell_entrypoints.bats` | 16/16成功 |
| 専用Python/pytest、baseline fixture | 6/6成功 |
| shell構文 / `git diff --check` | 成功 |
| 新規Rust2ファイルのrustfmt check | 成功 |
| 全体 `cargo fmt --check` | 既存の広範なformat差分により失敗 |

全Rustテストの唯一の失敗は、基準commitと同じUTC表記のassertion。
release checkでは`promotion.rs`の未使用fixture field warningも出たが、該当fieldは基準commitから変わっていない。

real-Pueue E2Eは同commitで実行した。専用WORKは`/tmp/pa-rust-e2e.17OTRL`だった。
実行中は同じsource/targetの更新やCargo/Batsを行わなかった。
既存debug helperのlifecycle traceを遅延`0,0,0`で有効化し、起動失敗時の段階を専用領域に記録する。
開始後に確認した稼働中daemonとディスク上launcherのinodeは両方`39733074`で一致した。
既存の成功・再チェック失敗・実行時OOM相当/内部エラーのコード変更ケースを通過した後、新しい実学習baselineはタスク12として成功し、loss `0.97334875433718737`を記録した。

その後、学習用editorの配線不備で候補投入に失敗した。実際に起動したのは`fake-agent`だったが、学習率を編集する処理は`fake_codex`側にしか追加されていなかった。
2回のeditor実行は終了コード0でも、返したチェック内容が適合せず`editor_output_invalid`となり、候補runは`rejected`で確定した。
これは今回の追加テストの不具合であり、既存問題とは分類しない。

到達不可能になった待機を打ち切るため、今回のrunnerだけをPID/argv確認後にTERMで停止した（終了143）。
専用プロセスは停止したが、想定した失敗時保持が働かずWORKは削除された。元の診断一時ファイルは復元できない。
停止前のDB観測は記録済み。signal停止時にも診断情報を残す回帰テストを追加する。
既存roko checkout・既存実験のファイルは削除していない。

この時点では実測学習候補のpromotion・再起動不変条件とE2E全体の成功は未確認で、配線とsignal処理を修正して再検証した。

### 配線修正後の検証

`4f77222`で実際の`fake-agent`に学習用editor処理を移し、許可済みPythonで学習率の1行だけを編集するようにした。
使われていなかった`fake_codex`側のeditor処理と、許可PATHに存在しないsed依存は除去した。
同commitのLinux Batsは18/18成功（終了0）。狭いPATHで実際のeditor出力を確認する回帰テストを含む。

TERM/INT時のhandlerは明示的に143/130をcleanupへ渡すようにしたが、追加したsignalテスト自体に待機上限・失敗時後始末・INTの検証不足が見つかった。
`86406bf`で後始末を追加し、残るINTと失敗経路の検証は後述の修正で補った。正常なTERMの1ケースが通っただけでsignal全体を検証済みとは扱わなかった。

隔離コピーの正確な`4f77222`で、新しい学習ケースだけを先に実行し、終了0を確認した。
既存harnessのsetupと`run_learning_case`を変更せず選択する診断実行であり、全E2Eの合格判定とは別に記録する。

| 実測項目 | 結果 |
| --- | --- |
| baseline loss | `0.97334875433718737` |
| candidate loss | `3.0985984047920148e-06` |
| candidate SHA / best SHA | 両方`6b39946d207b160ee0c86380f1695a4dad78da53` |
| 編集・実check・候補投入・promotion | 成功 |
| 再起動後の新しい照合観測 | 成功 |
| タスク数・実験ID・promotion数・bestの不変性 | 成功 |
| 学習率以外のfixtureと元リポジトリ保護 | 成功 |

同commitで`tests/e2e/run.sh`全体を実行した。判断と編集のtest doubleを、実LLMの研究能力を検証した結果とは扱わない。

### signal回帰テストの追加修正

`c6c8885`で専用process groupの監督とBats teardownを追加し、TERM・INT・準備待ち失敗時の後始末を検証した。
レビューで、監督側が期待する終了コードを合成し、本体の誤った終了コードを隠していた問題を検出した。
`600c1ec`で実際の子プロセスの終了コードを返すよう修正した。意図的に42を返す子を使い、修正前の失敗と修正後の成功を確認した。
対象4件と差分再レビューは成功。コントローラのローカルBats全体も21/21成功した。
この再レビューで残ったsignal handler準備完了待ちのMinorは、後述の最終修正で解消した。

`600c1ec`のLinux全Rust検証は、E2Eとは別の未使用の隔離コピー`source`と独立した`source/target`で実行した。
E2E中の`source-task2`とその実行ファイルには変更を加えていない。
通常/releaseのcheckは成功。全Rustは29targets、1485成功・1失敗・14ignore、終了101だった。
唯一の失敗は基準commitと同じ`daemon::code_change_candidate_commit_uses_fixed_identity_and_message`のUTC表記比較。
全体fmtは既存差分により終了1、新規Rust2ファイルのfmt・Python fixture6件・shell構文・diffcheckは成功した。

### 最終レビュー

全体レビューでCriticalはなし。Importantとして、backup directory作成後の親directory同期が失敗すると、再試行で親同期を飛ばして原本を置換し得る順序上の不備を検出した。
実際のクラッシュによる消失を再現したという意味ではなく、永続化確認の順序から確認した問題である。
`fd30526`でこの修正と、backup symlinkテストのpermission、lock待機テストの完了timeout、signalテストの子handler準備待ちの3点を修正した。
差分再レビューは4点とも解消、Critical/Importantなしと判定した。

非ブロッキングの残件: 新しい親directory同期失敗テストが、既存backupファイル再利用時に`BackupDirectorySync`失敗を繰り返す専用テストを置き換えた。
製品側の既存backup同期処理は維持され、一般の同期失敗テストも残るが、この専用の再利用テストは復元候補として記録する。
機能不具合の実証ではないため、追加の修正反復は行わない。

実装修正を含む`e6821cc`でLinuxの最終check・全Rust・Batsを再実行した。学習E2E側の実行中スナップショットは`4f77222`のまま維持した。
両commitのE2E本体、fake agent、learning fixtureは`git diff --quiet`で一致を確認した。
その間の製品Rust差分は`instructions_file.rs`の最終修正のみであり、最終コミットの単体・統合テストで別途検証する。
E2E全体を最終コミットで実行済みとは表現しない。

| 最終Linux検査（`e6821cc`） | 結果 |
| --- | --- |
| debug / release `cargo check --locked --offline --all-targets` | 両方成功 |
| `cargo test --locked --offline --lib instructions -- --test-threads=4` | 16/16成功 |
| 全Rust `--no-fail-fast -- --test-threads=1` | 29targets、1485成功・1失敗・14ignore、終了101 |
| Bats | 21/21成功、終了0 |
| 専用Python/pytestの学習fixture | 6/6成功 |
| 変更Rust3ファイルのfmt / shell構文 / diffcheck | 成功 |
| 全体fmt | 基準からあるformat差分で終了1 |

全Rustの失敗は引き続きUTCの`Z`と`+00:00`の文字列比較だけで、今回の追加機能の失敗ではない。
チェック終了後の隔離コピーは`e6821cc`でcleanだった。

## 全体E2Eの最終結果（`4f77222`）

`tests/e2e/run.sh`は`Rust E2E PASS`、終了0。既存の候補成功・check失敗・OOM相当/内部エラー、実学習、goal review、修復/非修復判断、有限wait、不正判断3回の上限、callback重複/欠落、fatal log停止、実行失敗のretry、回数上限とresume、期限切れclaim復旧、Codex継続とnetwork/credential境界、インストール検証まで通過した。
optionalな`PUEUE_AGENT_HEALTH_E2E=1`の追加シナリオ、実GPU OOM、実LLMの研究能力は今回の実行範囲に含めない。

全体実行の学習task12/13のlossは上記と一致し、候補SHAは`73d590140e32a1c7e8858076f415e2c4721ea859`、promotionは`improved`、current bestは候補だった。
以前の終盤`native_gate_failed`は今回再発しなかったが、失われた過去の診断情報から原因を確定・修正したとは主張しない。

終了後、専用WORK `/tmp/pa-rust-e2e.71mrDk`は成功時cleanupで削除され、専用pueuedも残っていないことを確認した。ログと実測値は検証記録へ保存済み。隔離ソースとビルド成果物はrokoの専用テスト領域に残した。
既存roko checkoutは引き続き`4193bbce`・変更/未追跡59件で、変更・clean・サービス停止はしていない。
ローカルmainにある未追跡`tests/e2e/learning_experiment/test_model.py`も今回の作業ブランチへ混ぜず、変更・削除していない。
