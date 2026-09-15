# 自律研究の信頼性改善：第1段階の設計

日付: 2026-09-15<br>
状態: 口頭合意を文書化、実装前のユーザーレビュー待ち<br>
基準: `main` at `5d1a8e0`<br>
対象: エージェント指示の整合性、明示的な指示更新、小規模な実学習の回帰テスト

## 1. 全体の導入順序

ユーザーが承認した6項目を、次の単位に分けて導入する。第1段階の完了を、全項目の完了とは扱わない。後続段階はこの文書ではロードマップであり、個別の設計・受入条件を確定してから実装する。

| 段階 | 導入するもの | 完了時に確認すること |
| --- | --- | --- |
| 1（本設計） | 指示の整合性修正、明示的な更新、実学習の回帰テスト | 指示と実際の権限が一致し、実測値で候補の改善を検証できる |
| 2 | 会話と研究履歴を継続する定期判断 | 継続・打ち切り・checkpoint・次候補の判断をsupervisorが安全に実行できる |
| 3 | 初回submit前の導入支援 | 環境・データ・評価結果出力を短時間の試走で確認できる |
| 4 | 評価条件の固定と再現確認 | best候補を同じ評価条件で再実行し、単発の改善と区別できる |
| 5 | Phase 6のOS隔離と時間予算 | 書き込み範囲・秘密情報・実行資源を制限し、時間でも実験を制御できる |

回帰テストは段階1だけでなく各段階で拡張する。不特定のコードを無人実行する運用の拡大は、段階5の実行隔離を前提とする。段階1〜4の試験は既存のtrusted-native境界内に限定する。

継続する条件:

- Linuxを正式な実行・検証対象とする。
- networkの既定許可は維持する。認証情報の継承許可とは区別する。
- SQLiteの目的・予算・lineageを正本とし、agentや指示ファイルから変更させない。
- MLリポジトリ固有の中間controllerを要求しない。
- MLリポジトリのmainへの自動merge/pushは導入しない。
- rokoはテスト専用とし、既存の実験やサービスには干渉しない。

## 2. 現状の根拠と問題

- [instructions.md](../../../templates/instructions.md)は「Phase 2 decision agent」「proposalまたはwait」「code changeを提案しない」という古い制限を含む。
- [agent.rs](../../../src/agent.rs)の実際のdecision出力スキーマには`goal_reached`と`code_change`があり、decision / diagnosisはそれぞれ専用の起動promptを持つ。古いテンプレートだけを原因に、現行のコード変更ループが動かないとは断定しない。
- [scheduler.rs](../../../src/scheduler.rs)の通常agent用promptは、プロジェクトの`.pueue-agent/instructions.md`を読むよう指示する。通常agentと専用roleの責任を明確に分ける必要がある。
- [init.rs](../../../src/init.rs)は存在する指示ファイルを保存し、初期化済みプロジェクトへの再initも拒否する。配布テンプレートを修正するだけでは既存プロジェクトに反映されない。
- [既存E2E](../../../tests/e2e/rust_supervisor.sh)にはreal Pueueを使った候補編集・check・実験・promotionの検証がある。ただしPython fixtureは`score()`に応じて固定lossを返し、学習計算そのものは行っていない。

## 3. 採用する方針

「新規init用テンプレートだけ修正」は既存利用者に届かず、「upgrade時に自動上書き」は独自指示を破壊する。このため、ユーザーが承認した**差分プレビュー、明示的な適用、バックアップ**を採用する。

新しい汎用テンプレート管理機構や、任意文書の自動マージ機構は作らない。指示ファイル1種類について、認識できる配布部分だけを更新する。既存の起動経路、CLI、セキュアなファイル操作、テスト基盤を再利用する。

## 4. 役割別の指示契約

| role | 許可する仕事 | 禁止・維持する境界 |
| --- | --- | --- |
| 通常agent / Periodic DeepCheck | 調査、限定された事実・推奨のscratch記録、人の追加指示の参照 | managed campaignではsourceの直接編集・commit・直接submitをしない。単独で達成やcampaign終了を確定しない |
| decision | supplied schemaに従う1つの`proposal` / 有限`wait` / 根拠付き`goal_reached` | project、scratch、Git、artifactを変更せず、Pueueを直接操作しない |
| diagnosis | supplied schemaに従う`continue` / `kill_and_resume` / `kill_and_escalate` | 自分ではkill、再投入、コード修正をしない |
| code-change editor | 指定candidate worktreeの編集と構造化された編集結果・check提案 | commit/ref更新/checkの最終認定/実験投入はsupervisorが所有する |

`code_change`はdecisionが返せるproposalであり、decision自身に編集権限を与えるものではない。goalの最終承認は既存のhuman reviewを維持する。

通常agentの非managed経路の既存権限は、この変更で広げたり一律に削除したりしない。古い「sourceを修正してcommit」という包括的な指示を、roleとcampaignの有無を区別する記述へ置き換える。

sessionの規則は変更しない。通常agentはproject設定、decision / diagnosisはfresh、editorは初回freshと同一sessionでの最大1回の修正である。研究担当の長期sessionは段階2で扱う。

配布テンプレートと実際のrole promptをそれぞれ検証する。専用promptにプロジェクトの任意指示全文を新たに埋め込む変更や、動的なprompt frameworkは導入しない。出力schemaとservice policyをprojectの指示で上書きできないことを維持する。

## 5. 指示ファイルの更新インターフェース

以下は追加予定のCLIであり、基準commitには存在しない。

```text
pueue-agent instructions update [PROJECT_ROOT]
pueue-agent instructions update --apply <PREVIEW_TOKEN> [PROJECT_ROOT]
```

既存CLIと同様、省略時のproject rootは現在のディレクトリとする。daemonや外部LLMを起動せず、初期化済みのローカルプロジェクトを対象とする。

### 5.1 プレビュー

- 引数なしのupdateは読み取り専用。ファイル、バックアップ、SQLite、Git、Pueueを変更しない。
- 状態、置換対象、差分、適用token、次のコマンドを表示する。既存のbounded/redacted出力規則を使い、独自指示やcredentialをそのまま監査ログへ流さない。
- tokenはcanonical project root、更新前の全ファイルbytes、更新後の全ファイルbytesに結び付くSHA-256とする。区切りの曖昧さがない構造でdigestを計算する。tokenは編集競合を検出するための値で、認証情報ではない。
- inputと更新後の候補はUTF-8、64 KiB以下。入力不正や上限超過では書き込まない。表示が省略・redactされた場合はその旨を示し、全文の確認はローカルファイルで行う。

### 5.2 更新できる範囲

- 基準commit `5d1a8e0` の配布テンプレートを既知の旧版として固定する。
- 旧版全体が連続した完全一致のbytesとしてちょうど1回存在する場合、その範囲だけを新版に置き換える。前後に追記された独自指示はbytes単位で保存する。
- 新版は配布部分を`<!-- pueue-agent:instructions v1 begin -->`と`<!-- pueue-agent:instructions v1 end -->`で囲み、独自指示はその外側に記述する。既知の配布内容との完全一致も検証し、markerだけを信用して内容を破棄しない。
- すでに新版であれば`current`として終了する。独自指示があっても配布部分が同じなら更新しない。
- 配布本文の途中が編集されている、未知の旧版、複数の配布部分、壊れたmarkerなどは`conflict`とし、自動の推測マージや旧文言の一括置換をしない。
- conflict時は配布部分と独自指示を利用者が手動で整理する手順を示す。CLIが未知の文章を捨てたり、コメント扱いにしたりしない。
- 指示ファイルが欠けている場合も暗黙に新規作成せず、復旧手順を示す。新規プロジェクトは通常のinitで新版を作る。

### 5.3 明示的な適用と復旧

- `--apply`は必ずpreview tokenを要求する。対象を読み直し、旧bytesと候補bytesから計算したtokenが一致しなければ、書き込み前に`conflict`とする。
- 更新コマンド間をロックで直列化する。原本、親ディレクトリ、backup先の型・所有者・identityを検証し、symlinkや原本のhardlinkを経由した書き換えを拒否する。
- 完全な原本を`.pueue-agent/instructions.backups/<OLD_SHA256>.md`へ保存する。ディレクトリは0700、新規backupと新版ファイルは0600とする。既存backupを上書きせず、同名ならbytesと安全なidentityの一致を確認する。
- backupの書き込みと永続化を確認してから、同じディレクトリの一時ファイルを使って新版全体を置き換える。原本を先にtruncateしない。適用直前にも原本のidentityとdigestを確認する。
- backup失敗、検証失敗、検出した競合では原本を置き換えない。置換後の永続化確認で失敗した場合は、適用結果が未確認であることとbackupの場所を報告し、成功や未変更と断定しない。
- 成功時は更新後digestとbackupの場所を表示する。再実行は`current`となり、backupを増殖させない。
- ロックに従わない外部エディタとの同時編集をOSレベルで完全に排除する機能ではない。更新中は別の編集を行わないよう案内し、既知の競合をfail closedにする。
- 既存agent runに遡及して指示を差し替えない。次にファイルを読むrunから反映されることを表示し、必要なら既存pause操作で新規automationを止めてから更新する手順を示す。

状態`current` / `update_available` / `updated`は終了コード0、`conflict` / unsafe path / 入出力失敗は1、CLI構文不正は既存clap規則の2とする。古いtokenで再適用した場合も、対象がすでに完全な新版なら書き込まず`current`とする。

更新はconfig、STATE.md、state.json、campaign、budget、実験、サービス設定に触れない。init、daemon起動、binary upgradeから自動適用しない。復旧はbackupを確認したうえで明示的に行い、backupの自動削除や新しい汎用rollbackコマンドは追加しない。

## 6. 実際に学習する回帰テスト

既存の固定値fixtureとfailure/recovery E2Eは残す。それとは別に、小規模なPython標準ライブラリの線形回帰fixtureを追加し、GPU・外部データ取得・追加MLライブラリなしで実行できるようにする。

- 固定の訓練データと別の固定評価データを使う。seed、初期値、更新回数を固定する。
- 学習は実際の勾配更新を行い、評価lossは学習後の予測から計算する。revisionや「候補」フラグで決めた固定値を結果として出力しない。
- baselineには学習が完了する保守的な学習率を用い、candidateは学習率だけを変更して同じ更新回数で改善するよう、fixtureの単体テストで確認する。
- 通常の`PUEUE_AGENT_EXPERIMENT_ID` / `PUEUE_AGENT_RESULT_PATH`を用いて有効なmanifestを出力する。candidateの事前作成ファイルのidentityを維持する。
- 既存fake-agent基盤で決定論的な`code_change`と編集結果を返し、supervisorがcheck、候補commit、real Pueue投入、評価、local best更新を行う。
- candidateでは訓練率以外の学習・評価処理とデータが変更されていないことをcheckする。これはfixtureのテスト条件であり、製品全体の評価固定機能を実装したとは扱わない。
- baselineとcandidateの実測値、候補SHA、metrics row、best refの対応を検証する。単に終了コード0だけで合格にしない。
- このfixtureの改善後にdaemonを再起動し、二重投入・二重promotionがなく、bestとlineageが保たれることを確認する。
- 元リポジトリのmain、source、remote、無関係なworktreeが変わらないことを確認する。

このE2Eは実学習と実際のsupervisor/Pueue連携を検証するが、判断部分はfake agentである。実LLMの研究能力や、任意MLリポジトリの成功を検証済みとは表現しない。新しいcheckpoint対応、GPUの実OOM誘発、実LLMの長期評価はこの段階に含めない。

## 7. 実装の配置と受入条件

対象は配布テンプレート、role prompt、指示更新用の小さなRust module、既存CLIへの入口、関連テスト、利用ガイドに絞る。更新の文字列判定と安全なファイル適用を分離し、schedulerやcode-change coordinatorの無関係なリファクタリングをしない。SQLite schemaの追加は不要とする。

受入条件:

1. 新規initの指示と実際のdecision/diagnosis/editor promptが、役割とschemaの許可範囲に矛盾しない。
2. managed通常agentへsource編集・commit・直接submitを指示しない。非managed経路とsession規則に意図しない変更がない。
3. previewが副作用を持たず、旧テンプレートと前後の独自指示を正しく識別する。
4. applyが承認した差分だけを適用し、原本backupと独自指示を保存する。
5. 独自編集、古いtoken、複数marker、UTF-8不正、サイズ超過、symlink/hardlink、backup失敗、同時update、書き込み途中の失敗をテストする。
6. 再実行は冪等で、既存initの「ファイルを勝手に上書きしない」契約を壊さない。
7. 実測lossを使ったbaseline→コード変更→candidate→best更新がreal Pueueで成立し、再起動でも重複しない。
8. 既存のRust/Bats/E2Eが退行しない。fake agentと実学習の区別を試験報告に記載する。
9. コマンド構文、更新の制約、backupからの手動復旧、適用タイミングを日本語ドキュメントに記載する。

## 8. 検証・統合

実装はTDDで既存worktree運用に従い、実装担当にはユーザー指定のLuna-maxを使用する。設計文書の承認後に、具体的な変更ファイルとテストを持つ実装計画を作成する。

ローカルで対象テスト・format・buildを確認し、Linux固有の受入判定はrokoで行う。同期前に`/home/romanohu/project/pueueAgent`の状態と対象を確認し、ユーザー変更を上書きしない。専用の一時state/Pueue profileを使い、本番相当の既存サービスや実験を操作しない。

Linuxの最終gate:

```bash
cargo fmt --check
cargo check --all-targets
cargo check --all-targets --release
cargo test --all-targets -- --test-threads=1
bats tests/test_shell_entrypoints.bats
tests/e2e/run.sh
```

実際に検証したcommit、環境、終了コード、テスト件数、未実行項目を記録する。macOSで実行できないLinux固有テストを、実行済みとして数えない。実装のmerge/pushは別途ユーザーの指示を得て行う。
