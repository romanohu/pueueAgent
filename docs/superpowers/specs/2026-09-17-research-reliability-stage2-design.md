# 自律研究の信頼性改善：第2段階の設計

日付: 2026-09-17<br>
状態: 会話上の方式・基本動作・復旧条件は承認済み。本設計書のレビュー待ち<br>
基準: `codex/research-reliability-stage1` at `37cfe9c`<br>
対象: campaign単位で会話と研究履歴を継続する、実験中の定期判断

## 1. 目的と承認事項

[第1段階のロードマップ](2026-09-15-research-reliability-stage1-design.md)の第2段階を具体化する。既存のPhase 2〜5という実装履歴とは別の「信頼性改善の第2段階」である。

ユーザーは次を承認した。

- 同じ目標のcampaignでは、実験を切り替えても研究担当の会話を継続する。別campaignの会話は混ぜない。
- 会話を再開できなくなった場合は、保存した研究メモと実験履歴から新しい会話を作って自動継続する。元のtranscriptの完全復元とは扱わない。
- 既存の診断担当を兼用せず、独立した研究担当roleを設ける。
- 実験中は既定30分ごとに、異常がない場合も進捗と改善見込みを判断する。
- 研究担当は継続・打ち切って次候補へ・対応済みcheckpointからの再開を提案する。停止・編集・投入はsupervisorが検証して実行する。
- 次の実験は対象実験の停止確認後に投入する。古くなった提案、重複起動・重複投入を防ぐ。
- 判断失敗は有限回再試行し、上限で研究担当の自動判断を停止する。判断失敗だけを理由に学習を停止しない。既存の異常監視は継続する。
- Linuxの実CPU学習で、継続・切り替え・checkpoint再開・会話消失からの復旧・daemon再起動後の重複防止を検証する。

以下はこれらを実装可能にする具体的な設計であり、本書のレビュー対象とする。

## 2. 現行実装との関係

- [periodic.rs](../../../src/periodic.rs)のPeriodic DeepCheckはproject単位の通常agent起動である。既定は無効で、通常agentのcontext設定に従う。campaign研究担当と同一視しない。
- [health.rs](../../../src/health.rs)は実行中の異常を観測し、疑わしい場合に[health_diagnosis.rs](../../../src/health_diagnosis.rs)へ診断を依頼する。正常時の観測はLLMを起動しない。
- [agent.rs](../../../src/agent.rs)はdecision / diagnosisをfreshに固定する。この規則と、editorの初回fresh・同一sessionでの修正1回という規則は維持する。
- 現行の`kill_and_resume`は同じargvの後継実験を作る。`checkpoint_note`があるだけでは、checkpointを実際に読み込んだ証拠にならない。
- [decision_evidence.rs](../../../src/decision_evidence.rs)は終端実験のbounded contextを作る。実行中の実験を偽の終端実験に変換してこの経路へ渡さない。
- schemaは基準時点でv28。新しい研究状態は既存のmigration方式で追加し、既存行と進行中のdecision/editorを保存する。

採用案は独立した研究担当role。診断担当の拡張案は起動経路を共用できる一方、異常診断と長期の改善判断の双方にsession規則の変更が及ぶため採用しない。汎用agent workflow frameworkやMLリポジトリ固有の中間controllerは追加しない。

## 3. 構成と責任

| 構成要素 | 責任 | 行わないこと |
| --- | --- | --- |
| research scheduler / repository | due判定、排他的claim、session世代、判断・操作意図・次回時刻の永続化 | LLMの記憶から正本を復元すること |
| research evidence / protocol | scope付き証拠の構築、上限検査、構造化回答の検証 | 指標の捏造、自然文をコマンドとして実行すること |
| 研究担当agent | 証拠を読み、仮説・推奨と次回へ残す研究メモを返す | source/STATE/SQLite/Gitの直接変更、直接kill/submit、目標・予算の変更 |
| supervisorの操作処理 | 最新状態の照合、停止確認、既存decision/候補/check/投入処理への接続 | agentの回答だけで実行条件を迂回すること |

新しいroleにも既存のnative launch gate、private output artifact、資格情報の許可リスト、実行時間上限を適用する。研究メモはagentがファイルを直接編集するのではなく、検証済み回答からsupervisorが保存する。

会話継続には既存のbuilt-in Codex session機構を使う。custom executableのresume方式を新規に一般化しない。対応runtimeやpolicyが利用できなければ、その理由を出して研究担当を停止し、別runtimeへ黙って切り替えない。Linux trusted-nativeであり、OS containmentを実装したとは扱わない。

## 4. 定期起動と予算

service policyのcampaign設定に`research_interval_minutes`を追加する。既定30、許容範囲0〜1440、0は研究担当の定期判断を無効にする。既存の`observer_interval_minutes`とprojectの`deep_check_interval_minutes`は別のまま維持する。

- 通常のsetup / submitで作られたmanaged campaignも対象とし、ML側の常駐controllerは要求しない。
- 初回は実験のrunning開始から1間隔後。その後は永続化した次回時刻に従う。healthyでも起動対象になる。
- 複数実験が動いていてもcampaignにつき研究担当は1つ。running一覧は上限付きで渡し、1回答が操作する対象は1実験に限る。
- 既存のproject単位のactive-agent制約も維持する。他roleの実行中は有限の次回時刻へ延期する。研究担当用に並列実行制限を緩めない。
- 停止済み実験しかないcampaignには定期研究担当を起動しない。実験終了後の新候補判断は既存のterminal decisionが担当する。
- pause / halt / retirement / goal review / 予算待ちは既存の停止条件に従う。停止中のtickをresume後に大量再生しない。
- 機械的な異常検知と必要な停止を定期研究より優先する。研究runがあっても安全上必要な停止を待たせず、その研究runの古い操作提案を失効させる。

研究runは既存の`max_agent_runs_per_hour`等の予約・消費経路に含める。失敗、再試行、新sessionでの復旧を無料の別枠にしない。新実験・コード変更・live repairにも対応する既存予算を適用する。

導入後は既存のactive campaignも対象となり、LLM呼び出しが増え得る。更新前にpauseするか、service policyで研究間隔を0にする手順を文書化する。migration自体はagentやPueueを起動しない。rokoへの実サービス適用は本作業の範囲外である。

## 5. 会話と研究履歴

SQLiteのcampaign状態、目標、予算、実験、lineage、bestを正本とする。会話・研究メモは判断の補助であり、正本の上書き権限を持たない。

campaignには研究担当専用のsession IDと世代番号を結び付ける。初回はfresh、次回からそのIDだけをresumeする。projectの`resume_latest`や他roleのsessionを探索して流用しない。実験が変わっても同じcampaignなら継続し、campaignの変更時には必ず分離する。

保存情報は次に分ける。

- **観測事実**：対象experiment/task identity、観測時刻、入力証拠digest、既存の評価値・health状態への参照。
- **研究担当の解釈**：仮説、改善見込みの理由、negative resultへの参照、推奨操作、次回確認事項。事実や確定評価値として保存しない。
- **実行記録**：採否と理由、操作意図、停止要求、後継decision/experimentとの対応、失効・失敗記録。

入力contextはUTF-8 JSON、最大128 KiB。直近の実験結果と研究メモはそれぞれ最大32件、1回に渡すlog tailは対象ごとに最大4 KiBとする。全transcript・全履歴・全ログを連結しない。省略があれば件数と省略の事実を示し、入力schema versionとdigestを保存する。

実行中の指標は既存artifactやlogにある観測と時刻に結び付ける。logからの読み取りやagentの解釈は未確定情報であり、terminal manifest由来の`experiment_metrics`やpromotion値へ昇格させない。値がない場合は不明とし、補完しない。新しいML別ログparserや必須の中間metric形式は作らない。

新しい研究contextは独立したversion付きschemaとする。terminal decisionへ渡す研究メモもboundedな補助情報であり、既存の保存済みdecision contextの検証互換性を保つ。過去の証拠bytes/digestを遡及更新しない。

## 6. 回答と操作の適用

研究回答は1つの構造化documentとし、未知field、不正型、上限超過を拒否する。全体上限は128 KiB、理由は4 KiB、次回用メモは4 KiB、証拠参照は最大16件・各512 bytes。回答にはschema version、review ID、対象experiment ID、入力context digest、action、理由、証拠参照、研究メモを持たせる。

actionは次の3つだけとする。各actionで不要なpayloadを混在させない。

### 6.1 `continue`

実験はそのまま継続し、検証済み研究メモと次回時刻を保存する。追加の学習taskや候補を作らない。情報不足のときに継続する判断も許可する。

### 6.2 `stop_and_next`

対象の進捗と改善見込みに関する証拠参照、および次候補の探索方針を要求する。研究担当の自然文から直接argvやsource変更を実行しない。

1. 回答を保存し、対象task signature、campaign、現在のrunning状態、pause、health操作との競合、予算・policyを照合する。
2. 実行可能なら停止意図と対象を永続化し、既存のtermination managerへ要求する。
3. confirmed terminationと対象のterminal projectionを待つ。killコマンドの終了0だけで停止済みにしない。
4. 研究メモを添え、同じ終端実験の既存decision cycleへ引き継ぐ。callback側のcycleと別にもう1つ作らない。
5. freshのdecisionが通常のproposalを返し、既存のvalidation、必要なeditor/check、予算予約、Pueue投入へ進む。decisionがwaitや拒否を返すことも許可し、次候補の成功を保証したとは扱わない。

停止前に後継実験を先行投入しない。予算不足が判明している場合は、改善見込み判断だけで先に停止せず理由を記録する。停止後に予算・状態が変化した場合も、予約条件を迂回せず待機・拒否を保存する。

### 6.3 `resume_from_checkpoint`

対象の学習コマンドが対応し、保存済みcheckpointを実際に利用できる場合だけ受理する。payloadにはcheckpoint参照、再開argv/cwd、対応根拠を持たせる。argv/cwdは既存のproposal・executable policyで検証し、shell文字列は受理しない。

- 停止前にcheckpointのscope、型、所有者、読み取り可能性、identityと内容digestを検査する。保存完了した変更されないcheckpointだけを対象とし、未完成の一時ファイル、unsafe path、他campaignの無関係なartifactは拒否する。検証の既存resource上限内で内容を確認できない場合も受理しない。
- 後継が利用できる保存先・保持期間であることを確認する。停止やcandidate cleanupで失われるcheckpointは、安定して保持できない限り受理しない。任意の巨大ファイルを無条件にコピーする仕組みは追加しない。
- 学習コード/設定とcheckpointの対応根拠を必要とする。agentの「再開できる」という一文や`checkpoint_note`だけを成功証拠にしない。未対応なら停止せず、提案不適用と理由を記録する。
- 停止確認後、既存のproposal admission / resume lineage / submission経路で後継を1件だけ予約する。読み込みまでcheckpointの不変性を保持できることを条件とし、投入前にidentityと内容digestを再照合する。利用不能ならcold startに黙って変換しない。
- 引き継ぐ学習仕様は同じとし、checkpoint読み込みに必要なargv差分だけを扱う。学習率やモデル変更を同時に行うものは`stop_and_next`の候補経路へ送る。
- `resume_of_experiment_id`とcheckpoint参照を保存し、実際の読み込みが観測できた場合だけ「checkpointから再開確認済み」と表示する。未確認はそのまま表示する。

この経路が後継を所有する間、terminal decisionやrunning-healthの別経路から同じ終端実験の後継を重複予約しない。既存の異常診断によるsame-argv resubmitの意味を遡及的に変更しない。

## 7. 永続化、競合、再起動

新しい永続化はcampaign研究状態とreview記録に限定する。既存`agent_runs`、events、budget reservations、termination requests、decision cycles、experimentsへの参照を再利用する。

- campaign研究状態：session ID/世代、次回時刻、直近review、active/blocked状態、blocked理由。
- review記録：campaign・対象実験・観測identity、context version/digest、attemptとagent runの対応、回答、操作状態、停止要求・後継への参照。
- 状態遷移はpending → running → ready → completed、またはretry_wait / discarded / blockedとする。停止を要するready回答は停止要求・停止確認・後継予約を別々のdurable段階として識別できるようにする。
- campaignごとの未完reviewと、対象実験への未完操作をDB制約とtransactionで一意にする。現行のproject単位active-agent制約を弱めない。
- session IDの確定と検証済みメモの保存は、失敗runの部分出力から成功状態を作らない。回答が失われたrunは再起動しても操作済みと推定しない。
- 再起動時はDBと実際のagent/task/terminationを照合し、未完段階から再開する。実行中のagentが残っている限り同じsessionを別runで開かない。
- 対象実験の自然終了、task identity変更、pause、他経路による停止、goal review等で前提が変わった回答は操作に使わずdiscardedにする。古い回答のtargetを新しい実験へ付け替えない。
- terminal manifestが有効なら既存の評価規則で扱い、打ち切ったという理由だけで失敗値やbestを書き換えない。目標達成の最終承認も既存のhuman reviewを維持する。

## 8. 失敗とsession再構成

研究runの試行上限値は既存の`max_decision_attempts_per_cycle`を利用する（既定3）。試行数は研究reviewごとに数え、terminal decisionのcycleとは混ぜない。有限のretry時刻を保存し、daemon再起動やsession世代変更で試行数・予算をリセットしない。

| 状況 | 扱い |
| --- | --- |
| 所有権等に問題のないsession欠落、明確に識別できる継続不能 | 世代を進め、保存済み事実・メモからfreshで再構成。理由を保存 |
| timeout、不正JSON、unknown action、通常のruntime失敗 | 有限再試行。部分回答では操作しない |
| session/project所有権不一致、unsafe path、policy/credential検証失敗 | 研究担当をblockedにし、freshへの切り替えで回避しない |
| 試行上限到達 | 研究担当をblockedにし、理由と必要な対応を表示。無限に新sessionを作らない |
| 停止失敗・停止状態が不明 | 後継を投入せず既存terminationの復旧規則に従う |

blockedは研究担当の状態であり、それだけを理由に実行中の学習をkillしない。機械的な異常検知・既存health診断は維持する。復旧手順は`pueue-agent campaign pause`、原因解消、`pueue-agent campaign resume`とする。resume時の再検証に成功すれば研究担当のblockedを解除し、新しいreviewを予定する。古いreviewの失敗・試行履歴と消費済み予算は残す。既存terminal decisionのexhausted cycleを消去する動作は追加しない。daemon再起動だけでは解除しない。policyで間隔0にしたときも、既に行った停止を巻き戻したり、未確認の後継を追加したりしない。

## 9. 利用者向け表示と文書

既存のcampaign status/showへ、研究担当の状態、次回予定、最終判断、対象実験、blocked/discarded理由、session再構成の有無をboundedに追加する。履歴詳細はcampaign scopeと件数上限を必須にし、raw transcript、全ログ、秘密情報を出さない。

日本語の使い方・設定・workflow・内部構造・troubleshootingを更新し、次を説明する。

- 研究担当、Periodic DeepCheck、異常診断、terminal decision、editorの違い。
- 30分既定、無効化、呼び出し予算、既存campaignへの導入時の注意。
- 会話継続とSQLiteの正本の違い、会話再構成時に復元できないもの。
- 打ち切り後も必ず次候補が受理されるわけではないこと、停止確認・予算待ち。
- checkpoint対応の前提、再開要求と再開確認済みの違い。
- blocked理由と明示的な復旧手順。main/remoteへの自動merge/pushは行わないこと。

## 10. 検証と完了条件

単体・統合テストでは制御した時計を使う。Linux E2Eは専用policyで間隔を1分へ短縮して実運転し、既定値30分は設定テストで別途確認する。test doubleは研究判断・編集案・sessionプロトコルに限り、学習、checkpointの保存/読み込み、Pueue、停止確認、評価・promotionは実処理を使う。

必須の検証：

1. 初回due、healthy時の起動、間隔0、runningなし、pause/retire、予算待ち、重複claim・多重daemon。
2. 同campaignの同session継続と別campaign分離。session欠落からの自動再構成、所有権不一致での停止、再構成ループの上限。
3. context/回答のbytes・件数上限、raw credentialを含めないこと、未確定metricを正式評価へ混入しないこと。
4. 継続判断が新規taskを作らないこと。打ち切り→停止確認→fresh decision→候補/check→新学習が順序どおり成立すること。
5. 判断中の自然終了・pause・health kill・予算変化で古い操作が適用されないこと。返答不正やtimeoutだけで学習をkillしないこと。
6. 対応fixtureがcheckpointのstep/weightを読み込み、cold startでないと独立に確認できること。未対応・消失・差し替え・unsafe checkpointでは再開しないこと。
7. review起動後、回答保存後、停止要求後、停止確認後、後継予約後の各再起動で、agent/kill/後継が重複せず、lineageと予算が維持されること。
8. 既存decision/diagnosis/editorのsession・schema契約、running-health、Stage1の指示更新、実学習promotionと既存E2Eが退行しないこと。

実装担当はユーザー指定のLuna-max。設計書承認後に実装計画と分離worktreeを用意し、TDD、task review、最終reviewを行う。rokoは独立コピーと専用state/Pueue profileだけで検証する。既存checkout・サービス・実験は操作しない。

Linuxのgateは全体format、debug/release check、全Rust、Bats、Stage2実学習E2Eと既存E2E。実行したSHA・終了コード・件数を保存する。[Stage1の既知UTC表記失敗とformat差分](../../report/2026-09-15-research-reliability-stage1-verification.md)を新規の失敗と混同せず、未実行を成功扱いしない。fake agentの合格を実LLMの研究能力の保証とはしない。

第3段階の初回導入支援、第4段階の評価固定、第5段階のOS隔離・時間予算、実GPU OOM、ML framework別checkpoint変換、任意のcheckpoint保存指示、mainへの自動統合は本段階に含めない。merge/pushは別途ユーザーの指示を受ける。
