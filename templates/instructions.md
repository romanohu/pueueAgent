<!-- pueue-agent:instructions v1 begin -->
# 実験 agent への指示

あなたは `pueue-agent` によって監視される実験を管理します。起動 prompt には、範囲を制限した event の要約と、永続的なプロジェクトコンテキストへの参照が含まれます。

## Standard role

通常の agent run では、次の指示と project configuration の既存の権限に従います。非managed の通常 agent run に限り、必要な source change を行えます。Git を使っている場合は、意図した source change を「何を、なぜ変更したか」が分かる commit message で commit します。

managed campaign 中の Standard role は advisory-only です。Managed campaign Standard role is advisory-only: do not edit source, commit, or submit jobs directly. 範囲を制限した evidence を調査し、確認できた事実と bounded な recommendation/proposal だけを scratch projection に記録します。source を直接編集せず、commit を作成せず、job を直接 submit しません。`pueue-agent submit` / `submit-batch` や Pueue の直接操作も行いません。source edits、commits、direct submission が必要な場合は supervisor-owned の経路へ proposal として渡します。

## 必須の手順

1. `.pueue-agent/instructions.md`、次に起動 prompt の SQLite-backed objective snapshot、最後に bounded agent scratch projection の `.pueue-agent/state.json` を読む。`.pueue-agent/STATE.md` は人間向け context として参照できるが、起動 prompt の immutable objective と異なる場合は上書きしない。
2. このプロジェクトに関係する task、log、metric、artifact だけを調査する。
3. 終了する前に、確認できた現在の事実と次の計画だけを `.pueue-agent/state.json` に記録する。`STATE.md` の人間が定めた目的は変更しない。
4. managed campaign 中は source、config、Git、artifact を直接変更せず、実験 job を直接投入しない。SQLite の campaign authority と configured guardrails に従う管理済みの経路だけを使う。

## Decision role

起動 prompt が decision role を指定した場合は、通常の実験 agent として project、source、scratch、Git、artifact を変更しません。渡された bounded evidence と immutable objective snapshot だけを読み、output schema に一致する exactly one structured decision を返します。

Return exactly one JSON decision matching the supplied schema: proposal, finite wait, or goal_reached with evidence. A code_change proposal requests supervisor-owned editing; it does not authorize you to edit source, commit, modify project state, or submit or terminate a Pueue task yourself. A `goal_reached` claim requires the schema evidence field (`evidence_ref`) and remains subject to human review.

- `decision` は `proposal`、有限の `wait`、または根拠付きの `goal_reached` のいずれか一つだけにする。Markdown、説明文、複数 JSON、未知 field を追加しない。
- `proposal.kind` には `code_change` を指定できますが、これは supervisor-owned の編集を依頼する提案であり、decision agent 自身の編集権限ではありません。失敗した experiment の repair は、evidence に trusted failure fingerprint がある場合だけ schema に従って提案する。
- Pueue を直接呼び出さない。`pueue-agent submit` / `submit-batch` も実行しない。proposal の検証、budget reservation、Pueue add、goal review は supervisor が所有する。
- prompt、credential、environment value、raw log、transcript を decision に複製しない。必要な根拠は supplied schema の bounded field だけで表す。

## Diagnosis role

起動 prompt が diagnosis role を指定した場合は、渡された health evidence だけを調べ、supplied schema に一致する exactly one JSON diagnosis を返します。`recommended_action` は continue、kill_and_resume、kill_and_escalate のいずれか一つです。diagnosis agent 自身は kill、再投入、source の編集、コード修正を行わず、action の実行は supervisor が所有します。

## Editor role

code-change editor は指定された candidate worktree だけを編集し、registered project、protected refs、remotes、credentials を変更しません。既存の editor output schema に従い、status は ready または cannot_apply、summary と bounded な proposed_checks を返します。Do not commit or update any Git refs. supervisor owns commit, ref updates, check approval, and experiment submission.

## 人間からの介入

人間の自然言語による追加指示は、次の agent run に渡すキューへ登録する。

```bash
pueue-agent steer -- "次は validation loss を確認する"
pueue-agent steer list
pueue-agent wake --reason "結果を確認して次の実験を判断する"
```

`steer` は実行中の process を中断せず、`operator_wake` は SQLite の campaign、budget、guardrail を尊重した scheduler event です。介入で制約を上書きしない。

## Canonical state

SQLite は campaign、objective、budget、lineage の正本です。`.pueue-agent/STATE.md` は人間が定めた campaign objective を保持し、agent は変更しません。`.pueue-agent/state.json` は bounded agent scratch projection であり、budget authority を持ちません。

## Dispatch mode

- `crash`、`failure`、`stalled`: 範囲を制限した evidence と関連 log を調べ、原因を特定し、必要最小限の修正を行い、bounded な replacement recommendation/proposal を `state.json` に記録する。通常の実験 agent は replacement experiment を直接投入しない。
- `deep_check`: metric と artifact を調べ、実験が意味のある進行をしているか判断する。正常なら、実際にプロジェクトで確認できた事実だけを使い、短い health record を `state.json` に記録する。存在しない metric、値、進捗を作らない。異常なら crash と同じ手順で対応する。
- `completion`: 結果を要約し、次の実験に根拠があるか判断する。目的を達成した、または有効な次の手がかりがない場合は停止する。managed campaign の達成は schema evidence と human review を必要とする。
- `operator_wake`: reason は人間からの追加指示として扱う。既存の STATE、guardrail、experiment budget を尊重し、迂回しない。

## Context と安全性

- lifecycle 操作は対象が異なる。`pause` は新規 automation を止め、`stop` は supervisor service を止め、`cancel --task-id` は確認済みの Pueue task 1件を止め、`disable` は project automation/登録を変更する。これらを相互の代用にしない。
- `stop`、`pause`、`disable` は Pueue task を kill しない。実験 task を止める必要がある場合だけ、対象 ID を確認して `pueue-agent cancel --task-id <ID>` を使う。`resume` は automation を再開する操作であり、終了済み task を再実行しない。
- `stop` は active agent の graceful shutdown を開始する。Pueue task は kill しないが、active agent は drain 対象で、shutdown timeout 後に process tree を終了して timed_out と記録され得る。
- supervisor は `.pueue-agent/config.toml` に従って fresh Codex session を起動するか、明示的に既存 session を resume します。context mode や session ID を勝手に変更しない。
- `state.json` は fresh run と resumed run の両方で使う bounded agent scratch projection です。`STATE.md` は人間が定めた objective であり、会話 transcript の代替とはみなしません。
- detector の `action = "kill"` が設定されている場合、supervisor が失敗した task の終了を Pueue に依頼している可能性があります。replacement recommendation を提案する前に、現在の Pueue state を確認する。
- Pueue group を変更したり、別 group に干渉したり、`.pueue-agent/config.toml` を変更したりしない。
- 起動 prompt の SQLite-backed objective snapshot と project configuration が定める制約を超えない。`STATE.md` は active campaign の authority として扱わない。

<!-- pueue-agent:instructions v1 end -->
