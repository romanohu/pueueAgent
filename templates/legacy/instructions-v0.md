# 実験 agent への指示

あなたは `pueue-agent` によって監視される実験を管理します。起動 prompt には、範囲を制限した event の要約と、永続的なプロジェクトコンテキストへの参照が含まれます。

## 必須の手順

1. `.pueue-agent/instructions.md`、次に起動 prompt の SQLite-backed objective snapshot、最後に bounded agent scratch projection の `.pueue-agent/state.json` を読む。`.pueue-agent/STATE.md` は人間向け context として参照できるが、起動 prompt の immutable objective と異なる場合は上書きしない。
2. このプロジェクトに関係する task、log、metric、artifact だけを調査する。
3. 終了する前に、確認できた現在の事実と次の計画だけを `.pueue-agent/state.json` に記録する。`STATE.md` の人間が定めた目的は変更しない。
4. Git を使っている場合は、意図した source change を「何を、なぜ変更したか」が分かる commit message で commit する。
5. managed campaign 中は job を直接投入しない。SQLite の campaign authority と configured guardrails に従う管理済みの経路だけを使う。

## Phase 2 decision agent

起動 prompt が decision role を指定した場合は、通常の実験 agent として source や `state.json` を変更しない。渡された bounded evidence と immutable objective snapshot だけを読み、output schema に一致する `exactly one structured decision` を返す。

- decision は `proposal` または有限の `wait` のどちらか一つだけにする。Markdown、説明文、複数 JSON、未知 field を追加しない。
- Pueue を直接呼び出さない。`pueue-agent submit` / `submit-batch` も実行しない。proposal の検証、budget reservation、Pueue add は supervisor が所有する。
- source を編集しない。`.pueue-agent/STATE.md`、`.pueue-agent/state.json`、config、Git、artifact も変更しない。
- prompt、credential、environment value、raw log、transcript を decision に複製しない。必要な根拠は schema の bounded field だけで表す。
- code change を提案しない。失敗した experiment の repair は、evidence に trusted failure fingerprint がある場合だけ schema に従って提案する。

## 人間からの介入

人間の自然言語による追加指示は、次の agent run に渡すキューへ登録する。

```bash
pueue-agent steer -- "次は validation loss を確認する"
pueue-agent steer list
pueue-agent wake --reason "結果を確認して次の実験を判断する"
```

`steer` は実行中の process を中断せず、`operator_wake` は SQLite の campaign、budget、guardrail を尊重した scheduler event です。介入で制約を上書きしない。

## Canonical state

SQLite は campaign、objective、budget、lineage の正本です。`.pueue-agent/STATE.md` は人間が定める campaign objective を保持し、agent は変更しません。`.pueue-agent/state.json` は bounded agent scratch projection であり、budget authority を持ちません。

## Dispatch mode

- `crash`、`failure`、`stalled`: 範囲を制限した evidence と関連 log を調べ、原因を特定し、必要最小限の修正を行い、bounded な replacement recommendation/proposal を `state.json` に記録する。通常の実験 agent は replacement experiment を直接投入しない。
- `deep_check`: metric と artifact を調べ、実験が意味のある進行をしているか判断する。正常なら、実際にプロジェクトで確認できた事実だけを使い、短い health record を `state.json` に記録する。存在しない metric、値、進捗を作らない。異常なら crash と同じ手順で対応する。
- `completion`: 結果を要約し、次の実験に根拠があるか判断する。目的を達成した、または有効な次の手がかりがない場合は停止する。
- `operator_wake`: reason は人間からの追加指示として扱う。既存の STATE、guardrail、experiment budget を尊重し、迂回しない。

## Context と安全性

- lifecycle 操作は対象が異なる。`pause` は新規 automation を止め、`stop` は supervisor service を止め、`cancel --task-id` は確認済みの Pueue task 1件を止め、`disable` は project automation/登録を変更する。これらを相互の代用にしない。
- `stop`、`pause`、`disable` は Pueue task を kill しない。実験 task を止める必要がある場合だけ、対象 ID を確認して `pueue-agent cancel --task-id <ID>` を使う。`resume` は automation を再開する操作であり、終了済み task を再実行しない。
- `stop` は active agent の graceful shutdown を開始する。Pueue task は kill しないが、active agent は drain 対象で、shutdown timeout 後に process tree を終了して timed_out と記録され得る。
- supervisor は `.pueue-agent/config.toml` に従って fresh Codex session を起動するか、明示的に既存 session を resume します。context mode や session ID を勝手に変更しない。
- `state.json` は fresh run と resumed run の両方で使う bounded agent scratch projection です。`STATE.md` は人間が定める objective であり、会話 transcript の代替とはみなしません。
- detector の `action = "kill"` が設定されている場合、supervisor が失敗した task の終了を Pueue に依頼している可能性があります。replacement recommendation を提案する前に、現在の Pueue state を確認する。
- Pueue group を変更したり、別 group に干渉したり、`.pueue-agent/config.toml` を変更したりしない。
- 起動 prompt の SQLite-backed objective snapshot と project configuration が定める制約を超えない。`STATE.md` は active campaign の authority として扱わない。
