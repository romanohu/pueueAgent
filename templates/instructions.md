# 実験 agent への指示

あなたは `pueue-agent` によって監視される実験を管理します。起動 prompt には、範囲を制限した event の要約と、永続的なプロジェクトコンテキストへの参照が含まれます。

## 必須の手順

1. `.pueue-agent/instructions.md`、次に canonical machine state の `.pueue-agent/state.json`、最後に補足情報として `.pueue-agent/STATE.md` を読む。
2. このプロジェクトに関係する task、log、metric、artifact だけを調査する。
3. 終了する前に、現在の事実、lineage、budget、次の計画を `.pueue-agent/state.json` に記録する。人間向けの経緯や補足は `.pueue-agent/STATE.md` に記録してよいが、canonical state と矛盾させない。
4. Git を使っている場合は、意図した source change を「何を、なぜ変更したか」が分かる commit message で commit する。
5. 監視対象の実験は必ず次の形式で投入する。

   ```bash
   pueue-agent submit -- <command...>
   ```

   raw の `pueue add` は実行しない。SQLite の submission accounting を迂回するためです。

## Submission kind と experiment budget

- 学習、評価、比較対象の本体は `experiment` として投入する。
- bootstrap、診断、準備、後片付けなど実験数に含めない制御 task は `control` として投入する。
- `control` は履歴と Pueue task には残るが、`max_experiments` を消費しない。その他の guardrail や group 制約を無視してよいという意味ではない。

```bash
pueue-agent submit --kind experiment -- python train.py --lr 0.001
pueue-agent submit --kind control -- python prepare.py
```

複数 job を投入する場合は `pueue-agent submit-batch --request-id <UUID> --manifest jobs.json` を使う。同じ request ID を再送した場合、accepted 済み job は二重投入せず、未確定 job の durable な状態を再利用する。

## 人間からの介入

人間の自然言語による追加指示は、次の agent run に渡すキューへ登録する。

```bash
pueue-agent steer -- "次は validation loss を確認する"
pueue-agent steer list
pueue-agent wake --reason "結果を確認して次の実験を判断する"
```

`steer` は実行中の process を中断せず、`operator_wake` は既存の state、budget、guardrail を尊重した scheduler event です。介入で制約を上書きしたり、`state.json` の canonical な budget と矛盾する指示を採用したりしない。

## Canonical state

`.pueue-agent/state.json` の `current_facts`、`next_action`、`budgets`、`active_lineage` を機械状態の正として扱う。`STATE.md` は人間向けの補足ノートであり、古い文章を根拠に canonical state を上書きしない。run の終了前に、変更した事実と次の計画を両方のファイルへ必要な範囲で反映する。

## Dispatch mode

- `crash`、`failure`、`stalled`: 範囲を制限した evidence と関連 log を調べ、原因を特定し、必要最小限の修正を行い、設定された制約が許す場合だけ replacement experiment を投入する。
- `deep_check`: metric と artifact を調べ、実験が意味のある進行をしているか判断する。正常なら短い health record を `STATE.md` に追記する。異常なら crash と同じ手順で対応する。
- `completion`: 結果を要約し、次の実験に根拠があるか判断する。目的を達成した、または有効な次の手がかりがない場合は停止する。
- `operator_wake`: reason は人間からの追加指示として扱う。既存の STATE、guardrail、experiment budget を尊重し、迂回しない。

## Context と安全性

- lifecycle 操作は対象が異なる。`pause` は新規 automation を止め、`stop` は supervisor service を止め、`cancel --task-id` は確認済みの Pueue task 1件を止め、`disable` は project automation/登録を変更する。これらを相互の代用にしない。
- `stop`、`pause`、`disable` は Pueue task を kill しない。実験 task を止める必要がある場合だけ、対象 ID を確認して `pueue-agent cancel --task-id <ID>` を使う。`resume` は automation を再開する操作であり、終了済み task を再実行しない。
- `stop` は active agent の graceful shutdown を開始する。Pueue task は kill しないが、active agent は drain 対象で、shutdown timeout 後に process tree を終了して timed_out と記録され得る。
- supervisor は `.pueue-agent/config.toml` に従って fresh Codex session を起動するか、明示的に既存 session を resume します。context mode や session ID を勝手に変更しない。
- `state.json` は fresh run と resumed run の両方で使う canonical machine state です。`STATE.md` は人間向けの supplementary context であり、会話 transcript の代替とはみなしません。
- detector の `action = "kill"` が設定されている場合、supervisor が失敗した task の終了を Pueue に依頼している可能性があります。replacement を提案・投入する前に、現在の Pueue state を確認する。
- Pueue group を変更したり、別 group に干渉したり、`pueue-agent submit` を迂回したり、`.pueue-agent/config.toml` を変更したりしない。
- `STATE.md` に記録された目的、制約、guardrail を超えない。
