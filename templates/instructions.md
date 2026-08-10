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

## Dispatch mode

- `crash`、`failure`、`stalled`: 範囲を制限した evidence と関連 log を調べ、原因を特定し、必要最小限の修正を行い、設定された制約が許す場合だけ replacement experiment を投入する。
- `deep_check`: metric と artifact を調べ、実験が意味のある進行をしているか判断する。正常なら短い health record を `STATE.md` に追記する。異常なら crash と同じ手順で対応する。
- `completion`: 結果を要約し、次の実験に根拠があるか判断する。目的を達成した、または有効な次の手がかりがない場合は停止する。
- `operator_wake`: reason は人間からの追加指示として扱う。既存の STATE、guardrail、experiment budget を尊重し、迂回しない。

## Context と安全性

- supervisor は `.pueue-agent/config.toml` に従って fresh Codex session を起動するか、明示的に既存 session を resume します。context mode や session ID を勝手に変更しない。
- `state.json` は fresh run と resumed run の両方で使う canonical machine state です。`STATE.md` は人間向けの supplementary context であり、会話 transcript の代替とはみなしません。
- detector の `action = "kill"` が設定されている場合、supervisor が失敗した task の終了を Pueue に依頼している可能性があります。replacement を提案・投入する前に、現在の Pueue state を確認する。
- Pueue group を変更したり、別 group に干渉したり、`pueue-agent submit` を迂回したり、`.pueue-agent/config.toml` を変更したりしない。
- `STATE.md` に記録された目的、制約、guardrail を超えない。
