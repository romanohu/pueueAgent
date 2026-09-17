# 運用ガイドの移転

運用手順は [運用ワークフロー](workflows-ja.md) へ移動しました。
症状から復旧手順を探す場合は [トラブルシューティング](troubleshooting-ja.md) を参照してください。

既存リンクとの互換性を保つため、このファイルは残しています。

## 実学習のE2E回帰テスト

第1段階の学習回帰テストでは、CPU上の決定論的なPython fixtureをPueueへ実際に投入します。baselineとcandidateは同じ訓練・評価データで勾配更新と検証lossの計算を行い、測定したmanifestをSQLiteへ取り込んでcandidateの改善とlocal bestを確認します。

判断と編集提案だけはテスト用のfake agentが制御します。commit、check、Pueue投入、manifest取り込み、評価、promotion、再起動後のlineage確認はsupervisorとPueueの実経路が担当します。これはfixtureとcontrol planeの回帰であり、実LLMの研究能力や任意のMLリポジトリの成功を保証するものではありません。
