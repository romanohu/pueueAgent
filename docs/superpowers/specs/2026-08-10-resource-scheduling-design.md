# NVIDIA GPU / コスト-aware scheduling 設計書

日付: 2026-08-10
ステータス: レビュー待ち

## 目的

NVIDIA GPU の空き状況を確認してから Pueue task を投入し、GPU resource の過剰予約と実験コストの上限超過を防ぐ。
resource 設定のない既存 project の submit 挙動は変更しない。

## 設定

```toml
[resources]
provider = "nvidia-smi"
gpu_count = 1
min_free_vram_gb = 24
max_gpu_utilization_percent = 95
poll_interval_seconds = 30
wait_for_resources = true
estimated_gpu_minutes = 180

[guardrails]
max_total_gpu_minutes = 2000
```

`[resources]` が存在しない場合は resource admission を無効にする。
`wait_for_resources = true` の場合は submission intent を pending のまま保持し、false の場合は resource 不足を typed error として返す。

第一段階ではコストを GPU 分数で表現する。通貨単位の料金は provider やクラウドごとの差が大きいため、後続機能とする。

## Resource provider

provider の境界を trait で定義する。

```text
ResourceProvider::snapshot() -> ResourceSnapshot
```

`NvidiaSmiProvider` は固定された `nvidia-smi` argv で次の値を取得する。

- GPU index
- total/free VRAM
- GPU utilization
- power draw

CSV 出力は型・行数・値の範囲を検証し、command timeout と出力 byte 上限を設ける。
`nvidia-smi` が存在しない、終了コードが失敗、出力が壊れている場合は snapshot を unknown とする。

provider が unknown のときは、新規 resource claim と新規 Pueue submit を作らない。既に実行中の task は停止しない。

## SQLite モデル

### `resource_snapshots`

provider 名、取得時刻、bounded な GPU resource JSON、状態 (`available` / `unknown` / `error`) を保存する。
古い snapshot は保持上限を設け、診断に必要な範囲だけ残す。

### `resource_claims`

project、submission、要求 GPU 数、最小 free VRAM、状態、lease、grant/release 時刻を保存する。
同一 submission に対する active claim は1つだけ許可する。

claim は Pueue add の前に transaction で取得し、Pueue add が成功したら submission と task ID に結び付ける。
add が失敗した場合は claim を解放し、submission intent は再試行可能な状態に戻す。

再起動時には期限切れ claim を回収する。agent による replacement experiment も同じ admission 経路を通す。

## Scheduling flow

```text
submit intent
  → resource snapshot
  → claim
  → pueue add
  → task observation
  → claim release
```

resource 不足時は `pueue add` を呼ばず pending submission として待機する。daemon の scheduler が `poll_interval_seconds` ごとに再評価する。
複数 project が待機している場合は priority と created time で順序を決め、同じ claim を二重に取得しない。

Pueue 外から GPU を使う process は resource snapshot で観測するだけで、強制停止や所有権の取得はしない。
そのため free VRAM の変化により admission が不確実になる場合は、内部 claim を保守的に扱う。

## Guardrail

実験の実測 GPU 分数または設定した estimated GPU 分数を project/campaign 単位で積算する。
`max_total_gpu_minutes` に到達したら、新規 submission と agent による replacement を停止し、理由を durable event と operator log に記録する。

## CLI / 診断

`status --json`、`inspect`、`doctor` は resource provider の状態、active claim、pending submission、累積 GPU 分数を表示する。
resource 制御専用の Web UI や通知は追加しない。

## 検証

- `nvidia-smi` の正常、空、壊れた CSV、timeout、非ゼロ終了
- VRAM と utilization の境界値
- resource claim の重複防止と lease 回収
- Pueue add 成功・失敗時の transaction 整合性
- 再起動後の pending submission と claim 復旧
- 複数 project の priority/fairness
- provider failure 時に fail closed となること
- GPU 分数 guardrail と既存の実験 guardrail の組み合わせ
- resource 設定がない project の既存 submit 回帰

## 非目標

- AMD / Apple GPU provider
- 通貨ベースのクラウド料金計算
- Pueue 外の process の停止
- 実行中 task の GPU migration
