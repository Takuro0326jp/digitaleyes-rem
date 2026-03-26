# 本番（Turso / Vercel）向けチェックリスト

## 1. Turso（LibSQL）

| 項目 | 内容 |
|------|------|
| **URL** | ダッシュボードの `libsql://....turso.io` を **`DATABASE_URL`**（または `TURSO_DATABASE_URL`）に設定。 |
| **トークン** | **`TURSO_AUTH_TOKEN`**（または `TURSO_API_TOKEN`）に read/write 用トークン。**URL だけでは接続できません。** |
| **マイグレーション** | サーバ（`proxy-server` または Vercel の API）が **`getDb()` 初回呼び出し**で `lib/sync-db.js` の `migrate` と `property-store` のテーブル作成を実行します。手動 SQL は通常不要です。 |
| **リージョン** | 例: `aws-ap-northeast-1` は東京近傍。レイテンシ重視ならこの構成で問題ありません。 |

`.env`（ローカル）と **Vercel → Settings → Environment Variables** の両方に、本番用の値を入れてください。  
**`.env` は Git に含めないでください。**

## 2. アプリ認証・デジタライズ API

| 変数 | 用途 |
|------|------|
| `JWT_SECRET` | **本番必須**（32 文字以上のランダム文字列推奨）。 |
| `SECRET_ID` / `SECRET_PASSWORD` | デジタライズ API プロキシ用（既存の `.env` と同じ）。 |

## 3. Vercel 特有の制約

- **Git に同梱された `data/users.json` / `data/properties.json`** はデプロイ物として読み取り可能ですが、**ランタイムでのファイル書き込みは不可**（物件追加・一部更新は 503 になる実装）。  
- **Turso に載るデータ**: 顧客スナップショット、来場ステータス、営業履歴、物件マスタ（`property_master`）など、`lib/sync-db.js` / `property-store` が使うテーブル。  
- **まだ JSON のままのデータ**: ユーザー一覧・初期物件の「デプロイ更新で反映」運用は `VERCEL.md` を参照。

## 4. メール・URL（任意）

招待メール等を使う場合: `SMTP_*`, `APP_LOGIN_URL`, `APP_URL`。

## 5. 素材ギャラリー S3（任意）

`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` など。バケット名は物件詳細画面から登録。

## 6. デプロイ後の確認

1. 環境変数保存後 **Redeploy**。  
2. ログイン → 差分/全件同期 → 顧客一覧が表示されること。  
3. Turso ダッシュボードでクエリログやストレージが増えていることを確認。

詳細手順はリポジトリ直下の **`VERCEL.md`** を参照してください。
