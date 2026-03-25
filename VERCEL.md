# Vercel へのデプロイ手順（はじめから）

このリポジトリは **静的 HTML** と **API（`api/handler.js`）** で動きます。ローカルの `proxy-server.js` の代わりに、Vercel のサーバーレス関数が認証とデジタライズ API のプロキシを担当します。

---

## 前提

- [Vercel](https://vercel.com) のアカウント（GitHub でサインアップ可）
- このプロジェクトを **GitHub 等のリモートリポジトリ** に push できること（Vercel は Git 連携が簡単です）

---

## 手順 1: リポジトリを GitHub に載せる

1. まだなら Git を初期化してコミットします。

   ```bash
   cd digitaleyes-rem
   git init
   git add .
   git commit -m "Initial commit"
   ```

2. GitHub で新しいリポジトリを作成し、remote を追加して push します。

   ```bash
   git remote add origin https://github.com/<あなたのユーザー名>/<リポジトリ名>.git
   git branch -M main
   git push -u origin main
   ```

`.env` は `.gitignore` に含めているため **コミットされません**。秘密情報は手順 4 で Vercel にだけ登録します。

---

## 手順 2: Vercel にプロジェクトをインポート

1. [Vercel Dashboard](https://vercel.com/dashboard) にログインします。
2. **Add New… → Project** を選びます。
3. **Import Git Repository** で、手順 1 の GitHub リポジトリを選びます。
4. **Framework Preset** は **Other** のままで構いません（自動検出でも可）。
5. **Root Directory** はリポジトリのルートのまま。
6. **Deploy** を押して初回デプロイを完了させます。

---

## 手順 3: 環境変数を設定する

ダッシュボードで **Project → Settings → Environment Variables** を開き、次を追加します。

| Name | 説明 |
|------|------|
| `JWT_SECRET` | **本番必須**。ログイン用トークンの署名鍵。32 文字以上のランダム文字列（推奨）。 |
| `SECRET_ID` | デジタライズ API のシークレット ID（ローカルの `.env` と同じ）。 |
| `SECRET_PASSWORD` | デジタライズ API のシークレットパスワード。 |
| `DATABASE_URL` / `TURSO_DATABASE_URL` | **顧客キャッシュ用 LibSQL**（推奨: [Turso](https://turso.tech/) の `libsql://...`）。Vercel 本番では未設定だと DB が開けず `500` になります。 |
| `TURSO_AUTH_TOKEN` / `TURSO_API_TOKEN` | Turso を使うときのトークン（ダッシュボードで発行）。 |

**Production / Preview / Development** のどれに付けるかは用途に合わせて選びます。少なくとも **Production** には入れてください。

### 顧客一覧のローカル DB・差分同期

- ローカル（`npm run dev`）では、未設定時 **`data/sync.db`** に顧客スナップショットが保存されます（`.gitignore` 対象）。
- **差分同期**は、可能なときは API の `conditions`（`c.upd_date >` 最終更新）でまとめて取得し、不可なら **更新日降順のページング**でウォーターマークより新しい行だけを取り込みます（通信量を抑えます）。
- **全件同期**は初回・リセット用です。一覧表示は同期後は **デジタライズへのリクエストなし**で DB から読みます。

保存後、**Deployments** から **Redeploy**（最新デプロイの「…」メニュー）を実行すると、環境変数が反映されます。

---

## 手順 4: 動作確認

1. デプロイ完了後に表示される URL（例: `https://xxxx.vercel.app`）を開きます。
2. トップ `/` は `login.html` にリダイレクトされます。
3. `data/users.json` のアカウントでログインします（初期例: `admin@example.com` / `admin`）。
4. 物件が登録済みならダッシュボードのデータ取得まで確認します。

**クリーン URL**: `cleanUrls` 有効のため、`/settings` や `/dashboard` でも HTML にアクセスできます。

---

## 注意（本番での data 書き込み）

Vercel のサーバーレス環境では **ファイルシステムへの永続書き込みができません**。

- **物件の追加・編集・削除、アクティブ物件の変更、パスワード変更** は、本番では `503` と案内メッセージが返る実装になっています。
- 運用案:
  - **`data/properties.json` / `data/users.json` を Git で編集して再デプロイ**する、または
  - 将来 **Vercel Postgres / KV** 等にデータを移す。

ログイン・セッション（JWT）と、**読み取り専用で載っている `data` の内容**に基づく **デジタライズ API の参照（ダッシュボード等）** は、環境変数と物件データが揃っていれば利用できます。

---

## ローカル開発（従来どおり）

```bash
npm run dev
# → node proxy-server.js（ポート 3001、メモリセッション + data への書き込み可）
```

ブラウザでは `http://localhost:3001/login.html`（または `/`）を開きます。

**Vercel と同じルーティングで試す**場合:

```bash
npx vercel dev
```

---

## トラブルシュート

| 現象 | 確認すること |
|------|----------------|
| ログイン直後に 401 | `JWT_SECRET` を設定して再デプロイしたか。 |
| 顧客データが取れない | `SECRET_ID` / `SECRET_PASSWORD` と、`data/properties.json` の物件・DB 情報。 |
| 設定で保存できない | 上記「本番での data 書き込み」の制限。ローカルで `proxy-server.js` を使うか、JSON を Git で更新して再デプロイ。 |

---

## 構成の整理

| 環境 | 役割 |
|------|------|
| **Vercel** | 静的ファイル + `api/handler.js`（JWT、読み取り `data`、デジタライズ HTTPS プロキシ） |
| **ローカル** | `proxy-server.js`（従来のメモリセッション + `data` への読み書き） |

フロントは `common.js` / `login.html` で、**`http(s)` で開いているときは API を同一オリジン（相対パス）** に向けるため、Vercel の URL でも追加設定は不要です。
