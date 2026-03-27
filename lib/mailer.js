const nodemailer = require("nodemailer");

function escHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/** パスワード再設定（ログインOTP / 招待と同一ビジュアル） */
function passwordResetHtml(payload = {}) {
  const name = escHtml(payload.name || "ご担当者");
  const resetUrl = String(payload.resetUrl || "").trim();
  const resetUrlEsc = escHtml(resetUrl);
  const brand = "#357cb0";
  const ink = "#465362";
  const muted = "#8a939c";
  const bg = "#f4f6f8";
  const hasUrl = Boolean(resetUrl);
  const ctaBlock = hasUrl
    ? `<tr><td style="padding:8px 24px 20px;" align="center">
<table role="presentation" cellspacing="0" cellpadding="0">
<tr><td align="center" style="border-radius:6px;background:${brand};">
<a href="${resetUrlEsc}" style="display:inline-block;padding:14px 28px;font-size:15px;font-weight:700;color:#ffffff;text-decoration:none;line-height:1.4;">パスワードを再設定する</a>
</td></tr>
</table>
<p style="margin:14px 0 0;font-size:12px;color:${muted};line-height:1.55;">ボタンが使えない場合は、次のURLをブラウザに貼り付けてください。<br><a href="${resetUrlEsc}" style="color:${brand};word-break:break-all;">${resetUrlEsc}</a></p>
</td></tr>`
    : `<tr><td style="padding:8px 24px 20px;">
<div style="padding:16px 18px;background:#fff8e6;border:1px solid #e6d4a8;border-radius:8px;">
<p style="margin:0;font-size:14px;font-weight:600;color:${ink};">再設定用リンクを表示できませんでした</p>
<p style="margin:10px 0 0;font-size:13px;color:${ink};line-height:1.65;">管理者へお問い合わせください。</p>
</div>
</td></tr>`;
  return `<!DOCTYPE html>
<html lang="ja">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:${bg};font-family:'Hiragino Sans','Hiragino Kaku Gothic ProN',Meiryo,sans-serif;">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:${bg};padding:24px 12px;">
<tr><td align="center">
<table role="presentation" width="100%" style="max-width:520px;background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.06);">
<tr><td style="background:${brand};padding:20px 24px;">
<p style="margin:0;font-size:11px;letter-spacing:.08em;color:rgba(255,255,255,.85);">DIGITALEYES</p>
<h1 style="margin:6px 0 0;font-size:18px;font-weight:700;color:#ffffff;line-height:1.35;">Real Estate Manager</h1>
<p style="margin:8px 0 0;font-size:13px;color:rgba(255,255,255,.9);">パスワード再設定</p>
</td></tr>
<tr><td style="padding:28px 24px 8px;">
<p style="margin:0;font-size:15px;color:${ink};line-height:1.6;">${name} 様</p>
<p style="margin:16px 0 0;font-size:14px;color:${ink};line-height:1.65;">パスワード再設定のリクエストを受け付けました。下のボタンから新しいパスワードを設定してください。</p>
<p style="margin:14px 0 0;font-size:13px;color:${ink};line-height:1.65;">心当たりがない場合は、このメールは破棄してください。アカウントのパスワードは変更されません。</p>
</td></tr>
${ctaBlock}
<tr><td style="padding:0 24px 24px;">
<p style="margin:0;font-size:12px;color:${muted};line-height:1.6;">リンクの有効期限は <strong style="color:${ink};">60分</strong> です。期限後はログイン画面の「パスワードを忘れた方はこちら」から再度お手続きください。</p>
</td></tr>
<tr><td style="padding:16px 24px;background:#fafbfc;border-top:1px solid #eceff2;">
<p style="margin:0;font-size:11px;color:#b0b8bf;line-height:1.5;text-align:center;">このメールは送信専用です。返信には回答できません。</p>
</td></tr>
</table>
<p style="margin:16px 0 0;font-size:11px;color:#a8b0b8;text-align:center;">&copy; DIGITALEYES</p>
</td></tr>
</table>
</body>
</html>`;
}

/** ログインOTP用 HTML（インラインCSSのみ・主要クライアント向け） */
function loginOtpHtml(payload = {}, code) {
  const name = escHtml(payload.name || "ご担当者");
  const codeEsc = escHtml(code);
  const brand = "#357cb0";
  const ink = "#465362";
  const muted = "#8a939c";
  const bg = "#f4f6f8";
  return `<!DOCTYPE html>
<html lang="ja">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:${bg};font-family:'Hiragino Sans','Hiragino Kaku Gothic ProN',Meiryo,sans-serif;">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:${bg};padding:24px 12px;">
<tr><td align="center">
<table role="presentation" width="100%" style="max-width:520px;background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.06);">
<tr><td style="background:${brand};padding:20px 24px;">
<p style="margin:0;font-size:11px;letter-spacing:.08em;color:rgba(255,255,255,.85);">DIGITALEYES</p>
<h1 style="margin:6px 0 0;font-size:18px;font-weight:700;color:#ffffff;line-height:1.35;">Real Estate Manager</h1>
<p style="margin:8px 0 0;font-size:13px;color:rgba(255,255,255,.9);">ログイン認証コード</p>
</td></tr>
<tr><td style="padding:28px 24px 8px;">
<p style="margin:0;font-size:15px;color:${ink};line-height:1.6;">${name} 様</p>
<p style="margin:16px 0 0;font-size:14px;color:${ink};line-height:1.65;">以下の認証コードをログイン画面に入力してください。</p>
</td></tr>
<tr><td style="padding:8px 24px 24px;" align="center">
<table role="presentation" cellspacing="0" cellpadding="0" style="width:100%;max-width:320px;background:#f0f4f8;border-radius:8px;border:1px solid #e2e8ee;">
<tr><td align="center" style="padding:20px 16px;">
<p style="margin:0 0 8px;font-size:11px;color:${muted};letter-spacing:.06em;">認証コード（6桁）</p>
<p style="margin:0;font-size:28px;font-weight:700;letter-spacing:0.35em;color:${brand};font-family:Consolas,Monaco,monospace;">${codeEsc}</p>
</td></tr>
</table>
</td></tr>
<tr><td style="padding:0 24px 24px;">
<p style="margin:0;font-size:12px;color:${muted};line-height:1.6;">有効期限は <strong style="color:${ink};">10分</strong> です。期限を過ぎた場合は、再度ログイン操作からお試しください。</p>
<p style="margin:14px 0 0;font-size:12px;color:${muted};line-height:1.6;">※ このメールに心当たりがない場合は、第三者があなたのアカウントへのアクセスを試みた可能性があります。コードを使わず、このメールを破棄してください。</p>
</td></tr>
<tr><td style="padding:16px 24px;background:#fafbfc;border-top:1px solid #eceff2;">
<p style="margin:0;font-size:11px;color:#b0b8bf;line-height:1.5;text-align:center;">このメールは送信専用です。返信には回答できません。</p>
</td></tr>
</table>
<p style="margin:16px 0 0;font-size:11px;color:#a8b0b8;text-align:center;">&copy; DIGITALEYES</p>
</td></tr>
</table>
</body>
</html>`;
}

/** アカウント招待メール用 HTML（OTP と同一ビジュアル言語・インライン CSS） */
function accountInviteHtml(payload = {}) {
  const name = escHtml(payload.name || "ご担当者");
  const email = escHtml(payload.email || "");
  const loginUrl = String(payload.loginUrl || "").trim();
  const loginUrlEsc = escHtml(loginUrl);
  const tempPassword = payload.tempPassword ? String(payload.tempPassword) : "";
  const pwdEsc = escHtml(tempPassword);
  const brand = "#357cb0";
  const ink = "#465362";
  const muted = "#8a939c";
  const bg = "#f4f6f8";
  const pwdBlock = tempPassword
    ? `<p style="margin:14px 0 0;font-size:13px;color:${ink};line-height:1.5;"><span style="color:${muted};font-size:11px;letter-spacing:.06em;display:block;margin-bottom:6px;">仮パスワード</span><span style="font-weight:600;font-family:Consolas,Monaco,monospace;word-break:break-all;font-size:15px;">${pwdEsc}</span></p>`
    : "";
  const hasUrl = Boolean(loginUrl);
  const ctaBlock = hasUrl
    ? `<tr><td style="padding:8px 24px 20px;" align="center">
<table role="presentation" cellspacing="0" cellpadding="0">
<tr><td align="center" style="border-radius:6px;background:${brand};">
<a href="${loginUrlEsc}" style="display:inline-block;padding:14px 28px;font-size:15px;font-weight:700;color:#ffffff;text-decoration:none;line-height:1.4;">ログイン画面を開く</a>
</td></tr>
</table>
<p style="margin:14px 0 0;font-size:12px;color:${muted};line-height:1.55;">ボタンが使えない場合は、次のURLをブラウザに貼り付けてください。<br><a href="${loginUrlEsc}" style="color:${brand};word-break:break-all;">${loginUrlEsc}</a></p>
</td></tr>`
    : `<tr><td style="padding:8px 24px 20px;">
<div style="padding:16px 18px;background:#fff8e6;border:1px solid #e6d4a8;border-radius:8px;">
<p style="margin:0;font-size:14px;font-weight:600;color:${ink};">ログインURLを同封できませんでした</p>
<p style="margin:10px 0 0;font-size:13px;color:${ink};line-height:1.65;">管理者から<strong>ログイン画面のURL</strong>を別途ご案内ください。メールアドレスと仮パスワードは下記をご利用いただけます。</p>
</div>
</td></tr>`;
  return `<!DOCTYPE html>
<html lang="ja">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:${bg};font-family:'Hiragino Sans','Hiragino Kaku Gothic ProN',Meiryo,sans-serif;">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:${bg};padding:24px 12px;">
<tr><td align="center">
<table role="presentation" width="100%" style="max-width:520px;background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.06);">
<tr><td style="background:${brand};padding:20px 24px;">
<p style="margin:0;font-size:11px;letter-spacing:.08em;color:rgba(255,255,255,.85);">DIGITALEYES</p>
<h1 style="margin:6px 0 0;font-size:18px;font-weight:700;color:#ffffff;line-height:1.35;">Real Estate Manager</h1>
<p style="margin:8px 0 0;font-size:13px;color:rgba(255,255,255,.9);">アカウント招待</p>
</td></tr>
<tr><td style="padding:28px 24px 8px;">
<p style="margin:0;font-size:15px;color:${ink};line-height:1.6;">${name} 様</p>
<p style="margin:16px 0 0;font-size:14px;color:${ink};line-height:1.65;">Real Estate Manager への招待が届きました。${hasUrl ? "下のボタンからログインし、" : ""}初回ログイン後にパスワードの変更をお願いします。</p>
</td></tr>
${ctaBlock}
<tr><td style="padding:0 24px 24px;">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f7f9fb;border-radius:8px;border:1px solid #e8edf2;">
<tr><td style="padding:18px 20px;">
<p style="margin:0 0 10px;font-size:11px;color:${muted};letter-spacing:.06em;">ログイン情報</p>
<p style="margin:0 0 8px;font-size:13px;color:${ink};line-height:1.5;"><span style="color:${muted};">メールアドレス</span><br><span style="font-weight:600;">${email}</span></p>
${pwdBlock}
</td></tr>
</table>
<p style="margin:16px 0 0;font-size:12px;color:${muted};line-height:1.65;">※ 仮パスワードは第三者に知られないようご注意ください。心当たりがない場合は、このメールを破棄し、管理者へご連絡ください。</p>
</td></tr>
<tr><td style="padding:16px 24px;background:#fafbfc;border-top:1px solid #eceff2;">
<p style="margin:0;font-size:11px;color:#b0b8bf;line-height:1.5;text-align:center;">このメールは送信専用です。返信には回答できません。</p>
</td></tr>
</table>
<p style="margin:16px 0 0;font-size:11px;color:#a8b0b8;text-align:center;">&copy; DIGITALEYES</p>
</td></tr>
</table>
</body>
</html>`;
}

function smtpConfig(env = {}) {
  const host = env.SMTP_HOST || "";
  const port = Number(env.SMTP_PORT || 587);
  const user = env.SMTP_USER || "";
  const pass = env.SMTP_PASS || "";
  return { host, port, user, pass };
}

function hasSmtpEnv(env = {}) {
  const c = smtpConfig(env);
  return Boolean(c.host && c.port && c.user && c.pass);
}

function createTransport(env = {}) {
  const c = smtpConfig(env);
  const port = Number(c.port);
  const secure = port === 465;
  return nodemailer.createTransport({
    host: c.host,
    port,
    secure,
    // 587 番台の STARTTLS で多くのプロバイダが要求する
    requireTLS: !secure && port === 587,
    auth: { user: c.user, pass: c.pass },
  });
}

async function sendAccountInviteMail(env = {}, payload = {}) {
  if (!hasSmtpEnv(env)) {
    return { sent: false, skipped: true, reason: "smtp_not_configured" };
  }
  const transport = createTransport(env);
  const from = env.MAIL_FROM || env.SMTP_USER;
  const subject = "【Real Estate Manager】アカウント招待のお知らせ";
  const loginUrl = payload.loginUrl || env.APP_LOGIN_URL || "";
  const lines = [
    `${payload.name || "ご担当者"} 様`,
    "",
    "Real Estate Manager への招待が届きました。",
    "以下の情報でログインし、初回ログイン後にパスワードの変更をお願いします。",
    "",
    loginUrl ? `ログインURL: ${loginUrl}` : "ログインURL: （管理者にご確認ください）",
    `メールアドレス: ${payload.email || ""}`,
    payload.tempPassword ? `仮パスワード: ${payload.tempPassword}` : "",
    "",
    "※ 仮パスワードは第三者に知られないようご注意ください。",
  ].filter(Boolean);
  const info = await transport.sendMail({
    from,
    to: payload.email,
    subject,
    text: lines.join("\n"),
    html: accountInviteHtml({ ...payload, loginUrl }),
  });
  return { sent: true, messageId: info.messageId };
}

/** 管理者が SMTP 疎通を確認する用（自分宛のみ） */
async function sendSmtpTestMail(env = {}, payload = {}) {
  if (!hasSmtpEnv(env)) {
    throw new Error("SMTP が未設定です");
  }
  const to = String(payload.to || "").trim();
  if (!to) throw new Error("送信先メールアドレスがありません");
  const transport = createTransport(env);
  const from = env.MAIL_FROM || env.SMTP_USER;
  const info = await transport.sendMail({
    from,
    to,
    subject: "【Real Estate Manager】メール送信テスト",
    text: [
      "このメールはアカウント設定画面からのテスト送信です。",
      "SMTP（SMTP_HOST / SMTP_PORT 等）の設定が有効であることを示します。",
      "",
      `送信時刻: ${new Date().toISOString()}`,
    ].join("\n"),
  });
  return { sent: true, messageId: info.messageId };
}

/** ログイン2段階認証コード送信 */
async function sendLoginOtpMail(env = {}, payload = {}) {
  if (!hasSmtpEnv(env)) {
    throw new Error("2段階認証メールを送信できません。SMTP設定が未登録です");
  }
  const to = String(payload.email || "").trim();
  const code = String(payload.code || "").trim();
  if (!to || !code) throw new Error("認証メール送信パラメータが不正です");
  const transport = createTransport(env);
  const from = env.MAIL_FROM || env.SMTP_USER;
  const textBody = [
    `${payload.name || "ご担当者"} 様`,
    "",
    "ログイン認証コードをお知らせします。",
    `認証コード: ${code}`,
    "",
    "このコードの有効期限は 10 分です。",
    "心当たりがない場合は、このメールを破棄してください。",
  ].join("\n");
  const info = await transport.sendMail({
    from,
    to,
    subject: "【Real Estate Manager】ログイン認証コード",
    text: textBody,
    html: loginOtpHtml(payload, code),
  });
  return { sent: true, messageId: info.messageId };
}

/** パスワード再設定メール */
async function sendPasswordResetMail(env = {}, payload = {}) {
  if (!hasSmtpEnv(env)) {
    throw new Error("パスワード再設定メールを送信できません。SMTP設定が未登録です");
  }
  const to = String(payload.email || "").trim();
  const resetUrl = String(payload.resetUrl || "").trim();
  if (!to) throw new Error("送信先メールアドレスがありません");
  const transport = createTransport(env);
  const from = env.MAIL_FROM || env.SMTP_USER;
  const textBody = [
    `${payload.name || "ご担当者"} 様`,
    "",
    "パスワード再設定のリクエストを受け付けました。",
    resetUrl ? `次のURLから新しいパスワードを設定してください（60分以内）:\n${resetUrl}` : "再設定用URLを同封できませんでした。管理者へお問い合わせください。",
    "",
    "心当たりがない場合は、このメールを破棄してください。",
  ].join("\n");
  const info = await transport.sendMail({
    from,
    to,
    subject: "【Real Estate Manager】パスワード再設定のご案内",
    text: textBody,
    html: passwordResetHtml({ ...payload, resetUrl }),
  });
  return { sent: true, messageId: info.messageId };
}

module.exports = {
  hasSmtpEnv,
  sendAccountInviteMail,
  sendSmtpTestMail,
  sendLoginOtpMail,
  sendPasswordResetMail,
};

