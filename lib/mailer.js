const nodemailer = require("nodemailer");

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
  return nodemailer.createTransport({
    host: c.host,
    port: c.port,
    secure: Number(c.port) === 465,
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
    "Real Estate Manager への招待が完了しました。",
    "以下の情報でログインしてください。",
    "",
    `ログインURL: ${loginUrl}`,
    `メールアドレス: ${payload.email || ""}`,
    payload.tempPassword ? `仮パスワード: ${payload.tempPassword}` : "",
    "",
    "初回ログイン後にパスワード変更をお願いします。",
  ].filter(Boolean);
  const info = await transport.sendMail({
    from,
    to: payload.email,
    subject,
    text: lines.join("\n"),
  });
  return { sent: true, messageId: info.messageId };
}

module.exports = {
  hasSmtpEnv,
  sendAccountInviteMail,
};

