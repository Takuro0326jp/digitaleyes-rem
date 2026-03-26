/**
 * 素材ギャラリー用 S3 は物件メタ（extra_json 由来のトップレベル）の s3BucketName / s3Region を参照する。
 */
function s3ConfigFromProperty(prop, env = process.env) {
  if (!prop) return null;
  const bucketName = String(prop.s3BucketName || "").trim();
  if (!bucketName) return null;
  const region = String(prop.s3Region || "").trim() || String(env.AWS_REGION || "ap-northeast-1");
  return { bucketName, region };
}

module.exports = { s3ConfigFromProperty };
