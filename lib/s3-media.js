const crypto = require("crypto");
const {
  S3Client,
  CreateBucketCommand,
  PutBucketCorsCommand,
  PutPublicAccessBlockCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
} = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");

function envOf(env = process.env) {
  const region = String(env.AWS_REGION || "ap-northeast-1");
  const expiresSec = Math.max(60, Number(env.AWS_S3_UPLOAD_EXPIRES || 300));
  const maxFileSize = Math.max(1024, Number(env.AWS_S3_MAX_FILE_SIZE || 104857600));
  const appUrl = String(env.APP_URL || env.APP_LOGIN_URL || "").replace(/\/+$/, "");
  return { region, expiresSec, maxFileSize, appUrl };
}

function s3Client(env = process.env) {
  const e = envOf(env);
  return new S3Client({ region: e.region });
}

function s3ClientForRegion(regionOverride, env = process.env) {
  const e = envOf(env);
  const region = String(regionOverride || e.region || "ap-northeast-1");
  return new S3Client({ region });
}

function randomSuffix(len = 6) {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let out = "";
  for (let i = 0; i < len; i++) out += chars[Math.floor(Math.random() * chars.length)];
  return out;
}

function generateBucketName(accountId) {
  const base = String(accountId || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  const prefix = (base.slice(0, 8) || "account00").padEnd(8, "0");
  return `digitaleyes-${prefix}-${randomSuffix(6)}`;
}

async function createAccountBucket(accountId, env = process.env) {
  const e = envOf(env);
  const s3 = s3Client(env);
  const bucketName = generateBucketName(accountId);

  const createInput = { Bucket: bucketName };
  if (e.region !== "us-east-1") {
    createInput.CreateBucketConfiguration = { LocationConstraint: e.region };
  }
  await s3.send(new CreateBucketCommand(createInput));

  await s3.send(new PutPublicAccessBlockCommand({
    Bucket: bucketName,
    PublicAccessBlockConfiguration: {
      BlockPublicAcls: true,
      IgnorePublicAcls: true,
      BlockPublicPolicy: true,
      RestrictPublicBuckets: true,
    },
  }));

  const allowedOrigins = e.appUrl ? [e.appUrl] : ["*"];
  await s3.send(new PutBucketCorsCommand({
    Bucket: bucketName,
    CORSConfiguration: {
      CORSRules: [{
        AllowedOrigins: allowedOrigins,
        AllowedMethods: ["GET", "PUT", "POST", "DELETE", "HEAD"],
        AllowedHeaders: ["*"],
        ExposeHeaders: ["ETag"],
        MaxAgeSeconds: 3000,
      }],
    },
  }));
  return { bucketName, region: e.region };
}

function buildAssetKey(propertyId, fileName) {
  const safe = String(fileName || "file").replace(/[^\w.\-\u3040-\u30ff\u4e00-\u9faf]/g, "_");
  const folder = propertyId ? String(propertyId) : "common";
  return `assets/${folder}/${crypto.randomUUID()}_${safe}`;
}

async function createUploadPresignedUrl({ bucketName, key, mimeType, fileSize, userId, accountId, region }, env = process.env) {
  const e = envOf(env);
  if (!bucketName) throw new Error("S3バケットが未設定です");
  if (!key) throw new Error("S3キーが未設定です");
  const size = Number(fileSize || 0);
  if (!Number.isFinite(size) || size <= 0) throw new Error("fileSize が不正です");
  if (size > e.maxFileSize) throw new Error(`ファイルサイズ上限を超えています（最大 ${e.maxFileSize} bytes）`);
  const s3 = s3ClientForRegion(region, env);
  const cmd = new PutObjectCommand({
    Bucket: bucketName,
    Key: key,
    ContentType: String(mimeType || "application/octet-stream"),
    ContentLength: size,
    Metadata: {
      "uploaded-by": String(userId || ""),
      "account-id": String(accountId || ""),
    },
  });
  const presignedUrl = await getSignedUrl(s3, cmd, { expiresIn: e.expiresSec });
  return { presignedUrl };
}

async function createReadSignedUrl({ bucketName, key, expiresIn = 3600, region }, env = process.env) {
  if (!bucketName || !key) return "";
  const s3 = s3ClientForRegion(region, env);
  const cmd = new GetObjectCommand({ Bucket: bucketName, Key: key });
  return getSignedUrl(s3, cmd, { expiresIn });
}

async function removeObject({ bucketName, key, region }, env = process.env) {
  if (!bucketName || !key) return;
  const s3 = s3ClientForRegion(region, env);
  await s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: key }));
}

module.exports = {
  envOf,
  generateBucketName,
  createAccountBucket,
  buildAssetKey,
  createUploadPresignedUrl,
  createReadSignedUrl,
  removeObject,
  s3ClientForRegion,
};

