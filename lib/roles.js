/**
 * 権限: 1=マスター 2=クライアント管理者 3=物件管理者
 * 4=互換（素材ギャラリー等で従来どおり許可する場合）
 */
const ROLE = {
  MASTER: 1,
  CLIENT_ADMIN: 2,
  PROPERTY_MANAGER: 3,
  LEGACY_EXTRA: 4,
};

function normalizeRole(r) {
  const n = Number(r);
  if (n === ROLE.MASTER) return ROLE.MASTER;
  if (n === ROLE.PROPERTY_MANAGER) return ROLE.PROPERTY_MANAGER;
  if (n === ROLE.LEGACY_EXTRA) return ROLE.LEGACY_EXTRA;
  if (n === ROLE.CLIENT_ADMIN) return ROLE.CLIENT_ADMIN;
  return ROLE.CLIENT_ADMIN;
}

function isPropertyScopedRole(role) {
  return Number(role) === ROLE.PROPERTY_MANAGER;
}

/** マスター・クライアント管理者（テナント内の全物件アクセス＋管理者メニュー） */
function isAdminLike(role) {
  const n = Number(role);
  return n === ROLE.MASTER || n === ROLE.CLIENT_ADMIN;
}

function isMaster(role) {
  return Number(role) === ROLE.MASTER;
}

/** ギャラリー API 等: ログイン済みで物件作業可なロール */
function canUsePropertyMediaRoles(role) {
  return [ROLE.MASTER, ROLE.CLIENT_ADMIN, ROLE.PROPERTY_MANAGER, ROLE.LEGACY_EXTRA].includes(
    Number(role)
  );
}

module.exports = {
  ROLE,
  normalizeRole,
  isPropertyScopedRole,
  isAdminLike,
  isMaster,
  canUsePropertyMediaRoles,
};
