const { isPropertyScopedRole } = require("./roles");

/**
 * ログイン直後: ログイン時表示に設定された物件があれば activePropertyId をそれに合わせる。
 */
async function applyLoginDefaultToUser(user, { listPropertiesByUser, updateUser }) {
  if (!user) return null;
  const propsAll = await listPropertiesByUser(user.tenantId);
  const visible =
    isPropertyScopedRole(user.role)
      ? (() => {
          if (!Array.isArray(user.propertyIds)) return [];
          const idSet = new Set(user.propertyIds.map(String));
          return propsAll.filter((p) => idSet.has(String(p.id)));
        })()
      : propsAll;
  const ids = new Set(visible.map((p) => String(p.id)));
  const def = user.loginDefaultPropertyId ? String(user.loginDefaultPropertyId) : "";
  let nextActive = user.activePropertyId != null && user.activePropertyId !== "" ? String(user.activePropertyId) : null;
  if (def && ids.has(def)) {
    nextActive = def;
  } else if (!nextActive || !ids.has(nextActive)) {
    nextActive = visible[0] ? String(visible[0].id) : null;
  }
  if (String(user.activePropertyId || "") === String(nextActive || "")) return user;
  const next = { ...user, activePropertyId: nextActive };
  await updateUser(next);
  return next;
}

module.exports = { applyLoginDefaultToUser };
