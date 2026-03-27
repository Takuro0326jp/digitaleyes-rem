/**
 * デジタライズ由来の行と、ローカル上書き（local_overrides）のマージ
 */
function mergeRemoteAndOverrides(remoteRow, overridesObj) {
  const out = { ...remoteRow };
  if (!overridesObj || typeof overridesObj !== "object") return out;
  for (const [k, v] of Object.entries(overridesObj)) {
    if (v === undefined) continue;
    if (v === null) {
      if (Object.prototype.hasOwnProperty.call(remoteRow, k)) out[k] = remoteRow[k];
      else delete out[k];
      continue;
    }
    out[k] = v;
  }
  return out;
}

function denormFromMerged(merged) {
  return {
    name: merged["c.name"] ?? null,
    kana: merged["c.kana"] ?? null,
    mail: merged["c.mail"] ?? null,
    tel: merged["c.tel"] ?? null,
    state: merged["c.state"] ?? null,
    city: merged["c.city"] ?? null,
    baitai: merged["c.baitai"] ?? null,
    status: merged["c.status"] ?? null,
    ninzu: merged["c.ninzu"] ?? null,
    yosan: merged["c.yosan"] ?? null,
    jikosikin: merged["c.jikosikin"] ?? null,
    questionnaire23: merged["c.questionnaire23"] ?? null,
    questionnaire24: merged["c.questionnaire24"] ?? null,
    date_entry: merged["c.date_entry"] ?? null,
    upd_date: merged["c.upd_date"] ?? null,
  };
}

/** UPSERT 用 args: property_id, customer_id, 18 denorm..., payload, remote_payload, local_overrides */
/** @param remoteSnapshotRow デジタライズ API 由来の生スナップショット（remote_payload 列用。マージ前の値を保持） */
function rowArgsForUpsert(propertyId, remoteSnapshotRow, mergedRow, localOverridesJson) {
  const d = denormFromMerged(mergedRow);
  const cid = String(mergedRow["c.id"] ?? "");
  return [
    propertyId,
    cid,
    d.name,
    d.kana,
    d.mail,
    d.tel,
    d.state,
    d.city,
    d.baitai,
    d.status,
    d.ninzu,
    d.yosan,
    d.jikosikin,
    d.questionnaire23,
    d.questionnaire24,
    d.date_entry,
    d.upd_date,
    JSON.stringify(mergedRow),
    JSON.stringify(remoteSnapshotRow),
    localOverridesJson,
  ];
}

module.exports = {
  mergeRemoteAndOverrides,
  denormFromMerged,
  rowArgsForUpsert,
};
