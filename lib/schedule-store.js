const { getDb } = require("./sync-db");

let _migrated = false;

async function migrate() {
  if (_migrated) return;
  const db = await getDb();
  await db.execute(`CREATE TABLE IF NOT EXISTS rooms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id TEXT NOT NULL,
    name TEXT NOT NULL,
    sort_order INTEGER DEFAULT 0
  )`);
  await db.execute(`CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id TEXT NOT NULL,
    room_id INTEGER NULL,
    customer_id TEXT NULL,
    reservation_date TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    source TEXT NOT NULL DEFAULT 'manual',
    reception_status TEXT NULL,
    staff_id TEXT NULL,
    participants INTEGER NOT NULL DEFAULT 1,
    meeting_type TEXT NULL,
    web_meeting_url TEXT NULL,
    customer_name_sei TEXT NULL,
    customer_name_mei TEXT NULL,
    customer_kana_sei TEXT NULL,
    customer_kana_mei TEXT NULL,
    customer_tel TEXT NULL,
    customer_email TEXT NULL,
    customer_status INTEGER NULL,
    memo TEXT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
  )`);
  await db.execute("CREATE INDEX IF NOT EXISTS idx_sched_prop_date ON schedules(property_id, reservation_date)");
  await db.execute("CREATE INDEX IF NOT EXISTS idx_sched_prop_staff ON schedules(property_id, staff_id)");
  await db.execute(`CREATE TABLE IF NOT EXISTS property_schedule_settings (
    property_id TEXT PRIMARY KEY,
    auto_slot_minutes INTEGER NOT NULL DEFAULT 60,
    daily_limit INTEGER NOT NULL DEFAULT 20,
    booking_open_days INTEGER NOT NULL DEFAULT 30,
    updated_at TEXT DEFAULT (datetime('now'))
  )`);
  _migrated = true;
}

async function ensureDefaultRooms(propertyId) {
  const db = await getDb();
  const r = await db.execute({
    sql: "SELECT COUNT(*) AS n FROM rooms WHERE property_id = ?",
    args: [propertyId],
  });
  const n = Number(r.rows?.[0]?.n ?? 0);
  if (n > 0) return;
  await db.batch([
    { sql: "INSERT INTO rooms(property_id, name, sort_order) VALUES(?, ?, ?)", args: [propertyId, "A room", 1] },
    { sql: "INSERT INTO rooms(property_id, name, sort_order) VALUES(?, ?, ?)", args: [propertyId, "B room", 2] },
  ]);
}

async function listRooms(propertyId) {
  await migrate();
  await ensureDefaultRooms(propertyId);
  const db = await getDb();
  const r = await db.execute({
    sql: "SELECT id, name, sort_order FROM rooms WHERE property_id = ? ORDER BY sort_order ASC, id ASC",
    args: [propertyId],
  });
  return (r.rows || []).map((x) => ({
    id: String(x.id),
    name: x.name,
    sortOrder: Number(x.sort_order || 0),
  }));
}

async function getScheduleSettings(propertyId) {
  await migrate();
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT property_id, auto_slot_minutes, daily_limit, booking_open_days
          FROM property_schedule_settings WHERE property_id = ? LIMIT 1`,
    args: [propertyId],
  });
  const x = r.rows?.[0];
  if (x) {
    return {
      propertyId: x.property_id,
      autoSlotMinutes: Number(x.auto_slot_minutes || 60),
      dailyLimit: Number(x.daily_limit || 20),
      bookingOpenDays: Number(x.booking_open_days || 30),
    };
  }
  await db.execute({
    sql: `INSERT INTO property_schedule_settings(property_id, auto_slot_minutes, daily_limit, booking_open_days, updated_at)
          VALUES (?, 60, 20, 30, datetime('now'))`,
    args: [propertyId],
  });
  return { propertyId, autoSlotMinutes: 60, dailyLimit: 20, bookingOpenDays: 30 };
}

async function saveScheduleSettings(propertyId, payload = {}) {
  await migrate();
  const curr = await getScheduleSettings(propertyId);
  const autoSlotMinutes = Math.max(15, Number(payload.autoSlotMinutes ?? curr.autoSlotMinutes));
  const dailyLimit = Math.max(1, Number(payload.dailyLimit ?? curr.dailyLimit));
  const bookingOpenDays = Math.max(1, Number(payload.bookingOpenDays ?? curr.bookingOpenDays));
  const db = await getDb();
  await db.execute({
    sql: `UPDATE property_schedule_settings
          SET auto_slot_minutes = ?, daily_limit = ?, booking_open_days = ?, updated_at = datetime('now')
          WHERE property_id = ?`,
    args: [autoSlotMinutes, dailyLimit, bookingOpenDays, propertyId],
  });
  return { propertyId, autoSlotMinutes, dailyLimit, bookingOpenDays };
}

function combineDateTime(date, time) {
  return new Date(`${date}T${time}:00`);
}

function overlap(aStart, aEnd, bStart, bEnd) {
  return aStart < bEnd && bStart < aEnd;
}

async function hasDoubleBooking(propertyId, roomId, reservationDate, startTime, endTime, excludeId = null) {
  if (!roomId) return false;
  const db = await getDb();
  const rows = await db.execute({
    sql: `SELECT id, start_time, end_time FROM schedules
          WHERE property_id = ? AND reservation_date = ? AND room_id = ?`,
    args: [propertyId, reservationDate, roomId],
  });
  const aS = combineDateTime(reservationDate, startTime);
  const aE = combineDateTime(reservationDate, endTime);
  for (const r of rows.rows || []) {
    if (excludeId && Number(r.id) === Number(excludeId)) continue;
    const bS = combineDateTime(reservationDate, r.start_time);
    const bE = combineDateTime(reservationDate, r.end_time);
    if (overlap(aS, aE, bS, bE)) return true;
  }
  return false;
}

function sanitizeScheduleInput(input, mode = "create") {
  const out = {
    room_id: input.roomId ? Number(input.roomId) : null,
    customer_id: input.customerId || null,
    reservation_date: input.date,
    start_time: input.startTime,
    end_time: input.endTime,
    status: input.status === "confirmed" ? "confirmed" : "pending",
    source: input.source === "form" ? "form" : "manual",
    reception_status: input.receptionStatus || null,
    staff_id: input.staffId || null,
    participants: Math.max(1, Number(input.participants || 1)),
    meeting_type: input.meetingType || null,
    web_meeting_url: input.webMeetingUrl || null,
    customer_name_sei: input.customerNameSei || null,
    customer_name_mei: input.customerNameMei || null,
    customer_kana_sei: input.customerKanaSei || null,
    customer_kana_mei: input.customerKanaMei || null,
    customer_tel: input.customerTel || null,
    customer_email: input.customerEmail || null,
    customer_status: input.customerStatus == null || input.customerStatus === "" ? null : Number(input.customerStatus),
    memo: input.memo || null,
  };
  if (!out.reservation_date || !out.start_time || !out.end_time) {
    throw new Error("日付・開始時刻・終了時刻は必須です");
  }
  if (out.end_time <= out.start_time) throw new Error("終了時刻は開始時刻より後にしてください");
  if (out.meeting_type !== "web") out.web_meeting_url = null;
  if (mode === "create" && !["pending", "confirmed"].includes(out.status)) out.status = "pending";
  return out;
}

async function createSchedule(propertyId, input) {
  await migrate();
  const payload = sanitizeScheduleInput(input, "create");
  if (await hasDoubleBooking(propertyId, payload.room_id, payload.reservation_date, payload.start_time, payload.end_time)) {
    throw new Error("同じ部屋・同じ時間帯に既存予約があります");
  }
  const db = await getDb();
  await db.execute({
    sql: `INSERT INTO schedules (
      property_id, room_id, customer_id, reservation_date, start_time, end_time, status, source,
      reception_status, staff_id, participants, meeting_type, web_meeting_url,
      customer_name_sei, customer_name_mei, customer_kana_sei, customer_kana_mei,
      customer_tel, customer_email, customer_status, memo, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
    args: [
      propertyId,
      payload.room_id,
      payload.customer_id,
      payload.reservation_date,
      payload.start_time,
      payload.end_time,
      payload.status,
      payload.source,
      payload.reception_status,
      payload.staff_id,
      payload.participants,
      payload.meeting_type,
      payload.web_meeting_url,
      payload.customer_name_sei,
      payload.customer_name_mei,
      payload.customer_kana_sei,
      payload.customer_kana_mei,
      payload.customer_tel,
      payload.customer_email,
      payload.customer_status,
      payload.memo,
    ],
  });
  const settings = await getScheduleSettings(propertyId);
  const cnt = await db.execute({
    sql: "SELECT COUNT(*) AS n FROM schedules WHERE property_id = ? AND reservation_date = ?",
    args: [propertyId, payload.reservation_date],
  });
  const dayCount = Number(cnt.rows?.[0]?.n || 0);
  return {
    warning: dayCount > settings.dailyLimit
      ? `予約上限数(${settings.dailyLimit})を超過しています（${dayCount}件）。`
      : "",
  };
}

async function updateSchedule(propertyId, id, input) {
  await migrate();
  const payload = sanitizeScheduleInput(input, "update");
  if (await hasDoubleBooking(propertyId, payload.room_id, payload.reservation_date, payload.start_time, payload.end_time, id)) {
    throw new Error("同じ部屋・同じ時間帯に既存予約があります");
  }
  const db = await getDb();
  await db.execute({
    sql: `UPDATE schedules SET
      room_id=?, customer_id=?, reservation_date=?, start_time=?, end_time=?, status=?, source=?,
      reception_status=?, staff_id=?, participants=?, meeting_type=?, web_meeting_url=?,
      customer_name_sei=?, customer_name_mei=?, customer_kana_sei=?, customer_kana_mei=?,
      customer_tel=?, customer_email=?, customer_status=?, memo=?, updated_at=datetime('now')
      WHERE id=? AND property_id=?`,
    args: [
      payload.room_id,
      payload.customer_id,
      payload.reservation_date,
      payload.start_time,
      payload.end_time,
      payload.status,
      payload.source,
      payload.reception_status,
      payload.staff_id,
      payload.participants,
      payload.meeting_type,
      payload.web_meeting_url,
      payload.customer_name_sei,
      payload.customer_name_mei,
      payload.customer_kana_sei,
      payload.customer_kana_mei,
      payload.customer_tel,
      payload.customer_email,
      payload.customer_status,
      payload.memo,
      Number(id),
      propertyId,
    ],
  });
}

async function deleteSchedule(propertyId, id) {
  await migrate();
  const db = await getDb();
  await db.execute({
    sql: "DELETE FROM schedules WHERE id = ? AND property_id = ?",
    args: [Number(id), propertyId],
  });
}

function toIso(date, time) {
  return `${date}T${time}:00`;
}

async function listSchedules(propertyId, filters = {}) {
  await migrate();
  const db = await getDb();
  const where = ["s.property_id = ?"];
  const args = [propertyId];

  if (filters.start) {
    where.push("s.reservation_date >= ?");
    args.push(filters.start.slice(0, 10));
  }
  if (filters.end) {
    where.push("s.reservation_date <= ?");
    args.push(filters.end.slice(0, 10));
  }
  if (filters.onlyPending === true) where.push("s.status = 'pending'");
  if (filters.onlyConfirmed === true) where.push("s.status = 'confirmed'");
  if (filters.staffId) {
    where.push("s.staff_id = ?");
    args.push(String(filters.staffId));
  }
  if (filters.roomId) {
    if (filters.roomId === "none") where.push("s.room_id IS NULL");
    else {
      where.push("s.room_id = ?");
      args.push(Number(filters.roomId));
    }
  }
  if (filters.keyword) {
    const q = `%${String(filters.keyword)}%`;
    where.push("(s.customer_name_sei LIKE ? OR s.customer_name_mei LIKE ? OR s.customer_email LIKE ? OR s.customer_tel LIKE ?)");
    args.push(q, q, q, q);
  }
  if (filters.statusValues && filters.statusValues.length) {
    const ph = filters.statusValues.map(() => "?").join(",");
    where.push(`CAST(s.customer_status AS TEXT) IN (${ph})`);
    args.push(...filters.statusValues.map(String));
  }

  const r = await db.execute({
    sql: `SELECT s.*, r.name AS room_name
          FROM schedules s
          LEFT JOIN rooms r ON r.id = s.room_id
          WHERE ${where.join(" AND ")}
          ORDER BY s.reservation_date ASC, s.start_time ASC`,
    args,
  });

  return (r.rows || []).map((x) => ({
    id: String(x.id),
    roomId: x.room_id == null ? null : String(x.room_id),
    roomName: x.room_name || null,
    customerId: x.customer_id || null,
    date: x.reservation_date,
    startTime: x.start_time,
    endTime: x.end_time,
    status: x.status,
    source: x.source,
    receptionStatus: x.reception_status || "",
    staffId: x.staff_id || "",
    participants: Number(x.participants || 1),
    meetingType: x.meeting_type || "",
    webMeetingUrl: x.web_meeting_url || "",
    customerNameSei: x.customer_name_sei || "",
    customerNameMei: x.customer_name_mei || "",
    customerKanaSei: x.customer_kana_sei || "",
    customerKanaMei: x.customer_kana_mei || "",
    customerTel: x.customer_tel || "",
    customerEmail: x.customer_email || "",
    customerStatus: x.customer_status == null ? "" : Number(x.customer_status),
    memo: x.memo || "",
    start: toIso(x.reservation_date, x.start_time),
    end: toIso(x.reservation_date, x.end_time),
  }));
}

module.exports = {
  listRooms,
  listSchedules,
  createSchedule,
  updateSchedule,
  deleteSchedule,
  getScheduleSettings,
  saveScheduleSettings,
};

