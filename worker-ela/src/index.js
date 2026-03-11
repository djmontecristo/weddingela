import webpush from "web-push";
// DJ MonteCristo Digital Worker (D1)
// - tokens: digital_tokens
// - sessions: admin_sessions
// - forms: client_forms
// - rsvps: rsvps
// - archive: archive
// =========================
// Admin session helpers
// =========================

async function adminSessionFromReq(req, env) {
  const auth = req.headers.get("authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
  if (!token) return null;

  const row = await env.DB.prepare(
    `SELECT session_token, expires_at
     FROM admin_sessions
     WHERE session_token = ?
     LIMIT 1`
  ).bind(token).first();
  if (!row) return null;
  if (Date.now() >= new Date(row.expires_at).getTime()) return null;

  return token;
}

async function partnerSessionFromReq(req, env) {
  const auth = req.headers.get("authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
  if (!token) return null;

  const row = await env.DB.prepare(
    `SELECT session_token, partner_id, expires_at
     FROM partner_sessions
     WHERE session_token = ?
     LIMIT 1`
  ).bind(token).first();

  if (!row) return null;
  if (Date.now() >= new Date(row.expires_at).getTime()) return null;

  return row;
}

function randomSessionToken(bytes = 24) {
  const arr = new Uint8Array(bytes);
  crypto.getRandomValues(arr);
  let s = btoa(String.fromCharCode(...arr));
  s = s.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
  return s;
}

function addMinutesISO(date, minutes) {
  const d = new Date(date.getTime() + minutes * 60 * 1000);
  return d.toISOString();
}

// =========================
// Worker
// =========================
export default {
  async fetch(request, env, ctx) {

    try {
    const url = new URL(request.url);
    const path = url.pathname;

// =========================
// REF LINK /ref/CODE
// =========================
if (path.startsWith("/ref/")) {

  const refCode = path.split("/")[2]?.trim().toUpperCase() || "";

  if (!refCode) {
    return new Response("Invalid ref", { status: 400 });
  }

  // εύρεση partner
  const partner = await env.DB.prepare(
    `SELECT id
     FROM partners
     WHERE ref_code = ?
       AND is_active = 1
     LIMIT 1`
  ).bind(refCode).first();

  if (!partner) {
    return Response.redirect(url.origin, 302);
  }

  const partner_id = partner.id;

  const ip =
    request.headers.get("cf-connecting-ip") ||
    request.headers.get("x-forwarded-for") ||
    "";

  const ua = request.headers.get("user-agent") || "";
  if (!ua || ua.length < 10) {
  return fetch(request);
}

  // anti-spam: ίδιο partner + ίδιο IP στα τελευταία 30 λεπτά
  const existing = await env.DB.prepare(
    `SELECT id
     FROM ref_clicks
     WHERE partner_id = ?
       AND ip = ?
       AND created_at > datetime('now','-30 minutes')
     LIMIT 1`
  ).bind(partner_id, ip).first();
//test
//  if (!existing) {
  //  await env.DB.prepare(
    //  `INSERT INTO ref_clicks (partner_id, ip, user_agent)
  //     VALUES (?, ?, ?)`
  //  ).bind(partner_id, ip, ua).run();
  //}
  
if (!existing) {

  await env.DB.prepare(
    `INSERT INTO ref_clicks (partner_id, ip, user_agent)
     VALUES (?, ?, ?)`
  ).bind(partner_id, ip, ua).run();
}
  // αφήνουμε το request να συνεχίσει
  return fetch(request);
}

    // ---- CORS / preflight ----

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }
// =========================
// PUBLIC: LEAD CREATE (Interest)
// body: { groom_first, bride_first, email, phone, wedding_date, ref? }
// =========================
if (path === "/api/leads/create" && request.method === "POST") {
  const body = await safeJson(request);

  const turnstile_token = String(body.turnstile_token || "").trim();
  if (!turnstile_token) {
    return json({ ok: false, error: "Missing Turnstile token" }, 400);
  }

  const email = (body.email || "").trim() || null;
  const phone = (body.phone || "").trim() || null;
  const wedding_date = (body.wedding_date || "").trim() || null;
  let ref = (body.ref || "").trim() || null;

if (ref) {
  const pr = await env.DB.prepare(
    `SELECT ref_code
     FROM partners
     WHERE ref_code = ?
       AND is_active = 1
     LIMIT 1`
  ).bind(ref).first();

  if (!pr) ref = null;
}

// basic validation
if (!phone) {
  return json({ ok: false, error: "Missing phone" }, 400);
}

  // optional: validate date format if provided
  if (wedding_date && !/^\d{4}-\d{2}-\d{2}$/.test(wedding_date)) {
    return json({ ok: false, error: "wedding_date must be YYYY-MM-DD" }, 400);
  }

// insert lead
const res = await env.DB.prepare(
  `INSERT INTO leads (email, phone, wedding_date, status, ref)
   VALUES (?, ?, ?, 'new', ?)`
).bind(email, phone, wedding_date, ref).run();

const pendingCount = await getPendingLeadsCount(env);

// send push to admin (do not fail lead creation if push fails)
try {
  await sendPushToAdmins(env, {
    title: "Νέο ενδιαφέρον",
    body: `Υπάρχουν ${pendingCount} νέα ενδιαφερόμενα ζευγάρια`,
    badge: pendingCount
  });
} catch (e) {
  console.log("PUSH_AFTER_LEAD_CREATE_FAILED", String(e?.message || e));
}

return json({
  ok: true,
  lead_id: res?.meta?.last_row_id ?? null,
  pending_count: pendingCount
}, 200);
}
    // health
    if (path === "/api/health") {
      return new Response("WORKER OK", { status: 200, headers: corsHeaders() });
    }

// =========================
// ADMIN: PUSH SUBSCRIBE
// body: PushSubscription JSON
// =========================
if (path === "/api/push/subscribe" && request.method === "POST") {
  if (!(await adminOk(request))) {
    return json({ ok: false, error: "Unauthorized" }, 401);
  }

  const body = await safeJson(request);

  const endpoint = String(body?.endpoint || "").trim();
  const p256dh = String(body?.keys?.p256dh || "").trim();
  const auth = String(body?.keys?.auth || "").trim();

  if (!endpoint || !p256dh || !auth) {
    return json({ ok: false, error: "Missing subscription fields" }, 400);
  }

  await env.DB.prepare(
    `INSERT INTO push_subscriptions (endpoint, p256dh, auth)
     VALUES (?, ?, ?)
     ON CONFLICT(endpoint) DO UPDATE SET
       p256dh = excluded.p256dh,
       auth   = excluded.auth`
  ).bind(endpoint, p256dh, auth).run();

  return json({ ok: true }, 200);
}
    // =========================
    // CONFIG
    // =========================
    const BASE_URL = "";

    // =========================
    // ADMIN AUTH
    // =========================
    async function adminOk(req) {
      const auth = req.headers.get("authorization") || "";
      const bearer = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
      if (!bearer) return false;

      // fallback (old way)
      if (env.ADMIN_PASSWORD && bearer === env.ADMIN_PASSWORD) return true;

      // session way
      const sess = await adminSessionFromReq(req, env);
      return !!sess;
    }

    // =========================
    // TOKEN HELPERS
    // =========================
    async function getTokenRow(env, token) {
      return await env.DB.prepare(
 `SELECT token, kind, form_token, email, phone, disabled, paid, paid_amount, partner_ref, expires_at, created_at
 FROM digital_tokens
 WHERE token = ?
 LIMIT 1`
      ).bind(token).first();
    }

    function normalizeToken(t) {
      return (t || "").trim().toLowerCase();
    }

    function makeToken() {
      const letters = "abcdefghijklmnopqrstuvwxyz";
      const randLetters = (n) =>
        Array.from({ length: n }, () => letters[Math.floor(Math.random() * letters.length)]).join("");
      const randDigits = (n) =>
        Array.from({ length: n }, () => String(Math.floor(Math.random() * 10))).join("");
      return `${randLetters(3)}${randDigits(4)}${randLetters(3)}`;
    }

    async function uniqueToken(env) {
      for (let i = 0; i < 40; i++) {
        const t = makeToken();
        const exists = await env.DB.prepare(`SELECT token FROM digital_tokens WHERE token = ?`).bind(t).first();
        if (!exists) return t;
      }
      throw new Error("Failed to generate unique token");
    }

    // Paid state is per event/root (same for all sibling tokens)
async function getPaidForRoot(env, rootToken) {
  const row = await env.DB.prepare(
    `SELECT MAX(paid_amount) AS paid_amount
     FROM digital_tokens
     WHERE form_token = ? OR token = ?`
  ).bind(rootToken, rootToken).first();

  return row?.paid_amount ?? null;
}

    // =========================
    // DATE HELPERS
    // =========================
    function parseYYYYMMDD(s) {
      if (!s) return null;
      const m = String(s).trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (!m) return null;
      const y = Number(m[1]), mo = Number(m[2]), d = Number(m[3]);
      if (mo < 1 || mo > 12 || d < 1 || d > 31) return null;
      return { y, mo, d };
    }

    function toDateUTC({ y, mo, d }) {
      return new Date(Date.UTC(y, mo - 1, d, 0, 0, 0));
    }

    function addMonthsUTC(dateUTC, months) {
      const y = dateUTC.getUTCFullYear();
      const m = dateUTC.getUTCMonth();
      const d = dateUTC.getUTCDate();
      const target = new Date(Date.UTC(y, m + months, 1, 0, 0, 0));
      const lastDay = new Date(Date.UTC(target.getUTCFullYear(), target.getUTCMonth() + 1, 0)).getUTCDate();
      target.setUTCDate(Math.min(d, lastDay));
      return target;
    }

    function addDaysUTC(dateUTC, days) {
      const t = new Date(dateUTC.getTime());
      t.setUTCDate(t.getUTCDate() + days);
      return t;
    }

    function isoDateUTC(dateUTC) {
      const y = dateUTC.getUTCFullYear();
      const m = String(dateUTC.getUTCMonth() + 1).padStart(2, "0");
      const d = String(dateUTC.getUTCDate()).padStart(2, "0");
      return `${y}-${m}-${d}`;
    }

    function nowUTC() {
      return new Date();
    }

    function isExpiredInvitation(weddingDateYYYYMMDD) {
      const p = parseYYYYMMDD(weddingDateYYYYMMDD);
      if (!p) return false;
      const eventUTC = toDateUTC(p);
      const expUTC = addMonthsUTC(eventUTC, 12);
      const expEnd = addDaysUTC(expUTC, 1);
      return nowUTC().getTime() >= expEnd.getTime();
    }

    // helper μόνο για εμφάνιση (“έως …”), όχι για κλείσιμο RSVP
    function isPastDeadline(deadlineYYYYMMDD) {
      const p = parseYYYYMMDD(deadlineYYYYMMDD);
      if (!p) return false;
      const ddlUTC = toDateUTC(p);
      const ddlEnd = addDaysUTC(ddlUTC, 1);
      return nowUTC().getTime() >= ddlEnd.getTime();
    }

    function cleanName(s) {
      return (s || "").toString().trim();
    }

    // =========================
    // ADMIN: LOGIN (creates session)
    // =========================
    if (path === "/api/admin/login" && request.method === "POST") {
      const body = await safeJson(request);
      const pw = String(body.password || "");

      if (!env.ADMIN_PASSWORD) return json({ ok: false, error: "ADMIN_PASSWORD not set" }, 500);
      if (!pw || pw !== env.ADMIN_PASSWORD) return json({ ok: false, error: "Unauthorized" }, 401);

      const sessionToken = randomSessionToken(24);
      const expiresAt = addMinutesISO(new Date(), 60 * 24 * 10);

      await env.DB.prepare(
        `INSERT INTO admin_sessions (session_token, expires_at)
         VALUES (?, ?)`
      ).bind(sessionToken, expiresAt).run();

      return json({ ok: true, session_token: sessionToken, expires_at: expiresAt }, 200);
    }

// =========================
// PARTNER: LOGIN (creates session)
// =========================
if (path === "/api/partners/login" && request.method === "POST") {
  const body = await safeJson(request);

  const phone = String(body.phone || "").trim();
  const password = String(body.password || "").trim();

  if (!phone || !password) {
    return json({ ok: false, error: "Missing phone or password" }, 400);
  }

  const partner = await env.DB.prepare(
    `SELECT id, full_name, phone, email, ref_code, password_hash, is_active
     FROM partners
     WHERE phone = ?
     LIMIT 1`
  ).bind(phone).first();

  if (!partner) {
    return json({ ok: false, error: "Invalid credentials" }, 401);
  }

  if (Number(partner.is_active) !== 1) {
    return json({ ok: false, error: "Inactive partner" }, 403);
  }

  if (!partner.password_hash) {
    return json({ ok: false, error: "Pending setup" }, 403);
  }

  // προσωρινό απλό check μέχρι να βάλουμε hash verify
  if (password !== partner.password_hash) {
    return json({ ok: false, error: "Invalid credentials" }, 401);
  }

  const sessionToken = randomSessionToken(24);
  const expiresAt = addMinutesISO(new Date(), 60 * 24 * 60); // 60 ημέρες

  await env.DB.prepare(
    `INSERT INTO partner_sessions (session_token, partner_id, expires_at)
     VALUES (?, ?, ?)`
  ).bind(sessionToken, partner.id, expiresAt).run();

  return json({
    ok: true,
    session_token: sessionToken,
    expires_at: expiresAt,
    partner: {
      id: partner.id,
      full_name: partner.full_name,
      phone: partner.phone,
      email: partner.email,
      ref_code: partner.ref_code
    }
  }, 200);
}

// =========================
// PARTNER: GET CURRENT SESSION
// =========================
if (path === "/api/partners/me" && request.method === "GET") {

  const sess = await partnerSessionFromReq(request, env);

  if (!sess) {
    return json({ ok: false, error: "Unauthorized" }, 401);
  }

  const partner = await env.DB.prepare(
    `SELECT id, full_name, phone, email, ref_code
     FROM partners
     WHERE id = ?
     LIMIT 1`
  ).bind(sess.partner_id).first();

  if (!partner) {
    return json({ ok: false, error: "Partner not found" }, 404);
  }

  return json({
    ok: true,
    partner
  }, 200);
}

// =========================
// ADMIN: PARTNERS LIST SUMMARY
// =========================
if (path === "/api/partners/list" && request.method === "GET") {
  if (!(await adminOk(request))) {
    return json({ ok: false, error: "Unauthorized" }, 401);
  }

  const priceRow = await env.DB.prepare(
    `SELECT price_c
     FROM settings
     WHERE id = 1
     LIMIT 1`
  ).first();

  const price_c = Number(priceRow?.price_c || 0);

  const rows = await env.DB.prepare(`
    SELECT
      p.id,
      p.full_name,
      p.ref_code,
      p.is_active,
      CASE
        WHEN p.password_hash IS NULL OR TRIM(p.password_hash) = '' THEN 'pending'
        WHEN p.is_active = 1 THEN 'active'
        ELSE 'inactive'
      END AS status,

      (
        SELECT COUNT(*)
        FROM ref_clicks rc
        WHERE rc.partner_id = p.id
      ) AS clicks,

      (
        SELECT COUNT(*)
        FROM leads l
        WHERE l.ref = p.ref_code
      ) AS leads,

      (
        SELECT COUNT(*)
        FROM digital_tokens dt
        WHERE dt.kind = 'form'
          AND dt.partner_ref = p.ref_code
          AND dt.paid_amount > 0
      ) AS purchases,

      (
        SELECT COALESCE(SUM(rp.amount), 0)
        FROM ref_payments rp
        WHERE rp.partner_id = p.id
      ) AS payments
    FROM partners p
    ORDER BY p.id DESC
  `).all();

  const items = (rows?.results || []).map(r => {
    const purchases = Number(r.purchases || 0);
    const payments = Number(r.payments || 0);
    const earnings = purchases * price_c;
    const balance = earnings - payments;

    return {
      id: r.id,
      full_name: r.full_name || "",
      ref_code: r.ref_code || "",
      status: r.status,
      clicks: Number(r.clicks || 0),
      leads: Number(r.leads || 0),
      purchases,
      balance
    };
  });

  return json({
    ok: true,
    price_c,
    items
  }, 200);
}

// =========================
// ADMIN: PARTNER DETAIL
// =========================
if (path === "/api/partners/detail" && request.method === "GET") {

  if (!(await adminOk(request))) {
    return json({ ok:false, error:"Unauthorized" }, 401);
  }

  const id = Number(url.searchParams.get("id"));

  if (!Number.isInteger(id) || id < 1) {
    return json({ ok:false, error:"Invalid id" }, 400);
  }

  const partner = await env.DB.prepare(`
    SELECT id, full_name, phone, email, ref_code, is_active
    FROM partners
    WHERE id = ?
    LIMIT 1
  `).bind(id).first();

  if (!partner) {
    return json({ ok:false, error:"Partner not found" }, 404);
  }

  const priceRow = await env.DB.prepare(
    `SELECT price_c FROM settings WHERE id=1`
  ).first();

  const price_c = Number(priceRow?.price_c || 0);

  const clicksRow = await env.DB.prepare(`
    SELECT COUNT(*) AS c
    FROM ref_clicks
    WHERE partner_id = ?
  `).bind(id).first();

  const leadsRow = await env.DB.prepare(`
    SELECT COUNT(*) AS c
    FROM leads
    WHERE ref = ?
  `).bind(partner.ref_code).first();

  const purchasesRow = await env.DB.prepare(`
    SELECT COUNT(*) AS c
    FROM digital_tokens
    WHERE kind='form'
      AND partner_ref = ?
      AND paid_amount > 0
  `).bind(partner.ref_code).first();

  const payments = await env.DB.prepare(`
    SELECT id, amount, created_at
    FROM ref_payments
    WHERE partner_id = ?
    ORDER BY created_at DESC
  `).bind(id).all();

  const paymentsTotalRow = await env.DB.prepare(`
    SELECT COALESCE(SUM(amount),0) AS total
    FROM ref_payments
    WHERE partner_id = ?
  `).bind(id).first();

  const clicks = Number(clicksRow?.c || 0);
  const leads = Number(leadsRow?.c || 0);
  const purchases = Number(purchasesRow?.c || 0);

  const earnings = purchases * price_c;
  const paymentsTotal = Number(paymentsTotalRow?.total || 0);
  const balance = earnings - paymentsTotal;

  return json({
    ok:true,
    partner:{
      id: partner.id,
      full_name: partner.full_name,
      phone: partner.phone,
      email: partner.email,
      ref_code: partner.ref_code,
      is_active: Number(partner.is_active) === 1
    },
    stats:{
      clicks,
      leads,
      purchases,
      earnings,
      payments: paymentsTotal,
      balance
    },
    payments: payments?.results || []
  },200);
}

    // =========================
    // API: CREATE 3 TOKENS (ADMIN)
    // =========================
    if (path === "/api/create" && request.method === "POST") {
      if (!(await adminOk(request))) return json({ ok: false, error: "Unauthorized" }, 401);

      const body = await safeJson(request);
      const email = (body.email || "").trim() || null;
      const phone = (body.phone || "").trim() || null;

      const formToken = await uniqueToken(env);
      const invToken = await uniqueToken(env);
      const rsvpToken = await uniqueToken(env);

      await env.DB.batch([
        env.DB.prepare(
          `INSERT INTO digital_tokens (token, kind, form_token, email, phone, disabled, paid)
           VALUES (?, 'form', ?, ?, ?, 0, 0)`
        ).bind(formToken, formToken, email, phone),

        env.DB.prepare(
          `INSERT INTO digital_tokens (token, kind, form_token, email, phone, disabled, paid)
           VALUES (?, 'invitation', ?, ?, ?, 0, 0)`
        ).bind(invToken, formToken, email, phone),

        env.DB.prepare(
          `INSERT INTO digital_tokens (token, kind, form_token, email, phone, disabled, paid)
           VALUES (?, 'rsvp', ?, ?, ?, 0, 0)`
        ).bind(rsvpToken, formToken, email, phone),
      ]);
// Δημιουργία αρχικού form record (ώστε να προ-γεμίζει η φόρμα)
await env.DB.prepare(
  `INSERT OR IGNORE INTO client_forms (token, email, phone, rsvp_enabled)
   VALUES (?, ?, ?, 1)`
).bind(formToken, email, phone).run();

return json({
  ok: true,
  tokens: { form: formToken, invitation: invToken, rsvp: rsvpToken },
urls: {
  form: `/form.html?t=${formToken}`,
  rsvp: `/rsvp.html?t=${rsvpToken}`,
  invitation: `/invitation.html?t=${invToken}`,
}
}, 200);
    }

// =========================
// ADMIN: PAID amount set
// body: { form_token, paid_amount }
// paid_amount: null = Not Paid, 0-500 = Paid
// =========================
if (path === "/api/admin/paid" && request.method === "POST") {
  if (!(await adminOk(request)))
    return json({ ok: false, error: "Unauthorized" }, 401);

  const body = await safeJson(request);
  const root = normalizeToken(body.form_token);

  let amount = body.paid_amount;

  if (!root)
    return json({ ok: false, error: "Missing form_token" }, 400);

  if (amount === "" || amount === "-" || amount == null) {
    amount = null;
  } else {
    amount = Number(amount);
    if (!Number.isFinite(amount) || amount < 0 || amount > 500)
      return json({ ok: false, error: "Amount must be 0-500 or null" }, 400);
  }

  const paidFlag = amount == null ? 0 : 1;

  await env.DB.prepare(
    `UPDATE digital_tokens
     SET paid = ?, paid_amount = ?
     WHERE form_token = ? OR token = ?`
  ).bind(paidFlag, amount, root, root).run();

  return json({
    ok: true,
    form_token: root,
    paid: paidFlag,
    paid_amount: amount
  }, 200);
}

// ======================================================
// ADMIN PRICES - GET (επιστροφή των 3 τιμών)
// ======================================================
if (path === "/api/admin/prices" && request.method === "GET") {
  if (!(await adminOk(request)))
    return json({ ok: false, error: "Unauthorized" }, 401);

  const row = await env.DB.prepare(
    `SELECT price_a, price_b, price_c
     FROM settings
     WHERE id=1`
  ).first();

  return json({
    ok: true,
    price_a: row?.price_a ?? 0,
    price_b: row?.price_b ?? 0,
    price_c: row?.price_c ?? 0
  }, 200);
}

// ======================================================
// ADMIN PRICES - UPDATE (αλλαγή τιμών)
// ======================================================
if (path === "/api/admin/prices" && request.method === "POST") {
  if (!(await adminOk(request)))
    return json({ ok: false, error: "Unauthorized" }, 401);

  const body = await safeJson(request);

  const a = Number(body.price_a);
  const b = Number(body.price_b);
  const c = Number(body.price_c);

  const okNum = (x) => Number.isFinite(x) && x >= 0 && x <= 500;

  if (!okNum(a) || !okNum(b) || !okNum(c)) {
    return json({ ok: false, error: "Prices must be numbers 0-500" }, 400);
  }

  // εξασφάλιση ότι υπάρχει row id=1
  await env.DB.prepare(
    `INSERT OR IGNORE INTO settings (id, price_a, price_b, price_c)
     VALUES (1, 0, 0, 0)`
  ).run();

  await env.DB.prepare(
    `UPDATE settings
     SET price_a=?, price_b=?, price_c=?
     WHERE id=1`
  ).bind(a, b, c).run();

  return json({ ok: true, price_a: a, price_b: b, price_c: c }, 200);
}

    // =========================
    // API: Resolve token
    // =========================
if (path === "/api/resolve" && request.method === "GET") {
  const t = normalizeToken(url.searchParams.get("t"));
  if (!t) return json({ ok: false, error: "Missing token" }, 400);

  if (t.length !== 10) {
    return json({ ok:false, error:"Invalid token" }, 400);
  }

  if (!/^[a-z0-9]+$/.test(t)) {
    return json({ ok:false, error:"Invalid token format" }, 400);
  }

  const row = await getTokenRow(env, t);
  if (!row || Number(row.disabled) === 1) return json({ ok: false, error: "Invalid / disabled token" }, 403);
     
      return json({
        ok: true,
        token: row.token,
        kind: row.kind,
        form_token: row.form_token,
      }, 200);
    }

// =========================
// API: TOKEN META
// =========================

if (path === "/api/token/meta" && request.method === "GET") {
  const t = normalizeToken(url.searchParams.get("t"));

  if (!t) return json({ ok:false, error:"Missing t" }, 400);

  const row = await env.DB.prepare(
    `SELECT partner_ref
     FROM digital_tokens
     WHERE token = ? OR form_token = ?
     LIMIT 1`
  ).bind(t, t).first();

  return json({
    ok: true,
    partner_ref: String(row?.partner_ref || "").trim()
  }, 200);
}


    // =========================
    // API: siblings
    // =========================
if (path === "/api/siblings" && request.method === "GET") {
  const t = normalizeToken(url.searchParams.get("t"));
  if (!t) return json({ ok: false, error: "Missing token" }, 400);

  if (t.length !== 10) {
    return json({ ok:false, error:"Invalid token" }, 400);
  }

  if (!/^[a-z0-9]+$/.test(t)) {
    return json({ ok:false, error:"Invalid token format" }, 400);
  }

  const tok = await getTokenRow(env, t);
  if (!tok || Number(tok.disabled) === 1) return json({ ok: false, error: "Invalid / disabled token" }, 403);
      const root = tok.form_token || tok.token;

      const rows = await env.DB.prepare(
        `SELECT token, kind
         FROM digital_tokens
         WHERE form_token = ? OR token = ?`
      ).bind(root, root).all();

      const out = { form: null, invitation: null, rsvp: null };
      for (const r of (rows?.results || [])) {
        if (r.kind === "form") out.form = r.token;
        if (r.kind === "invitation") out.invitation = r.token;
        if (r.kind === "rsvp") out.rsvp = r.token;
      }

      if (!out.form || !out.invitation || !out.rsvp) {
        return json({ ok: false, error: "Missing sibling tokens" }, 500);
      }

      return json({
        ok: true,
        tokens: out,
        urls: {
          form: `${BASE_URL}/form.html?t=${out.form}`,
          invitation: `${BASE_URL}/invitation.html?t=${out.invitation}`,
          rsvp: `${BASE_URL}/rsvp.html?t=${out.rsvp}`,
        }
      }, 200);
    }

    // =========================
    // API: Load form (FORM token only)
    // =========================
if (path === "/api/form" && request.method === "GET") {
  const t = normalizeToken(url.searchParams.get("t"));

  if (!t) return json({ ok: false, error: "Missing token" }, 400);

if (t.length !== 10) {
  return json({ ok:false, error:"Invalid token" }, 400);
}
  if (!/^[a-z0-9]+$/.test(t)) {
    return json({ ok:false, error:"Invalid token format" }, 400);
  }

 const tok = await getTokenRow(env, t);
if (!tok || Number(tok.disabled) === 1) {
  return json({ ok: false, error: "Invalid / disabled token" }, 403);
}

if (tok.kind !== "form") {
  return json({ ok: false, error: "Forbidden" }, 403);
}

const readOnly = false;
const root = tok.form_token || tok.token;

  const row = await env.DB.prepare(
    `SELECT
      id, token,
      groom_first, bride_first,
      groom_parents, bride_parents,
      koumbaroi_type, koumbaros_1, koumbaros_2, koumbaros_3,
      intro_choice, intro_custom,
      email, phone,
      wedding_date, wedding_time,
      venue, venue_maps_url,
      reception_venue, reception_maps_url,
      iban,
      countdown_enabled,
      rsvp_deadline, rsvp_enabled,
      notes,
      created_at, updated_at
     FROM client_forms
     WHERE token = ?
     ORDER BY id DESC
     LIMIT 1`
  ).bind(root).first();

  const paid_amount = await getPaidForRoot(env, root);

  const prefRow = await env.DB.prepare(
    `SELECT MAX(partner_ref) AS partner_ref
     FROM digital_tokens
     WHERE form_token = ? OR token = ?`
  ).bind(root, root).first();

  const partner_ref = String(prefRow?.partner_ref || "").trim();

  return json({
    ok: true,
    kind: tok.kind,
    form_token: root,
    readOnly,
    paid_amount,
    paid: paid_amount != null,
    partner_ref,
    form: row || null,
  }, 200);
}

    // =========================
    // API: Save form (FORM token only)
    // =========================
    if (path === "/api/form" && request.method === "POST") {
      const body = await safeJson(request);
      const t = normalizeToken(body.t);
      if (!t) return json({ ok: false, error: "Missing token" }, 400);

      const tok = await getTokenRow(env, t);
      if (!tok || Number(tok.disabled) === 1) {
        return json({ ok: false, error: "Invalid / disabled token" }, 403);
      }
      if (tok.kind !== "form") {
        return json({ ok: false, error: "Read-only token" }, 403);
      }

      const root = tok.form_token || tok.token;

      const groom_first = (body.groom_first || "").trim() || null;
      const bride_first = (body.bride_first || "").trim() || null;

      const groom_parents = (body.groom_parents || "").trim() || null;
      const bride_parents = (body.bride_parents || "").trim() || null;

      const koumbaroi_type = (body.koumbaroi_type || "").trim() || "koumbaros";
      const koumbaros_1 = (body.koumbaros_1 || "").trim() || null;
      const koumbaros_2 = (body.koumbaros_2 || "").trim() || null;
      const koumbaros_3 = (body.koumbaros_3 || "").trim() || null;

      const intro_choice = (body.intro_choice || "").trim() || "t1";
      const intro_custom = (body.intro_custom || "").trim() || null;

      const email = (body.email || tok.email || "").trim() || null;
      const phone = (body.phone || tok.phone || "").trim() || null;

      const wedding_date = (body.wedding_date || "").trim() || null;
      const wedding_time = (body.wedding_time || "").trim() || null;

      const venue = (body.venue || "").trim() || null;
      const venue_maps_url = (body.venue_maps_url || "").trim() || null;

      const reception_venue = (body.reception_venue || "").trim() || null;
      const reception_maps_url = (body.reception_maps_url || "").trim() || null;

      const ibanRaw = (body.iban || "").trim() || null;
      const notes = (body.notes || "").trim() || null;

      let rsvp_deadline = (body.rsvp_deadline || "").trim() || null;
      let countdown_enabled = body.countdown_enabled != null ? Number(body.countdown_enabled) : null;
      let rsvp_enabled = body.rsvp_enabled != null ? Number(body.rsvp_enabled) : null;

      // auto deadline if empty (7 μέρες πριν). Δεν κλείνει RSVP, μόνο εμφανίζει “έως …”
      if (!rsvp_deadline && wedding_date) {
        const p = parseYYYYMMDD(wedding_date);
        if (p) {
          const eventUTC = toDateUTC(p);
          rsvp_deadline = isoDateUTC(addDaysUTC(eventUTC, -7));
        }
      }
      if (countdown_enabled == null) countdown_enabled = 1;
      if (![0, 1].includes(countdown_enabled)) countdown_enabled = 1;
      if (rsvp_enabled == null) rsvp_enabled = 1;

      const existing = await env.DB.prepare(
        `SELECT id FROM client_forms WHERE token = ? ORDER BY id DESC LIMIT 1`
      ).bind(root).first();

      if (existing?.id) {
        await env.DB.prepare(
          `UPDATE client_forms
           SET
             groom_first=?, bride_first=?,
             groom_parents=?, bride_parents=?,
             koumbaroi_type=?, koumbaros_1=?, koumbaros_2=?, koumbaros_3=?,
             intro_choice=?, intro_custom=?,
             email=?, phone=?,
             wedding_date=?, wedding_time=?,
             venue=?, venue_maps_url=?,
             reception_venue=?, reception_maps_url=?,
             iban=?,
             countdown_enabled=?,
             rsvp_deadline=?, rsvp_enabled=?,
             notes=?,
             updated_at=CURRENT_TIMESTAMP
           WHERE id=?`
        ).bind(
          groom_first, bride_first,
          groom_parents, bride_parents,
          koumbaroi_type, koumbaros_1, koumbaros_2, koumbaros_3,
          intro_choice, intro_custom,
          email, phone,
          wedding_date, wedding_time,
          venue, venue_maps_url,
          reception_venue, reception_maps_url,
          ibanRaw,
          countdown_enabled,
          rsvp_deadline, rsvp_enabled,
          notes,
          existing.id
        ).run();
      } else {
        await env.DB.prepare(
          `INSERT INTO client_forms
            (token,
             groom_first, bride_first,
             groom_parents, bride_parents,
             koumbaroi_type, koumbaros_1, koumbaros_2, koumbaros_3,
             intro_choice, intro_custom,
             email, phone,
             wedding_date, wedding_time,
             venue, venue_maps_url,
             reception_venue, reception_maps_url,
             iban,
             countdown_enabled,
             rsvp_deadline, rsvp_enabled,
             notes)
           VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        ).bind(
          root,
          groom_first, bride_first,
          groom_parents, bride_parents,
          koumbaroi_type, koumbaros_1, koumbaros_2, koumbaros_3,
          intro_choice, intro_custom,
          email, phone,
          wedding_date, wedding_time,
          venue, venue_maps_url,
          reception_venue, reception_maps_url,
          ibanRaw,
          countdown_enabled,
          rsvp_deadline, rsvp_enabled,
          notes
        ).run();
      }

      await env.DB.prepare(
        `UPDATE digital_tokens SET email = ?, phone = ? WHERE form_token = ? OR token = ?`
      ).bind(email, phone, root, root).run();

      return json({ ok: true }, 200);
    }

    // =========================
    // API: Invitation view model
    // =========================
if (path === "/api/invitation" && request.method === "GET") {
  const t = normalizeToken(url.searchParams.get("t"));
  if (!t) return json({ ok: false, error: "Missing token" }, 400);

  if (t.length !== 10) {
    return json({ ok:false, error:"Invalid token" }, 400);
  }

  if (!/^[a-z0-9]+$/.test(t)) {
    return json({ ok:false, error:"Invalid token format" }, 400);
  }

      const tok = await getTokenRow(env, t);
      if (!tok || Number(tok.disabled) === 1) return json({ ok: false, error: "Invalid / disabled token" }, 403);
      if (tok.kind !== "invitation") return json({ ok: false, error: "Wrong token kind" }, 403);

const previewMode = url.searchParams.get("preview") === "1";
const paid_amount = await getPaidForRoot(env, tok.form_token || tok.token);
const isPaid = paid_amount != null;

if (!isPaid && !previewMode) {
  return json({ ok: false, error: "Payment required" }, 403);
}
      const form = await env.DB.prepare(
        `SELECT
          groom_first, bride_first,
          groom_parents, bride_parents,
          koumbaroi_type, koumbaros_1, koumbaros_2, koumbaros_3,
          intro_choice, intro_custom,
          wedding_date, wedding_time,
          venue, venue_maps_url,
          reception_venue, reception_maps_url,
          iban,
          countdown_enabled,
          rsvp_deadline, rsvp_enabled
         FROM client_forms
         WHERE token = ?
         ORDER BY id DESC
         LIMIT 1`
      ).bind(tok.form_token).first();

      if (!form) return json({ ok: false, error: "No form data yet" }, 404);
      if (isExpiredInvitation(form.wedding_date)) return json({ ok: false, error: "Expired" }, 410);

// -------- NEW CODE (siblings tokens) --------
const sibRows = await env.DB.prepare(
  `SELECT token, kind
   FROM digital_tokens
   WHERE form_token = ? OR token = ?`
).bind(tok.form_token, tok.form_token).all();

const tokens = { form:null, invitation:null, rsvp:null };

for (const r of (sibRows?.results || [])) {
  if (r.kind === "form") tokens.form = r.token;
  if (r.kind === "invitation") tokens.invitation = r.token;
  if (r.kind === "rsvp") tokens.rsvp = r.token;
}
// -------- END NEW CODE --------
      // RSVP δεν “κλείνει” ποτέ από deadline. Μόνο από checkbox.
      const enabled = Number(form.rsvp_enabled ?? 1) === 1;

      // deadline μόνο για εμφάνιση (“έως …”) και μόνο αν είναι στο μέλλον + enabled
      const deadlineRaw = form.rsvp_deadline || null;
      const deadlineForDisplay =
        (enabled && deadlineRaw && !isPastDeadline(deadlineRaw)) ? deadlineRaw : null;

      const groom_name = cleanName(form.groom_first);
      const bride_name = cleanName(form.bride_first);

      return json({
        ok: true,
        form_token: tok.form_token,
  		tokens,
        couple: { bride_name, groom_name },

        families: {
          groom_parents: form.groom_parents || null,
          bride_parents: form.bride_parents || null,
        },

        koumbaroi: {
          type: form.koumbaroi_type || "koumbaros",
          k1: form.koumbaros_1 || null,
          k2: form.koumbaros_2 || null,
          k3: form.koumbaros_3 || null,
        },

        intro: {
          choice: form.intro_choice || "t1",
          custom: form.intro_custom || null,
        },
        wedding_date: form.wedding_date || null,
        wedding_time: form.wedding_time || null,
        venue: form.venue || null,
        venue_maps_url: form.venue_maps_url || null,
        reception_venue: form.reception_venue || null,
        reception_maps_url: form.reception_maps_url || null,
        iban: form.iban || null,
        countdown_enabled: Number(form.countdown_enabled ?? 1) === 1,
        rsvp: {
          enabled,
          deadline: deadlineForDisplay, // null αν έχει περάσει → δεν θα δείχνει “έως …”
          allowed: enabled              // εμφανίζεται/κρύβεται μόνο από checkbox
        }
      }, 200);
    }

    // =========================
    // API: Submit RSVP
    // =========================
    if (path === "/api/rsvp/submit" && request.method === "POST") {
      const body = await safeJson(request);
      const t = normalizeToken(body.t);
      if (!t) return json({ ok: false, error: "Missing token" }, 400);

      const tok = await getTokenRow(env, t);
      if (!tok || Number(tok.disabled) === 1) return json({ ok: false, error: "Invalid / disabled token" }, 403);
      if (tok.kind !== "invitation") return json({ ok: false, error: "Wrong token kind" }, 403);

      const form = await env.DB.prepare(
        `SELECT wedding_date, rsvp_enabled
         FROM client_forms
         WHERE token = ?
         ORDER BY id DESC
         LIMIT 1`
      ).bind(tok.form_token).first();

      if (!form) return json({ ok: false, error: "No form data yet" }, 404);
      if (isExpiredInvitation(form.wedding_date)) return json({ ok: false, error: "Expired" }, 410);

      // RSVP κλείνει ΜΟΝΟ αν το απενεργοποιήσει το ζευγάρι.
      const enabled = Number(form.rsvp_enabled ?? 1) === 1;
      if (!enabled) return json({ ok: false, error: "RSVP is closed" }, 403);

const name = (body.name || "").trim();
const attending = (body.attending || "").trim();
let party_size = body.party_size != null ? Number(body.party_size) : (attending === "no" ? 0 : 1);
const note = (body.note || "").trim() || null;

if (!name) return json({ ok: false, error: "Missing name" }, 400);
if (!["yes", "no"].includes(attending)) return json({ ok: false, error: "Invalid attending" }, 400);

// Όχι → 0 άτομα
if (attending === "no") {
  party_size = 0;
} else {
  if (!Number.isFinite(party_size) || party_size < 1 || party_size > 50) {
    return json({ ok: false, error: "Invalid party_size" }, 400);
  }
}

await env.DB.prepare(
  `INSERT INTO rsvps (token, name, attending, party_size, note)
   VALUES (?, ?, ?, ?, ?)`
).bind(tok.form_token, name, attending, party_size, note).run();

return json({ ok: true }, 200);
}

    // =========================
    // API: RSVP LIST
    // =========================
    if (path === "/api/rsvp/list" && request.method === "GET") {
      const t = normalizeToken(url.searchParams.get("t"));
      if (!t) return json({ ok: false, error: "Missing token" }, 400);

      const tok = await getTokenRow(env, t);
      if (!tok || Number(tok.disabled) === 1) return json({ ok: false, error: "Invalid / disabled token" }, 403);
      if (tok.kind !== "rsvp") return json({ ok: false, error: "Wrong token kind" }, 403);

      const form = await env.DB.prepare(
        `SELECT
          groom_first, bride_first,
          email, wedding_date, rsvp_deadline, rsvp_enabled
         FROM client_forms
         WHERE token = ?
         ORDER BY id DESC
         LIMIT 1`
      ).bind(tok.form_token).first();

      const rows = await env.DB.prepare(
        `SELECT id, name, attending, party_size, note, created_at
         FROM rsvps
         WHERE token = ?
         ORDER BY id DESC`
      ).bind(tok.form_token).all();

      const groom_name = cleanName(form?.groom_first);
      const bride_name = cleanName(form?.bride_first);

      return json({
        ok: true,
        form_token: tok.form_token,
        couple: { bride_name, groom_name },
        email: form?.email || tok.email || null,
        wedding_date: form?.wedding_date || null,
        rsvp_deadline: form?.rsvp_deadline || null,
        rsvp_enabled: Number(form?.rsvp_enabled ?? 1) === 1,
        rsvps: rows?.results || [],
      }, 200);
    }

    // =========================
    // API: RSVP toggle (from RSVP page)
    // =========================
    if (path === "/api/rsvp/toggle" && request.method === "POST") {
      const t = normalizeToken(url.searchParams.get("t"));
      if (!t) return json({ ok: false, error: "Missing token" }, 400);

      const tok = await getTokenRow(env, t);
      if (!tok || Number(tok.disabled) === 1) return json({ ok: false, error: "Invalid / disabled token" }, 403);
      if (tok.kind !== "rsvp") return json({ ok: false, error: "Wrong token kind" }, 403);

      const body = await safeJson(request);
      const enabled = body.enabled != null ? Number(body.enabled) : null;
      if (enabled == null || ![0, 1].includes(enabled)) {
        return json({ ok: false, error: "enabled must be 0 or 1" }, 400);
      }

      const existing = await env.DB.prepare(
        `SELECT id FROM client_forms WHERE token = ? ORDER BY id DESC LIMIT 1`
      ).bind(tok.form_token).first();

      if (!existing?.id) return json({ ok: false, error: "No form data yet" }, 404);

      await env.DB.prepare(
        `UPDATE client_forms SET rsvp_enabled = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`
      ).bind(enabled, existing.id).run();

      return json({ ok: true, rsvp_enabled: enabled }, 200);
    }

    // =========================
    // API: RSVP CSV
    // =========================
    if (path === "/api/rsvp/csv" && request.method === "GET") {
      const t = normalizeToken(url.searchParams.get("t"));
      if (!t) return new Response("Missing token", { status: 400, headers: corsHeaders() });

      const tok = await getTokenRow(env, t);
      if (!tok || Number(tok.disabled) === 1 || tok.kind !== "rsvp") {
        return new Response("Forbidden", { status: 403, headers: corsHeaders() });
      }

      const rows = await env.DB.prepare(
        `SELECT name, attending, party_size, note, created_at
         FROM rsvps
         WHERE token = ?
         ORDER BY id ASC`
      ).bind(tok.form_token).all();

function fmtDMY(iso){
  if (!iso) return "";
  const m = String(iso).match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return iso;
  return `${Number(m[3])}/${Number(m[2])}/${m[1]}`; // 2/3/2026
}

function attendingGR(v){
  return String(v || "").toLowerCase() === "yes" ? "Ναι" : "Όχι";
}

      const header = ["Όνομα", "Παρουσία", "Άτομα", "Σημείωση", "Ημερομηνία"];
      const lines = [header.join(",")];

for (const r of rows?.results || []) {
  lines.push([
    csvCell(r.name),
    csvCell(attendingGR(r.attending)),
    csvCell(String(r.attending || "").toLowerCase() === "yes" ? (r.party_size ?? 1) : 0),
    csvCell(r.note || ""),
    csvCell(fmtDMY(r.created_at)),
  ].join(","));
}

      const csv = "\ufeff" + lines.join("\n");
      return new Response(csv, {
        status: 200,
        headers: {
          ...corsHeaders(),
          "content-type": "text/csv; charset=utf-8",
          "content-disposition": `attachment; filename="rsvp.csv"`,
        },
      });
    }

    // =========================
    // ADMIN: LIST couples
    // =========================
if (path === "/api/admin/list" && request.method === "GET") {
  if (!(await adminOk(request)))
    return json({ ok: false, error: "Unauthorized" }, 401);
    
    const leadsCountRow = await env.DB.prepare(`
  SELECT COUNT(*) AS c
  FROM leads
  WHERE status = 'new'
`).first();

const eventsCountRow = await env.DB.prepare(`
  SELECT COUNT(DISTINCT form_token) AS c
  FROM digital_tokens
`).first();

const paidCountRow = await env.DB.prepare(`
  SELECT COUNT(*) AS c
  FROM digital_tokens
  WHERE kind = 'form'
    AND paid_amount > 0
`).first();

const partnerLeadsCountRow = await env.DB.prepare(`
  SELECT COUNT(DISTINCT form_token) AS c
  FROM digital_tokens
  WHERE paid = 1
    AND partner_ref IS NOT NULL
    AND TRIM(partner_ref) <> ''
`).first();

const archivedPaidCountRow = await env.DB.prepare(`
  SELECT COUNT(*) AS c
  FROM archive
  WHERE paid_amount IS NOT NULL
    AND paid_amount > 0
`).first();

  // =========================
  // 1️⃣ LEADS (new only)
  // =========================
  const leadRows = await env.DB.prepare(`
    SELECT id, groom_first, bride_first, email, phone, wedding_date, created_at, ref
    FROM leads
    WHERE status = 'new'
    ORDER BY created_at DESC
  `).all();

  const leadItems = (leadRows?.results || []).map(r => ({
    kind: "lead",
    lead_id: r.id,
    partner_ref: r.ref || null,
    form_token: null,
    wedding_date: r.wedding_date || null,
    email: r.email || null,
    phone: r.phone || null,
    couple: {
      groom_name: r.groom_first || "",
      bride_name: r.bride_first || ""
    },
    tokens: null,
    urls: null,
    paid: false,
    paid_amount: null
  }));

  // =========================
  // 2️⃣ EVENTS (όπως πριν)
  // =========================
  const rows = await env.DB.prepare(`
    WITH latest_forms AS (
      SELECT cf.*
      FROM client_forms cf
      JOIN (
        SELECT token, MAX(id) AS max_id
        FROM client_forms
        GROUP BY token
      ) x ON x.token = cf.token AND x.max_id = cf.id
    )
    SELECT
      dt.form_token,
      MAX(dt.email)    AS tok_email,
      MAX(dt.phone)    AS tok_phone,
      MAX(dt.disabled) AS disabled,
      MAX(dt.paid)     AS paid,
      MAX(dt.paid_amount) AS paid_amount,
      MAX(dt.partner_ref) AS partner_ref,

      MAX(CASE WHEN dt.kind='form' THEN dt.token END)       AS form_token_link,
      MAX(CASE WHEN dt.kind='invitation' THEN dt.token END) AS invitation_token_link,
      MAX(CASE WHEN dt.kind='rsvp' THEN dt.token END)       AS rsvp_token_link,

      lf.groom_first,
      lf.bride_first,
      lf.email AS form_email,
      lf.phone AS form_phone,
      lf.wedding_date,
      lf.created_at

    FROM digital_tokens dt
    LEFT JOIN latest_forms lf
      ON lf.token = dt.form_token
    GROUP BY dt.form_token
    ORDER BY
      (MAX(lf.wedding_date) IS NULL) ASC,
      MAX(lf.wedding_date) DESC,
      MAX(lf.created_at) DESC
  `).all();

  const eventItems = (rows?.results || []).map(r => ({
    kind: "event",
    form_token: r.form_token,
    disabled: Number(r.disabled ?? 0) === 1,
    paid: Number(r.paid ?? 0) === 1,
    paid_amount: (r.paid_amount == null ? null : Number(r.paid_amount)),
    partner_ref: r.partner_ref || null,
    couple: {
      bride_name: r.bride_first || "",
      groom_name: r.groom_first || ""
    },

    wedding_date: r.wedding_date || null,
    email: (r.form_email || r.tok_email || null),
    phone: (r.form_phone || r.tok_phone || null),

    tokens: {
      form: r.form_token_link,
      invitation: r.invitation_token_link,
      rsvp: r.rsvp_token_link
    },

    urls: {
      form: r.form_token_link ? `/form.html?t=${r.form_token_link}` : null,
      invitation: r.invitation_token_link ? `/invitation.html?t=${r.invitation_token_link}` : null,
      rsvp: r.rsvp_token_link ? `/rsvp.html?t=${r.rsvp_token_link}` : null,
    }
  }));

  // =========================
  // Return combined list
  // =========================
return json({
  ok: true,
  stats: {
    leads: Number(leadsCountRow?.c || 0),
    events: Number(eventsCountRow?.c || 0),
    paid: Number(paidCountRow?.c || 0),
    partners: Number(partnerLeadsCountRow?.c || 0),
    archived_paid: Number(archivedPaidCountRow?.c || 0)
  },
  items: [...leadItems, ...eventItems]
}, 200);
}

// =========================
// ADMIN: ARCHIVE LIST (new table: archive)
// =========================
if (path === "/api/admin/archive/list" && request.method === "GET") {
  if (!(await adminOk(request))) return json({ ok: false, error: "Unauthorized" }, 401);

const rows = await env.DB.prepare(`
SELECT
  form_token,
  created_date,
  wedding_date,
  groom_first,
  bride_first,
  paid_amount,
  partner_ref
FROM archive
  ORDER BY wedding_date DESC
  LIMIT 500
`).all();

const items = (rows?.results || []).map(r => ({
  form_token: r.form_token,
  created_date: r.created_date || null,
  wedding_date: r.wedding_date || null,
  paid_amount: (r.paid_amount == null ? null : Number(r.paid_amount)),
  partner_ref: r.partner_ref || null,
  couple: {
    groom_name: r.groom_first || "",
    bride_name: r.bride_first || ""
  }
}));

  return json({ ok: true, items }, 200);
}
    
// =========================
// ADMIN: DELETE archive entry
// body: { form_token }
// =========================
if (path === "/api/admin/archive/delete" && request.method === "POST") {
  if (!(await adminOk(request)))
    return json({ ok: false, error: "Unauthorized" }, 401);

  const body = await safeJson(request);
  const root = normalizeToken(body.form_token);

  if (!root)
    return json({ ok: false, error: "Missing form_token" }, 400);

  await env.DB.prepare(
    `DELETE FROM archive WHERE form_token = ?`
  ).bind(root).run();

  return json({ ok: true }, 200);
}

 // =========================
// ADMIN: DELETE event (archive|hard)
// body: { form_token, mode }
// =========================
if (path === "/api/admin/delete" && request.method === "POST") {
  if (!(await adminOk(request))) return json({ ok: false, error: "Unauthorized" }, 401);

  const body = await safeJson(request);
  const root = normalizeToken(body.form_token);
  const mode = String(body.mode || "").trim().toLowerCase();

  if (!root) return json({ ok: false, error: "Missing form_token" }, 400);
  if (!["archive", "hard"].includes(mode)) return json({ ok: false, error: "mode must be archive or hard" }, 400);

  const exists = await env.DB.prepare(
    `SELECT token FROM digital_tokens WHERE form_token = ? OR token = ? LIMIT 1`
  ).bind(root, root).first();

  if (!exists) return json({ ok: false, error: "Event not found" }, 404);

  if (mode === "archive") {

    const f = await env.DB.prepare(
      `SELECT groom_first, bride_first, wedding_date, created_at
       FROM client_forms
       WHERE token = ?
       ORDER BY id DESC
       LIMIT 1`
    ).bind(root).first();

    const created_date =
      f?.created_at
        ? String(f.created_at).slice(0, 10)
        : new Date().toISOString().slice(0, 10);

    // paid amount
    const paidRow = await env.DB.prepare(
      `SELECT MAX(paid_amount) AS paid_amount
       FROM digital_tokens
       WHERE form_token = ? OR token = ?`
    ).bind(root, root).first();

    const paid_amount = paidRow?.paid_amount ?? null;

    // partner ref
    const prefRow = await env.DB.prepare(
      `SELECT MAX(partner_ref) AS partner_ref
       FROM digital_tokens
       WHERE form_token = ? OR token = ?`
    ).bind(root, root).first();

    const partner_ref = prefRow?.partner_ref ?? null;

    await env.DB.prepare(
      `INSERT INTO archive
       (form_token, created_date, wedding_date, groom_first, bride_first, paid_amount, partner_ref)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    ).bind(
      root,
      created_date,
      f?.wedding_date || null,
      f?.groom_first || null,
      f?.bride_first || null,
      paid_amount,
      partner_ref
    ).run();

    await env.DB.batch([
      env.DB.prepare(`DELETE FROM rsvps WHERE token = ?`).bind(root),
      env.DB.prepare(`DELETE FROM client_forms WHERE token = ?`).bind(root),
      env.DB.prepare(`DELETE FROM digital_tokens WHERE form_token = ? OR token = ?`).bind(root, root),
    ]);

    return json({ ok: true, mode: "archive" }, 200);
  }

  await env.DB.batch([
    env.DB.prepare(`DELETE FROM rsvps WHERE token = ?`).bind(root),
    env.DB.prepare(`DELETE FROM client_forms WHERE token = ?`).bind(root),
    env.DB.prepare(`DELETE FROM digital_tokens WHERE form_token = ? OR token = ?`).bind(root, root),
  ]);

  return json({ ok: true, mode: "hard" }, 200);
}

// =========================
// ADMIN: DELETE LEAD
// body: { lead_id }
// =========================
if (path === "/api/admin/leads/delete" && request.method === "POST") {
  if (!(await adminOk(request)))
    return json({ ok: false, error: "Unauthorized" }, 401);

  const body = await safeJson(request);
  const lead_id = Number(body.lead_id);

  if (!Number.isInteger(lead_id) || lead_id < 1) {
    return json({ ok: false, error: "Invalid lead_id" }, 400);
  }

  await env.DB.prepare(
    `UPDATE leads SET status='deleted' WHERE id=?`
  ).bind(lead_id).run();

  return json({ ok: true }, 200);
}
// =========================
// ADMIN: CONVERT LEAD -> CREATE TOKENS
// body: { lead_id }
// =========================
if (path === "/api/admin/leads/convert" && request.method === "POST") {
  if (!(await adminOk(request)))
    return json({ ok: false, error: "Unauthorized" }, 401);

  const body = await safeJson(request);
  const lead_id = Number(body.lead_id);

  if (!Number.isInteger(lead_id) || lead_id < 1) {
    return json({ ok: false, error: "Invalid lead_id" }, 400);
  }

  const lead = await env.DB.prepare(
    `SELECT id, email, phone, wedding_date, status, ref
     FROM leads
     WHERE id = ?
     LIMIT 1`
  ).bind(lead_id).first();

  if (!lead) return json({ ok: false, error: "Lead not found" }, 404);
  if (String(lead.status) !== "new")
    return json({ ok: false, error: "Lead is not new" }, 400);

  const email = (lead.email || "").trim() || null;
  const phone = (lead.phone || "").trim() || null;

  const formToken = await uniqueToken(env);
  const invToken  = await uniqueToken(env);
  const rsvpToken = await uniqueToken(env);

  // partner_ref ΜΟΝΟ από lead.ref (DB), όχι από query params
  const pref = (lead.ref || "").trim() || null;

  await env.DB.batch([
    env.DB.prepare(
      `INSERT INTO digital_tokens
       (token, kind, form_token, email, phone, disabled, paid, paid_amount, partner_ref)
       VALUES (?, 'form', ?, ?, ?, 0, 0, NULL, ?)`
    ).bind(formToken, formToken, email, phone, pref),

    env.DB.prepare(
      `INSERT INTO digital_tokens
       (token, kind, form_token, email, phone, disabled, paid, paid_amount, partner_ref)
       VALUES (?, 'invitation', ?, ?, ?, 0, 0, NULL, ?)`
    ).bind(invToken, formToken, email, phone, pref),

    env.DB.prepare(
      `INSERT INTO digital_tokens
       (token, kind, form_token, email, phone, disabled, paid, paid_amount, partner_ref)
       VALUES (?, 'rsvp', ?, ?, ?, 0, 0, NULL, ?)`
    ).bind(rsvpToken, formToken, email, phone, pref),
  ]);

  await env.DB.prepare(
    `INSERT OR IGNORE INTO client_forms (token, email, phone, rsvp_enabled)
     VALUES (?, ?, ?, 1)`
  ).bind(formToken, email, phone).run();

await env.DB.prepare(
  `UPDATE client_forms
   SET wedding_date = COALESCE(NULLIF(?, ''), wedding_date),
       updated_at = CURRENT_TIMESTAMP
   WHERE token = ?`
).bind(
  String(lead.wedding_date || "").trim(),
  formToken
).run();

  await env.DB.prepare(
    `UPDATE leads
     SET status='converted', form_token=?
     WHERE id=?`
  ).bind(formToken, lead_id).run();

  // ΠΡΟΣΟΧΗ: ΔΕΝ επιστρέφουμε ποτέ ?p=... στα links
  return json({
    ok: true,
    form_token: formToken,
    tokens: { form: formToken, invitation: invToken, rsvp: rsvpToken },
    urls: {
      form: `/form.html?t=${formToken}`,
      rsvp: `/rsvp.html?t=${rsvpToken}`,
      invitation: `/invitation.html?t=${invToken}`,
    }
  }, 200);
}

async function getPendingLeadsCount(env) {
  const row = await env.DB.prepare(
    `SELECT COUNT(*) AS pending
     FROM leads
     WHERE status = 'new'`
  ).first();

  return Number(row?.pending || 0);
}

async function sendPushToAdmins(env, payload) {
  webpush.setVapidDetails(
    env.VAPID_CONTACT,
    env.VAPID_PUBLIC_KEY,
    env.VAPID_PRIVATE_KEY
  );

  const rows = await env.DB.prepare(
    `SELECT id, endpoint, p256dh, auth
     FROM push_subscriptions
     ORDER BY id DESC`
  ).all();

  const subs = rows?.results || [];

  console.log("PUSH_SUBS", subs.length);

  const tasks = subs.map((sub) =>
    webpush.sendNotification(
      {
        endpoint: sub.endpoint,
        keys: {
          p256dh: sub.p256dh,
          auth: sub.auth
        }
      },
      JSON.stringify(payload),
      { TTL: 60 }
    )
  );

  const results = await Promise.allSettled(tasks);

  let sent = 0;
  let failed = 0;

  for (let i = 0; i < results.length; i++) {

    const result = results[i];
    const sub = subs[i];

    if (result.status === "fulfilled") {
      sent++;
      continue;
    }

    failed++;

    const e = result.reason;
    const statusCode = Number(e?.statusCode || 0);

    console.log(
      "PUSH_SEND_ERROR",
      sub.id,
      statusCode,
      String(e?.message || e)
    );

    if (statusCode === 404 || statusCode === 410) {

      try {

        await env.DB.prepare(
          `DELETE FROM push_subscriptions WHERE id = ?`
        ).bind(sub.id).run();

        console.log("PUSH_SUB_DELETED", sub.id);

      } catch (delErr) {

        console.log(
          "PUSH_SUB_DELETE_FAILED",
          sub.id,
          String(delErr?.message || delErr)
        );

      }

    }

  }

  return { sent, failed };
}

// ---------- helpers ----------

function corsHeaders() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,authorization",
  };
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { ...corsHeaders(), "content-type": "application/json; charset=utf-8" },
  });
}

async function safeJson(request) {
  try {
    return await request.json();
  } catch {
    return {};
  }
}

function csvCell(v) {
  const s = String(v ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

    return new Response("Not Found", {
      status: 404,
      headers: corsHeaders(),
    });

    } catch (e) {
      console.log("WORKER_ERROR", String(e?.message || e));
      return json(
        { ok: false, error: String(e?.message || e || "Internal Error") },
        500
      );
    }
  }
};

