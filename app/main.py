
import os
import io
import csv
import sqlite3
import psycopg2
from psycopg2.extras import RealDictCursor
import hashlib
import secrets
import re

from datetime import date, datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, Form, Depends, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware


# =========================================================
# CONFIGURATION
# =========================================================

APP_SECRET = os.getenv(
    "APP_SECRET",
    "local-development-secret-change-before-deployment"
)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_PATH = os.getenv("DB_PATH", "./invoices.db")
USE_POSTGRES = bool(DATABASE_URL)

app = FastAPI(title="Recruitment Invoice Manager")

app.add_middleware(
    SessionMiddleware,
    secret_key=APP_SECRET,
    https_only=os.getenv("HTTPS_ONLY", "false").lower() == "true",
    same_site="lax"
)

app.mount(
    "/static",
    StaticFiles(directory="app/static"),
    name="static"
)

templates = Jinja2Templates(directory="app/templates")

# QR images are stored under app/static/uploads. On Render Free this local
# folder is ephemeral; use persistent object storage for permanent QR images.
UPLOAD_DIR = os.path.join("app", "static", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)
MAX_QR_BYTES = 3 * 1024 * 1024  # 3 MB


# =========================================================
# DATABASE
# =========================================================


class CompatCursor:
    """Small compatibility wrapper so the existing SQLite-style app can use PostgreSQL."""
    def __init__(self, cursor, lastrowid=None):
        self._cursor = cursor
        self.lastrowid = lastrowid

    def fetchone(self):
        row = self._cursor.fetchone()
        return dict(row) if row is not None and not isinstance(row, dict) else row

    def fetchall(self):
        rows = self._cursor.fetchall()
        return [dict(row) if not isinstance(row, dict) else row for row in rows]

    def __iter__(self):
        for row in self._cursor:
            yield dict(row) if not isinstance(row, dict) else row

    def rowcount(self):
        return self._cursor.rowcount

    @property
    def description(self):
        return self._cursor.description


class CompatConnection:
    def __init__(self, raw):
        self.raw = raw

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.raw.commit()
            else:
                self.raw.rollback()
        finally:
            self.raw.close()
        return False

    def commit(self):
        self.raw.commit()

    def rollback(self):
        self.raw.rollback()

    def close(self):
        self.raw.close()

    def execute(self, sql, params=()):
        # SQLite schema-inspection command translated to PostgreSQL metadata.
        pragma = re.match(r"\s*PRAGMA\s+table_info\((\w+)\)\s*;?\s*$", sql, re.I)
        if USE_POSTGRES and pragma:
            table = pragma.group(1)
            cur = self.raw.cursor(cursor_factory=RealDictCursor)
            cur.execute(
                """SELECT ordinal_position - 1 AS cid, column_name AS name,
                          data_type AS type, 0 AS notnull, column_default AS dflt_value,
                          CASE WHEN is_identity='YES' OR column_default LIKE 'nextval%%'
                               THEN 1 ELSE 0 END AS pk
                   FROM information_schema.columns
                   WHERE table_schema = current_schema() AND table_name = %s
                   ORDER BY ordinal_position""",
                (table,)
            )
            return CompatCursor(cur)

        if USE_POSTGRES:
            query = sql.replace("?", "%s")
            # Add RETURNING id for invoice inserts because the original code reads lastrowid.
            invoice_insert = bool(re.match(r"\s*INSERT\s+INTO\s+invoices\s*\(", query, re.I))
            if invoice_insert and "RETURNING" not in query.upper():
                query = query.rstrip().rstrip(";") + " RETURNING id"
            cur = self.raw.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, params or ())
            lastrowid = None
            if invoice_insert:
                returned = cur.fetchone()
                if returned:
                    lastrowid = returned.get("id")
            return CompatCursor(cur, lastrowid)

        cur = self.raw.execute(sql, params or ())
        return CompatCursor(cur, getattr(cur, "lastrowid", None))

    def executescript(self, script):
        if not USE_POSTGRES:
            return self.raw.executescript(script)
        cur = self.raw.cursor(cursor_factory=RealDictCursor)
        # The schema script contains simple CREATE TABLE statements only.
        statements = [part.strip() for part in script.split(";") if part.strip()]
        for statement in statements:
            statement = re.sub(
                r"\bINTEGER\s+PRIMARY\s+KEY\b",
                "SERIAL PRIMARY KEY",
                statement,
                flags=re.I
            )
            statement = statement.replace("REAL", "DOUBLE PRECISION")
            cur.execute(statement)
        return CompatCursor(cur)


def connect():
    if USE_POSTGRES:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is missing")
        url = DATABASE_URL
        # Neon requires SSL; append it if the connection URL has no sslmode.
        if "sslmode=" not in url:
            url += ("&" if "?" in url else "?") + "sslmode=require"
        raw = psycopg2.connect(url, connect_timeout=15)
        return CompatConnection(raw)

    db_directory = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(db_directory, exist_ok=True)
    raw = sqlite3.connect(DB_PATH, timeout=30)
    raw.row_factory = sqlite3.Row
    raw.execute("PRAGMA foreign_keys = ON")
    return CompatConnection(raw)


def hashpw(password):
    # Retained for compatibility with the existing starter project.
    # Replace with Argon2/bcrypt before production deployment.
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def init_db():
    with connect() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS sellers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                address TEXT,
                bank_account TEXT,
                bank_name TEXT,
                ifsc TEXT,
                pan TEXT,
                msme TEXT,
                email TEXT,
                phone TEXT,
                qr_code TEXT,
                active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY,
                invoice_no TEXT UNIQUE NOT NULL,
                invoice_date TEXT,
                created_by INTEGER,
                seller_id INTEGER,
                client_name TEXT,
                client_address TEXT,
                candidate_name TEXT,
                designation TEXT,
                doj TEXT,
                salary REAL,
                description TEXT,
                amount REAL,
                status TEXT DEFAULT 'Draft',
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(created_by) REFERENCES users(id),
                FOREIGN KEY(seller_id) REFERENCES sellers(id)
            );

            CREATE TABLE IF NOT EXISTS audit (
                id INTEGER PRIMARY KEY,
                invoice_id INTEGER,
                user_id INTEGER,
                action TEXT,
                at TEXT DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Migration for existing databases
        columns = {
            row["name"]
            for row in c.execute("PRAGMA table_info(users)").fetchall()
        }

        if "full_name" not in columns:
            c.execute("ALTER TABLE users ADD COLUMN full_name TEXT")

        seller_columns = {
            row["name"] for row in c.execute("PRAGMA table_info(sellers)").fetchall()
        }
        if "qr_code" not in seller_columns:
            c.execute("ALTER TABLE sellers ADD COLUMN qr_code TEXT")

        # Existing users initially use their username as full name.
        c.execute("""
            UPDATE users
            SET full_name = username
            WHERE full_name IS NULL OR TRIM(full_name) = ''
        """)

        # Create initial Admin only if no user exists.
        if not c.execute(
            "SELECT id FROM users LIMIT 1"
        ).fetchone():

            admin_username = os.getenv("ADMIN_USERNAME", "admin")
            admin_password = os.getenv(
                "ADMIN_PASSWORD",
                "ChangeMe123!"
            )

            c.execute("""
                INSERT INTO users
                    (username, full_name, password_hash, role)
                VALUES (?, ?, ?, ?)
            """, (
                admin_username,
                admin_username,
                hashpw(admin_password),
                "admin"
            ))

        c.commit()


@app.on_event("startup")
def startup():
    init_db()


# =========================================================
# AUTHENTICATION
# =========================================================

def current_user(request: Request):
    user_id = request.session.get("uid")

    if not user_id:
        return None

    with connect() as c:
        return c.execute("""
            SELECT * FROM users
            WHERE id = ? AND active = 1
        """, (user_id,)).fetchone()


def require_user(request: Request):
    user = current_user(request)

    if not user:
        raise HTTPException(
            status_code=401,
            detail="Please log in"
        )

    return user


def admin_only(user):
    if user["role"] != "admin":
        raise HTTPException(
            status_code=403,
            detail="Admin access required"
        )


# =========================================================
# AUTOMATIC INVOICE / VOUCHER NUMBER
# Format: D.S-20261021-12:00
# =========================================================

def generate_invoice_number(full_name, conn):
    now = datetime.now(ZoneInfo("Asia/Kolkata"))

    # Divyanshu Srivastava -> D.S
    words = re.findall(
        r"[A-Za-z0-9]+",
        (full_name or "").strip()
    )

    initials = ".".join(
        word[0].upper() for word in words
    )

    if not initials:
        initials = "U"

    # Example: D.S-20261021-12:00
    base = f"{initials}-{now.strftime('%Y%m%d-%H:%M')}"

    invoice_no = base
    counter = 2

    # Avoid duplicate invoice numbers within the same minute.
    while conn.execute(
        "SELECT 1 FROM invoices WHERE invoice_no = ?",
        (invoice_no,)
    ).fetchone():

        invoice_no = f"{base}-{counter:02d}"
        counter += 1

    return invoice_no


# =========================================================
# DASHBOARD
# =========================================================

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    user = current_user(request)

    if not user:
        return RedirectResponse("/login", status_code=302)

    with connect() as c:
        if user["role"] == "admin":

            invoices = c.execute("""
                SELECT
                    i.*,
                    s.name AS seller_name,
                    u.username,
                    u.full_name
                FROM invoices i
                LEFT JOIN sellers s ON s.id = i.seller_id
                LEFT JOIN users u ON u.id = i.created_by
                ORDER BY i.id DESC
            """).fetchall()

            sellers = c.execute("""
                SELECT * FROM sellers
                ORDER BY name
            """).fetchall()

            users = c.execute("""
                SELECT id, username, full_name, role, active
                FROM users
                ORDER BY id
            """).fetchall()

        else:

            invoices = c.execute("""
                SELECT
                    i.*,
                    s.name AS seller_name,
                    u.username,
                    u.full_name
                FROM invoices i
                LEFT JOIN sellers s ON s.id = i.seller_id
                LEFT JOIN users u ON u.id = i.created_by
                WHERE i.created_by = ?
                ORDER BY i.id DESC
            """, (user["id"],)).fetchall()

            sellers = c.execute("""
                SELECT * FROM sellers
                WHERE active = 1
                ORDER BY name
            """).fetchall()

            users = []

    return templates.TemplateResponse(request, "index.html", {
            "user": user,
            "invoices": invoices,
            "sellers": sellers,
            "users": users,
            "today": date.today().isoformat()
        
        })


# =========================================================
# LOGIN / LOGOUT
# =========================================================

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(request, "login.html", {
            "error": ""
        
        })


@app.post("/login")
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...)
):
    with connect() as c:
        user = c.execute("""
            SELECT * FROM users
            WHERE username = ? AND active = 1
        """, (username.strip(),)).fetchone()

    if not user or user["password_hash"] != hashpw(password):
        return templates.TemplateResponse(request, "login.html", {
            "error": "Invalid username or password"
            
        }, status_code=401)

    request.session.clear()
    request.session["uid"] = user["id"]

    return RedirectResponse("/", status_code=303)


@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


# =========================================================
# ADMIN: CREATE USER
# =========================================================

@app.post("/users")
def create_user(
    request: Request,
    full_name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form("user"),
    user=Depends(require_user)
):
    admin_only(user)

    full_name = full_name.strip()
    username = username.strip()

    if not full_name or not username:
        raise HTTPException(
            status_code=400,
            detail="Full name and username are required"
        )

    if len(password) < 8:
        raise HTTPException(
            status_code=400,
            detail="Password must be at least 8 characters"
        )

    if role not in ("user", "admin"):
        role = "user"

    try:
        with connect() as c:
            c.execute("""
                INSERT INTO users
                    (full_name, username, password_hash, role)
                VALUES (?, ?, ?, ?)
            """, (
                full_name,
                username,
                hashpw(password),
                role
            ))
            c.commit()

    except (sqlite3.IntegrityError, psycopg2.IntegrityError):
        raise HTTPException(
            status_code=400,
            detail="Username already exists"
        )

    return RedirectResponse("/", status_code=303)


# =========================================================
# ADMIN: CREATE SELLER PROFILE
# =========================================================

def save_qr_upload(qr_file: UploadFile | None) -> str | None:
    """Validate and save an optional QR image; return its static URL path."""
    if not qr_file or not qr_file.filename:
        return None
    allowed = {".png", ".jpg", ".jpeg", ".webp"}
    ext = os.path.splitext(qr_file.filename.lower())[1]
    if ext not in allowed:
        raise HTTPException(400, "QR code must be PNG, JPG, JPEG, or WEBP")
    content = qr_file.file.read(MAX_QR_BYTES + 1)
    if len(content) > MAX_QR_BYTES:
        raise HTTPException(400, "QR image must be 3 MB or smaller")
    if not content:
        return None
    # Check the image signature instead of trusting the extension alone.
    signatures = {
        ".png": content.startswith(b"\x89PNG\r\n\x1a\n"),
        ".jpg": content.startswith(b"\xff\xd8\xff"),
        ".jpeg": content.startswith(b"\xff\xd8\xff"),
        ".webp": len(content) >= 12 and content[:4] == b"RIFF" and content[8:12] == b"WEBP",
    }
    if not signatures.get(ext, False):
        raise HTTPException(400, "Uploaded file does not appear to be a valid image")
    filename = secrets.token_hex(16) + ext
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    with open(os.path.join(UPLOAD_DIR, filename), "wb") as f:
        f.write(content)
    return "/static/uploads/" + filename


@app.post("/users/{user_id}/delete")
def delete_user(user_id: int, user=Depends(require_user)):
    """Admin-only user deletion. Protect current/last admin and invoice history."""
    admin_only(user)
    if int(user["id"]) == user_id:
        raise HTTPException(status_code=400, detail="You cannot delete your own account while signed in")

    with connect() as c:
        target = c.execute("SELECT id, username, role FROM users WHERE id = ?", (user_id,)).fetchone()
        if not target:
            raise HTTPException(status_code=404, detail="User not found")

        if target["role"] == "admin":
            admins = c.execute("SELECT COUNT(*) AS n FROM users WHERE role = 'admin' AND active = 1").fetchone()
            if admins and admins["n"] <= 1:
                raise HTTPException(status_code=400, detail="Cannot delete the last active admin")

        linked = c.execute("SELECT COUNT(*) AS n FROM invoices WHERE created_by = ?", (user_id,)).fetchone()
        if linked and linked["n"] > 0:
            raise HTTPException(
                status_code=409,
                detail="This user has invoices. Delete or reassign those invoices first; historical invoice records are protected."
            )

        c.execute("DELETE FROM users WHERE id = ?", (user_id,))
        c.commit()

    return RedirectResponse("/", status_code=303)


@app.post("/sellers")
def create_seller(
    request: Request,
    name: str = Form(...),
    address: str = Form(""),
    bank_account: str = Form(""),
    bank_name: str = Form(""),
    ifsc: str = Form(""),
    pan: str = Form(""),
    msme: str = Form(""),
    email: str = Form(""),
    phone: str = Form(""),
    qr_file: UploadFile | None = File(None),
    user=Depends(require_user)
):
    admin_only(user)
    qr_code = save_qr_upload(qr_file)
    with connect() as c:
        c.execute("""
            INSERT INTO sellers
                (name, address, bank_account, bank_name, ifsc, pan, msme, email, phone, qr_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name.strip(), address, bank_account, bank_name, ifsc,
            pan, msme, email, phone, qr_code
        ))
        c.commit()
    return RedirectResponse("/", status_code=303)


@app.post("/sellers/{seller_id}/edit")
def edit_seller(
    seller_id: int,
    request: Request,
    name: str = Form(...),
    address: str = Form(""),
    bank_account: str = Form(""),
    bank_name: str = Form(""),
    ifsc: str = Form(""),
    pan: str = Form(""),
    msme: str = Form(""),
    email: str = Form(""),
    phone: str = Form(""),
    qr_file: UploadFile | None = File(None),
    user=Depends(require_user)
):
    admin_only(user)
    with connect() as c:
        existing = c.execute("SELECT * FROM sellers WHERE id = ?", (seller_id,)).fetchone()
        if not existing:
            raise HTTPException(404, "Seller profile not found")
        qr_code = save_qr_upload(qr_file) or existing["qr_code"]
        c.execute("""
            UPDATE sellers SET name=?, address=?, bank_account=?, bank_name=?,
                ifsc=?, pan=?, msme=?, email=?, phone=?, qr_code=?
            WHERE id=?
        """, (name.strip(), address, bank_account, bank_name, ifsc, pan,
              msme, email, phone, qr_code, seller_id))
        c.commit()
    return RedirectResponse("/#seller-profiles", status_code=303)


@app.post("/sellers/{seller_id}/delete")
def delete_seller(
    seller_id: int,
    request: Request,
    user=Depends(require_user)
):
    admin_only(user)
    with connect() as c:
        seller = c.execute("SELECT id FROM sellers WHERE id = ?", (seller_id,)).fetchone()
        if not seller:
            raise HTTPException(404, "Seller profile not found")
        used = c.execute("SELECT 1 FROM invoices WHERE seller_id = ? LIMIT 1", (seller_id,)).fetchone()
        if used:
            # Keep historical invoice references intact; hide this seller from new invoices.
            c.execute("UPDATE sellers SET active = 0 WHERE id = ?", (seller_id,))
        else:
            c.execute("DELETE FROM sellers WHERE id = ?", (seller_id,))
        c.commit()
    return RedirectResponse("/#seller-profiles", status_code=303)


# =========================================================
# CREATE INVOICE
# =========================================================

@app.post("/invoices")
def create_invoice(
    request: Request,
    invoice_no: str = Form(""),
    invoice_date: str = Form(...),
    seller_id: int = Form(...),
    client_name: str = Form(...),
    client_address: str = Form(""),
    candidate_name: str = Form(...),
    designation: str = Form(""),
    doj: str = Form(""),
    salary: float = Form(0),
    description: str = Form("Recruitment / Manpower Supply"),
    amount: float = Form(...),
    user=Depends(require_user)
):
    if salary < 0 or amount < 0:
        raise HTTPException(
            status_code=400,
            detail="Salary and amount cannot be negative"
        )

    with connect() as c:

        seller = c.execute("""
            SELECT id FROM sellers
            WHERE id = ? AND active = 1
        """, (seller_id,)).fetchone()

        if not seller:
            raise HTTPException(
                status_code=400,
                detail="Select a valid seller"
            )

        # Auto-generate when invoice number is left blank.
        if not invoice_no.strip():
            full_name = (
                user["full_name"] or user["username"]
            )
            invoice_no = generate_invoice_number(
                full_name,
                c
            )
        else:
            invoice_no = invoice_no.strip()

        try:
            cursor = c.execute("""
                INSERT INTO invoices (
                    invoice_no,
                    invoice_date,
                    created_by,
                    seller_id,
                    client_name,
                    client_address,
                    candidate_name,
                    designation,
                    doj,
                    salary,
                    description,
                    amount
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                invoice_no,
                invoice_date,
                user["id"],
                seller_id,
                client_name,
                client_address,
                candidate_name,
                designation,
                doj,
                salary,
                description,
                amount
            ))

            invoice_id = cursor.lastrowid

            c.execute("""
                INSERT INTO audit
                    (invoice_id, user_id, action)
                VALUES (?, ?, ?)
            """, (
                invoice_id,
                user["id"],
                "created"
            ))

            c.commit()

        except (sqlite3.IntegrityError, psycopg2.IntegrityError):
            raise HTTPException(
                status_code=400,
                detail="Invoice number already exists"
            )

    return RedirectResponse("/", status_code=303)


# =========================================================
# EDIT INVOICE
# =========================================================

@app.post("/invoices/{iid}/edit")
def edit_invoice(
    iid: int,
    request: Request,
    invoice_date: str = Form(...),
    seller_id: int = Form(...),
    client_name: str = Form(...),
    client_address: str = Form(""),
    candidate_name: str = Form(...),
    designation: str = Form(""),
    doj: str = Form(""),
    salary: float = Form(0),
    description: str = Form(""),
    amount: float = Form(...),
    status: str = Form("Draft"),
    user=Depends(require_user)
):
    if salary < 0 or amount < 0:
        raise HTTPException(
            status_code=400,
            detail="Salary and amount cannot be negative"
        )

    if status not in ("Draft", "Issued", "Cancelled"):
        status = "Draft"

    with connect() as c:
        invoice = c.execute("""
            SELECT * FROM invoices WHERE id = ?
        """, (iid,)).fetchone()

        if not invoice:
            raise HTTPException(404, "Invoice not found")

        if (
            user["role"] != "admin"
            and invoice["created_by"] != user["id"]
        ):
            raise HTTPException(403, "Not allowed")

        seller = c.execute("""
            SELECT id FROM sellers
            WHERE id = ? AND active = 1
        """, (seller_id,)).fetchone()

        if not seller:
            raise HTTPException(400, "Invalid seller")

        c.execute("""
            UPDATE invoices
            SET invoice_date = ?,
                seller_id = ?,
                client_name = ?,
                client_address = ?,
                candidate_name = ?,
                designation = ?,
                doj = ?,
                salary = ?,
                description = ?,
                amount = ?,
                status = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (
            invoice_date,
            seller_id,
            client_name,
            client_address,
            candidate_name,
            designation,
            doj,
            salary,
            description,
            amount,
            status,
            iid
        ))

        c.execute("""
            INSERT INTO audit
                (invoice_id, user_id, action)
            VALUES (?, ?, ?)
        """, (iid, user["id"], "edited"))

        c.commit()

    return RedirectResponse("/", status_code=303)


# =========================================================
# VIEW / PRINT INVOICE
# =========================================================

@app.post("/invoices/{iid}/delete")
def delete_invoice(iid: int, user=Depends(require_user)):
    """Allow admins to permanently delete an invoice and its audit entries."""
    admin_only(user)
    with connect() as c:
        invoice = c.execute("SELECT id FROM invoices WHERE id = ?", (iid,)).fetchone()
        if not invoice:
            raise HTTPException(status_code=404, detail="Invoice not found")
        # Audit rows are not foreign-key cascaded in the existing schema.
        c.execute("DELETE FROM audit WHERE invoice_id = ?", (iid,))
        c.execute("DELETE FROM invoices WHERE id = ?", (iid,))
        c.commit()
    return RedirectResponse("/", status_code=303)


@app.get("/invoice/{iid}", response_class=HTMLResponse)
def invoice_view(
    iid: int,
    request: Request,
    user=Depends(require_user)
):
    with connect() as c:
        invoice = c.execute("""
            SELECT
                i.*,
                s.name AS seller_name,
                s.address AS seller_address,
                s.bank_account,
                s.bank_name,
                s.ifsc,
                s.pan,
                s.msme,
                s.email,
                s.phone,
                s.qr_code,
                u.username,
                u.full_name
            FROM invoices i
            JOIN sellers s ON s.id = i.seller_id
            JOIN users u ON u.id = i.created_by
            WHERE i.id = ?
        """, (iid,)).fetchone()

    if not invoice:
        raise HTTPException(404, "Invoice not found")

    if (
        user["role"] != "admin"
        and invoice["created_by"] != user["id"]
    ):
        raise HTTPException(403, "Not allowed")

    return templates.TemplateResponse(request, "invoice.html", {
            "user": user,
            "i": invoice
        
        })


# =========================================================
# CSV EXPORT
# Admin: all invoices
# User: own invoices only
# =========================================================

@app.get("/export.csv")
def export_csv(user=Depends(require_user)):
    with connect() as c:
        query = """
            SELECT
                i.invoice_no,
                i.invoice_date,
                u.username AS created_by,
                u.full_name AS created_by_name,
                s.name AS seller_name,
                s.address AS seller_address,
                s.bank_account,
                s.bank_name,
                s.ifsc,
                s.pan,
                s.msme,
                i.client_name,
                i.client_address,
                i.candidate_name,
                i.designation,
                i.doj,
                i.salary,
                i.description,
                i.amount,
                i.status,
                i.updated_at
            FROM invoices i
            LEFT JOIN users u ON u.id = i.created_by
            LEFT JOIN sellers s ON s.id = i.seller_id
        """

        if user["role"] != "admin":
            query += " WHERE i.created_by = ?"
            rows = c.execute(
                query + " ORDER BY i.id DESC",
                (user["id"],)
            ).fetchall()
        else:
            rows = c.execute(
                query + " ORDER BY i.id DESC"
            ).fetchall()

    stream = io.StringIO()
    writer = csv.writer(stream)

    if rows:
        writer.writerow(rows[0].keys())
        for row in rows:
            writer.writerow([row[key] for key in row.keys()])
    else:
        writer.writerow([
            "invoice_no",
            "invoice_date",
            "created_by",
            "seller_name",
            "client_name",
            "candidate_name",
            "doj",
            "salary",
            "amount",
            "status"
        ])

    content = "\ufeff" + stream.getvalue()

    return StreamingResponse(
        iter([content]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition":
                "attachment; filename=recruitment_invoices.csv"
        }
    )
