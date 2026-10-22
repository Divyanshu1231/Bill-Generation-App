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
