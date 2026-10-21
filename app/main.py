
import os
import io
import csv
import sqlite3
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

DB_PATH = os.getenv("DB_PATH", "./invoices.db")

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

def connect():
    db_directory = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(db_directory, exist_ok=True)

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


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
