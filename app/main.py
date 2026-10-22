
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
