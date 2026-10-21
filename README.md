# Recruitment Invoice Manager

FastAPI + server-rendered responsive dashboard for managing recruitment estimates/invoices based on the two provided reference PDFs.

## Features
- Admin and user login
- Admin creates users and multiple seller profiles
- Seller profiles include address, bank account, bank, IFSC, PAN, MSME, email and phone
- Users select a seller and create invoices with client, candidate, designation, DOJ, salary and service charge
- Invoice view with browser Print / Save as PDF
- CSV export: Admin sees all invoices; users see their own
- Admin can view all invoices; users can view only their own

## Deploy to Render free
1. Upload this project folder to a new GitHub repository.
2. In Render, choose **New + → Blueprint** and select the repository (Render reads `render.yaml`).
3. Set `ADMIN_PASSWORD` in the Render environment setup to a strong password (at least 8 characters).
4. Deploy and open the generated `onrender.com` URL.
5. Sign in with username `admin` and the password you configured.
6. Immediately add seller profiles and create user accounts. Do not use sample/reference bank details without permission.

## Important free-tier limitation
This starter uses SQLite at `/tmp/invoices.db`. Render free web services have an ephemeral filesystem, so data can be lost when the service restarts, redeploys or sleeps. **Do not use this for real business records until a persistent database is configured.** For persistent production data, use a PostgreSQL database and update the database layer to use `DATABASE_URL` (this starter does not yet include PostgreSQL adapter). Render's free PostgreSQL availability/retention may change; check the current Render dashboard and pricing.

## Local run
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
set ADMIN_PASSWORD=ChangeThisPassword123!
uvicorn app.main:app --reload
```
Open http://127.0.0.1:8000

## Security / scope notes
- Change the initial admin password immediately and set a unique `APP_SECRET`.
- Password hashes in this starter use SHA-256 for a minimal demo; replace with Argon2/bcrypt before production.
- This starter supports create and view; the edit route is included as a POST endpoint but a dedicated edit form is not yet wired into the UI.
- This is a starter application, not a reviewed accounting/tax product. Verify GST/tax requirements and invoice wording with your accountant.
