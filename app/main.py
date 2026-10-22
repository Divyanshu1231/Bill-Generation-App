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
