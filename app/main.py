import os, re, json, hmac, hashlib, secrets, sqlite3, time, requests, threading, copy, uuid
from pathlib import Path
from typing import Optional
import csv, io
from fastapi import FastAPI, HTTPException, Depends, Header, Request, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from matcher import load_candidate_records_auto, candidate_fields, send_doubletick_candidate_message

BASE=Path(__file__).resolve().parent
load_dotenv(BASE/'.env')
DB= os.getenv('DATABASE_PATH', str(BASE/'data'/'app.db'))
Path(DB).parent.mkdir(parents=True, exist_ok=True)
SECRET=os.getenv('APP_SECRET','').strip()
if not SECRET: SECRET='dev-'+secrets.token_urlsafe(32)
app=FastAPI(title='Space Consultant Recruitment Messaging API', version='1.0.0')

# DATABASE_URL selects PostgreSQL; otherwise local SQLite is used.
DATABASE_URL=os.getenv('DATABASE_URL','').strip()
try:
    from psycopg2 import IntegrityError as PostgresIntegrityError
    from psycopg2.extras import RealDictCursor
except ImportError:
    PostgresIntegrityError=Exception

class PgCursor:
    def __init__(self, cursor, inserted_id=None):
        self._cursor=cursor; self.lastrowid=inserted_id
    def fetchone(self): return self._cursor.fetchone()
    def fetchall(self): return self._cursor.fetchall()

class PgDB:
    def __init__(self):
        import psycopg2
        self.conn=psycopg2.connect(DATABASE_URL, sslmode='require', connect_timeout=15)
    def execute(self, sql, params=()):
        sql=sql.replace('BEGIN IMMEDIATE','BEGIN')
        if 'PRAGMA table_info(templates)' in sql:
            cur=self.conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='templates'")
            return PgCursor(cur)
        sql=sql.replace("date(created_at,'+5 hours','+30 minutes')=date('now','+5 hours','+30 minutes')", "(created_at::timestamp + interval '5 hours 30 minutes')::date=(CURRENT_TIMESTAMP + interval '5 hours 30 minutes')::date")
        sql=sql.replace('?', '%s')
        wants_id=sql.lstrip().upper().startswith('INSERT INTO LOGS ') and 'RETURNING' not in sql.upper()
        if wants_id: sql += ' RETURNING id'
        cur=self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, params)
        inserted_id=None
        if wants_id:
            row=cur.fetchone(); inserted_id=row['id'] if row else None
        return PgCursor(cur, inserted_id)
    def commit(self): self.conn.commit()
    def rollback(self): self.conn.rollback()
    def close(self): self.conn.close()

def db():
    if DATABASE_URL:
        c=PgDB()
        c.execute("CREATE TABLE IF NOT EXISTS users(id BIGSERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, role TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        c.execute("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        c.execute("CREATE TABLE IF NOT EXISTS templates(id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, name TEXT NOT NULL, kind TEXT NOT NULL, content TEXT NOT NULL, template_name TEXT DEFAULT '', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        existing={r['name'] for r in c.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='templates'").fetchall()}
        for col,ddl in {'language':"TEXT DEFAULT 'en'",'category':"TEXT DEFAULT 'MARKETING'",'approval_status':"TEXT DEFAULT 'DRAFT'",'header_text':"TEXT DEFAULT ''",'footer_text':"TEXT DEFAULT ''",'provider_detail':"TEXT DEFAULT ''",'submitted_at':"TEXT DEFAULT ''",'header_media_url':"TEXT DEFAULT ''"}.items():
            if col not in existing: c.execute(f'ALTER TABLE templates ADD COLUMN {col} {ddl}')
        c.execute("CREATE TABLE IF NOT EXISTS chat_messages(id BIGSERIAL PRIMARY KEY, user_id BIGINT, phone TEXT NOT NULL, candidate_name TEXT DEFAULT '', direction TEXT NOT NULL, message_type TEXT DEFAULT 'TEXT', content TEXT DEFAULT '', media_url TEXT DEFAULT '', provider_message_id TEXT DEFAULT '', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        chat_cols={r['name'] for r in c.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='chat_messages'").fetchall()}
        if 'is_read' not in chat_cols: c.execute("ALTER TABLE chat_messages ADD COLUMN is_read INTEGER NOT NULL DEFAULT 0")
        c.execute("CREATE TABLE IF NOT EXISTS internal_notes(id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, phone TEXT NOT NULL, note TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        c.execute("CREATE TABLE IF NOT EXISTS logs(id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, candidate_name TEXT, phone TEXT, kind TEXT, template_name TEXT, status TEXT, detail TEXT, provider_message_id TEXT DEFAULT '', status_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        c.execute("CREATE TABLE IF NOT EXISTS doubletick_webhook_events(id BIGSERIAL PRIMARY KEY, event_type TEXT DEFAULT '', provider_message_id TEXT DEFAULT '', recipient_phone TEXT DEFAULT '', provider_status TEXT DEFAULT '', normalized_status TEXT DEFAULT '', detail TEXT DEFAULT '', payload_json TEXT DEFAULT '', matched_log_id BIGINT, processing_result TEXT DEFAULT '', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        log_cols={r['name'] for r in c.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='logs'").fetchall()}
        for col,ddl in {'provider_message_id':"TEXT DEFAULT ''",'status_updated_at':"TIMESTAMP DEFAULT CURRENT_TIMESTAMP"}.items():
            if col not in log_cols: c.execute(f'ALTER TABLE logs ADD COLUMN {col} {ddl}')
        c.commit(); return c
    c=sqlite3.connect(os.getenv('DATABASE_PATH', str(BASE/'data'/'app.db'))); c.row_factory=sqlite3.Row
    c.execute("CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, role TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    c.execute("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    c.execute("CREATE TABLE IF NOT EXISTS templates(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, name TEXT NOT NULL, kind TEXT NOT NULL, content TEXT NOT NULL, template_name TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    existing={r['name'] for r in c.execute('PRAGMA table_info(templates)').fetchall()}
    for col,ddl in {'language':"TEXT DEFAULT 'en'",'category':"TEXT DEFAULT 'MARKETING'",'approval_status':"TEXT DEFAULT 'DRAFT'",'header_text':"TEXT DEFAULT ''",'footer_text':"TEXT DEFAULT ''",'provider_detail':"TEXT DEFAULT ''",'submitted_at':"TEXT DEFAULT ''",'header_media_url':"TEXT DEFAULT ''"}.items():
        if col not in existing: c.execute(f'ALTER TABLE templates ADD COLUMN {col} {ddl}')
    c.execute("CREATE TABLE IF NOT EXISTS chat_messages(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, phone TEXT NOT NULL, candidate_name TEXT DEFAULT '', direction TEXT NOT NULL, message_type TEXT DEFAULT 'TEXT', content TEXT DEFAULT '', media_url TEXT DEFAULT '', provider_message_id TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    chat_cols={r['name'] for r in c.execute('PRAGMA table_info(chat_messages)').fetchall()}
    if 'is_read' not in chat_cols: c.execute("ALTER TABLE chat_messages ADD COLUMN is_read INTEGER NOT NULL DEFAULT 0")
    c.execute("CREATE TABLE IF NOT EXISTS internal_notes(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, phone TEXT NOT NULL, note TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    c.execute("CREATE TABLE IF NOT EXISTS logs(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, candidate_name TEXT, phone TEXT, kind TEXT, template_name TEXT, status TEXT, detail TEXT, provider_message_id TEXT DEFAULT '', status_updated_at TEXT DEFAULT CURRENT_TIMESTAMP, created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    c.execute("CREATE TABLE IF NOT EXISTS doubletick_webhook_events(id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT DEFAULT '', provider_message_id TEXT DEFAULT '', recipient_phone TEXT DEFAULT '', provider_status TEXT DEFAULT '', normalized_status TEXT DEFAULT '', detail TEXT DEFAULT '', payload_json TEXT DEFAULT '', matched_log_id INTEGER, processing_result TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    existing_logs={r['name'] for r in c.execute('PRAGMA table_info(logs)').fetchall()}
    for col,ddl in {'provider_message_id':"TEXT DEFAULT ''",'status_updated_at':"TEXT DEFAULT CURRENT_TIMESTAMP"}.items():
        if col not in existing_logs: c.execute(f'ALTER TABLE logs ADD COLUMN {col} {ddl}')
    c.commit(); return c

def phash(pw, salt=None):
    salt=salt or secrets.token_hex(16)
    return salt+'$'+hashlib.pbkdf2_hmac('sha256',pw.encode(),salt.encode(),240000).hex()
def verify(pw, saved):
    try:
        salt, digest=saved.split('$',1)
        return hmac.compare_digest(hashlib.pbkdf2_hmac('sha256',pw.encode(),salt.encode(),240000).hex(),digest)
    except Exception:return False
def token(uid, role):
    payload=f'{uid}:{role}:{int(time.time())+86400}'
    sig=hmac.new(SECRET.encode(),payload.encode(),hashlib.sha256).hexdigest()
    import base64
    return base64.urlsafe_b64encode((payload+':'+sig).encode()).decode().rstrip('=')
def current_user(authorization: Optional[str]=Header(None)):
    if not authorization or not authorization.startswith('Bearer '): raise HTTPException(401,'Login required')
    import base64
    try:
        raw=base64.urlsafe_b64decode(authorization[7:]+'===').decode(); uid,role,exp,sig=raw.split(':')
        payload=f'{uid}:{role}:{exp}'
        if int(exp)<time.time() or not hmac.compare_digest(sig,hmac.new(SECRET.encode(),payload.encode(),hashlib.sha256).hexdigest()): raise ValueError()
        c=db(); u=c.execute('SELECT id,username,role,active FROM users WHERE id=?',(uid,)).fetchone(); c.close()
        if not u or not u['active']: raise ValueError()
        return dict(u)
    except Exception: raise HTTPException(401,'Invalid or expired session')
def admin(u=Depends(current_user)):
    if u['role']!='admin': raise HTTPException(403,'Admin access required')
    return u

def get_setting(key, default=''):
    c=db(); r=c.execute('SELECT value FROM settings WHERE key=?',(key,)).fetchone(); c.close(); return r['value'] if r else default
def airtable_urls():
    raw=get_setting('airtable_urls','')
    if raw:
        try:
            urls=json.loads(raw)
            if isinstance(urls,list): return [str(x).strip() for x in urls if str(x).strip()]
        except Exception: pass
    legacy=get_setting('airtable_url','').strip()
    return [legacy] if legacy else []
# Airtable candidate cache: filter changes reuse the same fetched records instead
# of downloading every Airtable view again. Cache is per running app worker.
CANDIDATE_CACHE_TTL = max(30, int(os.getenv('AIRTABLE_CACHE_TTL_SECONDS', '300')))
_candidate_cache = {'key': None, 'expires': 0.0, 'rows': None}
_candidate_cache_lock = threading.Lock()

def clear_candidate_cache():
    with _candidate_cache_lock:
        _candidate_cache.update({'key': None, 'expires': 0.0, 'rows': None})

def records(force_refresh=False):
    urls=airtable_urls()
    if not urls: raise HTTPException(400,'Admin has not configured any Airtable candidate view URLs.')
    cache_key=tuple(urls)
    now=time.monotonic()
    if not force_refresh and _candidate_cache['key']==cache_key and _candidate_cache['rows'] is not None and now < _candidate_cache['expires']:
        return copy.deepcopy(_candidate_cache['rows'])
    try:
        # Lock prevents simultaneous filter requests from all fetching Airtable.
        with _candidate_cache_lock:
            now=time.monotonic()
            if not force_refresh and _candidate_cache['key']==cache_key and _candidate_cache['rows'] is not None and now < _candidate_cache['expires']:
                return copy.deepcopy(_candidate_cache['rows'])
            token_value=os.getenv('AIRTABLE_TOKEN','').strip()
            combined=[]; seen=set()
            for index,url in enumerate(urls):
                rows,_=load_candidate_records_auto(url,token_value)
                for row in rows:
                    rid=str(row.get('_record_id') or abs(hash(json.dumps(row,sort_keys=True,default=str))))
                    # Prefix IDs by source so records from different Airtable bases cannot collide.
                    unique=f'{index}:{rid}'
                    if unique in seen: continue
                    seen.add(unique); row['_record_id']=unique; combined.append(row)
            _candidate_cache.update({'key':cache_key, 'expires':time.monotonic()+CANDIDATE_CACHE_TTL, 'rows':combined})
            return copy.deepcopy(combined)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502,'Airtable fetch failed: '+str(e))
def norm(v): return re.sub(r'\s+',' ',str(v or '').strip()).casefold()
def values(v):
    if isinstance(v,list): return [str(x).strip() for x in v if str(x).strip()]
    return [str(v).strip()] if str(v or '').strip() else []
def row_for(r):
    f=candidate_fields(r)
    return {'id':r.get('_record_id') or str(abs(hash(json.dumps(r,sort_keys=True,default=str)))), 'name':str(f.get('name') or ''),'phone':str(f.get('phone') or ''),'specialization':values(f.get('specialization')),'state':values(f.get('state') or f.get('location')),'city':values(f.get('city')),'raw':r}
def replace_vars(text,c):
    f=candidate_fields(c)
    d={'name':f.get('name',''),'doctor_name':f.get('name',''),'specialization':f.get('specialization',''),'city':f.get('city',''),'state':f.get('state','')}
    return re.sub(r'\{\{\s*([a-zA-Z_]+)\s*\}\}',lambda m:str(d.get(m.group(1).lower(),'')),text)

class Login(BaseModel): username:str; password:str
class AdminPasswordChange(BaseModel): current_password:str; new_password:str=Field(min_length=8, max_length=128)
class UserCreate(BaseModel): username:str; password:str=Field(min_length=8)
class UserUpdate(BaseModel): password:Optional[str]=None; active:Optional[bool]=None
class AirtableConfig(BaseModel): urls:list[str]=[]; max_messages_per_user_per_day:int=50
class TemplateIn(BaseModel):
    name:str; kind:str='custom'; content:str; template_name:str=''
    language:str='en'; category:str='MARKETING'; header_text:str=''; footer_text:str=''; header_media_url:str=''
class SendIn(BaseModel): candidate_ids:list[str]; kind:str; template_name:str=''; message:str=''; language:str='en'
class CsvPreviewIn(BaseModel): upload_id:str
class CsvSendIn(BaseModel): upload_id:str; candidate_ids:list[str]; template_name:str; language:str='en'; confirm_opt_in:bool=False
class ChatReplyIn(BaseModel): message:str=Field(min_length=1,max_length=4000)


# ---- Pull-based delivery status sync (backup for missed/late webhooks) ----
_STATUS_RANK={'PROCESSING':0,'ENQUEUED':1,'UNKNOWN':1,'SENT':2,'DELIVERED':3,'READ':4}
_sync_lock=threading.Lock()
_sync_last_try={}

def sync_pending_statuses(user_id=None, limit=15):
    """Ask DoubleTick (GET /whatsapp/message/status) for messages still stuck in
    ENQUEUED/SENT/UNKNOWN and update logs. API limit is 1 request/second."""
    if not _sync_lock.acquire(blocking=False):
        return {'checked':0,'updated':0,'busy':True}
    checked=updated=0
    try:
        key=os.getenv('DOUBLETICK_API_KEY','').strip()
        if not key: return {'checked':0,'updated':0,'error':'DOUBLETICK_API_KEY missing'}
        c=db()
        try:
            sql="SELECT id,status,provider_message_id FROM logs WHERE UPPER(COALESCE(status,'')) IN ('ENQUEUED','UNKNOWN','SENT') AND COALESCE(provider_message_id,'')<>''"
            params=[]
            if user_id:
                sql+=" AND user_id=?"; params.append(user_id)
            rows=c.execute(sql+" ORDER BY id DESC LIMIT 300",tuple(params)).fetchall()
            for r in rows:
                if checked>=limit: break
                lid=r['id']; pid=str(r['provider_message_id'] or '').strip()
                if not re.fullmatch(r'[0-9a-fA-F-]{36}',pid): continue
                if time.time()-_sync_last_try.get(lid,0)<300: continue
                _sync_last_try[lid]=time.time(); checked+=1
                try:
                    resp=requests.get('https://public.doubletick.io/whatsapp/message/status',
                                      headers={'Authorization':key,'Accept':'application/json'},
                                      params={'messageId':pid},timeout=15)
                except Exception:
                    time.sleep(1.1); continue
                if resp.status_code==429: break
                if resp.status_code!=200:
                    time.sleep(1.1); continue
                try: d=resp.json()
                except Exception: d={}
                new=None; detail=''
                if d.get('failedAt'):
                    new='FAILED'; detail=str(d.get('failedReason') or d.get('errorCode') or 'Message failed')[:1500]
                    if _is_frequency_capping(detail) or str(d.get('errorCode') or '')=='131049':
                        new='FREQUENCY_CAPPING'
                elif d.get('readAt'): new='READ'
                elif d.get('deliveredAt'): new='DELIVERED'
                elif d.get('sentAt'): new='SENT'
                old=str(r['status'] or '').upper()
                if new and new!=old and (new in ('FAILED','FREQUENCY_CAPPING') or _STATUS_RANK.get(new,0)>_STATUS_RANK.get(old,0)):
                    c.execute("UPDATE logs SET status=?,detail=?,status_updated_at=CURRENT_TIMESTAMP WHERE id=?",
                              (new, detail or 'DoubleTick status update: '+new, lid))
                    c.commit(); updated+=1
                time.sleep(1.1)
        finally:
            c.close()
    except Exception:
        import logging
        logging.getLogger("uvicorn.error").exception("DoubleTick status sync failed")
    finally:
        _sync_lock.release()
    return {'checked':checked,'updated':updated}

def _status_sync_loop():
    time.sleep(20)
    while True:
        try: sync_pending_statuses(None,limit=20)
        except Exception: pass
        cleanup_old_chats()
        time.sleep(900)

@app.post('/api/history/sync-status')
def history_sync_status(u=Depends(current_user)):
    return sync_pending_statuses(None if u['role']=='admin' else u['id'],limit=15)


# ---- Chat retention: only the last N days of chats are kept (default 7) ----
CHAT_RETENTION_DAYS=max(1,int(os.getenv('CHAT_RETENTION_DAYS','7') or 7))

def _chat_cutoff():
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc)-timedelta(days=CHAT_RETENTION_DAYS)).strftime('%Y-%m-%d %H:%M:%S')

def cleanup_old_chats():
    """Delete saved WhatsApp chat messages older than CHAT_RETENTION_DAYS.
    Message History (logs) and internal notes are not touched."""
    try:
        c=db()
        try:
            c.execute("DELETE FROM chat_messages WHERE created_at < ?",(_chat_cutoff(),))
            # Remove WhatsApp delivery-error texts that older code saved as if the candidate had written them.
            c.execute("DELETE FROM chat_messages WHERE direction='IN' AND COALESCE(provider_message_id,'')='' AND (content LIKE ? OR content LIKE ? OR content LIKE ? OR content LIKE ?)",
                      ('This message was not delivered%','Message undeliverable%','Re-engagement message%','Message Undeliverable%'))
            c.commit()
        finally:
            c.close()
    except Exception:
        import logging
        logging.getLogger("uvicorn.error").exception("Old chat cleanup failed")

# ---- Import old customer replies from DoubleTick (backfill) ----
_backfill_states={}
_backfill_lock=threading.Lock()

def _new_backfill_state():
    return {'running':False,'total':0,'done':0,'inserted':0,'skipped_existing':0,'not_found':0,'errors':0,'last_error':'','finished':False}

def _run_chat_backfill(days, max_phones, user_id, st):
    """user_id=None imports for every recruiter (admin); otherwise only chats whose
    latest owner is that recruiter."""
    from datetime import datetime, timedelta, timezone
    key=os.getenv('DOUBLETICK_API_KEY','').strip()
    try:
        waba=dt_sender_number()
    except Exception as exc:
        st.update(running=False,finished=True,last_error=str(getattr(exc,'detail',exc))); _backfill_lock.release(); return
    if not key:
        st.update(running=False,finished=True,last_error='DOUBLETICK_API_KEY missing'); _backfill_lock.release(); return
    now=datetime.now(timezone.utc)
    start=(now+timedelta(hours=5,minutes=30)-timedelta(days=days)).strftime('%d-%m-%Y')
    oldest=now-timedelta(days=CHAT_RETENTION_DAYS)
    c=db()
    try:
        if user_id:
            rows=c.execute("SELECT phone,MAX(id) mid FROM logs WHERE COALESCE(phone,'')<>'' AND user_id=? GROUP BY phone ORDER BY mid DESC LIMIT ?",(user_id,max_phones)).fetchall()
        else:
            rows=c.execute("SELECT phone,MAX(id) mid FROM logs WHERE COALESCE(phone,'')<>'' GROUP BY phone ORDER BY mid DESC LIMIT ?",(max_phones,)).fetchall()
        phones=[]; seen=set()
        for r in rows:
            d=re.sub(r'\D','',str(r['phone'] or ''))
            if len(d)==10: d='91'+d
            if len(d)>=11 and d not in seen: seen.add(d); phones.append(d)
        st['total']=len(phones)
        owner_sql=("SELECT user_id,candidate_name FROM logs WHERE RIGHT(REGEXP_REPLACE(COALESCE(phone,''),'[^0-9]','','g'),10)=? ORDER BY id DESC LIMIT 1" if DATABASE_URL else
                   "SELECT user_id,candidate_name FROM logs WHERE substr(replace(replace(replace(phone,'+',''),' ',''),'-',''),-10)=? ORDER BY id DESC LIMIT 1")
        for d in phones:
            try:
                owner=c.execute(owner_sql,(d[-10:],)).fetchone()
                owner_uid=owner['user_id'] if owner else None
                if user_id and owner_uid!=user_id:
                    st['done']+=1; continue   # another recruiter owns this chat now
                resp=requests.get('https://public.doubletick.io/chat-messages',
                                  headers={'Authorization':key,'Accept':'application/json'},
                                  params={'wabaNumber':waba,'customerNumber':d,'startDate':start},timeout=30)
                if resp.status_code==429:
                    time.sleep(10)
                    resp=requests.get('https://public.doubletick.io/chat-messages',
                                      headers={'Authorization':key,'Accept':'application/json'},
                                      params={'wabaNumber':waba,'customerNumber':d,'startDate':start},timeout=30)
                if resp.status_code==404: st['not_found']+=1
                elif resp.status_code not in (200,201):
                    st['errors']+=1; st['last_error']=f'HTTP {resp.status_code}: {resp.text[:200]}'
                else:
                    data=resp.json()
                    items=data.get('messages') if isinstance(data,dict) else data
                    for it in (items or []):
                        if not isinstance(it,dict): continue
                        if str(it.get('messageOriginType') or '').upper()!='CUSTOMER': continue
                        mid=str(it.get('id') or '').strip()
                        m=it.get('message') if isinstance(it.get('message'),dict) else {}
                        doc=m.get('document') if isinstance(m.get('document'),dict) else {}
                        mtype=str(m.get('messageType') or m.get('type') or 'TEXT').upper()
                        media=str(m.get('url') or m.get('mediaUrl') or doc.get('url') or doc.get('link') or '')[:2000]
                        fname=str(m.get('fileName') or m.get('filename') or doc.get('filename') or doc.get('fileName') or '').strip()
                        text=m.get('text') or m.get('caption') or ''
                        if isinstance(text,dict): text=text.get('body') or json.dumps(text,ensure_ascii=False)
                        text=str(text or '')
                        if fname: text=(f'📄 {fname}'+('\n'+text if text else ''))
                        if not text: text=f'[{mtype}]' if media else f'[{mtype} message]'
                        text=text[:5000]
                        try: when=datetime.fromtimestamp(int(it.get('messageTime'))/1000,tz=timezone.utc)
                        except Exception: when=now
                        if when<oldest: continue   # would be removed by the 7-day cleanup anyway
                        same=c.execute("SELECT id,media_url FROM chat_messages WHERE provider_message_id=? LIMIT 1",(mid,)).fetchone() if mid else None
                        if same:
                            if media and not (same['media_url'] or ''):
                                c.execute("UPDATE chat_messages SET media_url=?,message_type=? WHERE id=?",(media,mtype,same['id'])); st['links_added']=st.get('links_added',0)+1
                            st['skipped_existing']+=1; continue
                        dup=None
                        for ex in c.execute("SELECT id,media_url,created_at FROM chat_messages WHERE phone=? AND direction='IN' AND content=?",(d,text)).fetchall():
                            try:
                                exd=ex['created_at'] if not isinstance(ex['created_at'],str) else datetime.fromisoformat(ex['created_at'].replace('T',' ').split('.')[0])
                                if abs((exd.replace(tzinfo=None)-when.replace(tzinfo=None)).total_seconds())<180: dup=ex; break
                            except Exception: pass
                        if dup:
                            if media and not (dup['media_url'] or ''):
                                c.execute("UPDATE chat_messages SET media_url=?,message_type=? WHERE id=?",(media,mtype,dup['id'])); st['links_added']=st.get('links_added',0)+1
                            st['skipped_existing']+=1; continue
                        is_read=0 if (now-when).total_seconds()<48*3600 else 1
                        name=str((owner['candidate_name'] if owner else '') or d)[:200]
                        c.execute("INSERT INTO chat_messages(user_id,phone,candidate_name,direction,message_type,content,media_url,provider_message_id,created_at,is_read) VALUES(?,?,?,?,?,?,?,?,?,?)",
                                  (owner_uid,d,name,'IN',mtype,text,media,mid,when.strftime('%Y-%m-%d %H:%M:%S'),is_read))
                        st['inserted']+=1
                    c.commit()
            except Exception as exc:
                try: c.rollback()
                except Exception: pass
                st['errors']+=1; st['last_error']=str(exc)[:200]
            st['done']+=1
            time.sleep(0.3)
    finally:
        c.close(); st.update(running=False,finished=True)
        _backfill_lock.release()

@app.post('/api/chats/backfill')
def chats_backfill(days:int=7,max_phones:int=3000,u=Depends(current_user)):
    days=max(1,min(days,CHAT_RETENTION_DAYS)); max_phones=max(1,min(max_phones,10000))
    key=0 if u['role']=='admin' else u['id']
    st=_backfill_states.setdefault(key,_new_backfill_state())
    if not _backfill_lock.acquire(blocking=False):
        return {'ok':True,'already_running':True,'busy_other_user':not st.get('running'),**st}
    st.clear(); st.update(_new_backfill_state()); st['running']=True
    threading.Thread(target=_run_chat_backfill,args=(days,max_phones,None if u['role']=='admin' else u['id'],st),daemon=True).start()
    return {'ok':True,'started':True,**st}

@app.get('/api/chats-backfill-status')
def chats_backfill_status(u=Depends(current_user)):
    key=0 if u['role']=='admin' else u['id']
    return dict(_backfill_states.setdefault(key,_new_backfill_state()))

@app.on_event('startup')
def startup():
    threading.Thread(target=_status_sync_loop,daemon=True).start()
    cleanup_old_chats()
    # ADMIN_USERNAME / ADMIN_PASSWORD are the authoritative bootstrap credentials.
    # Existing HR users, templates, settings, logs and chats are never deleted.
    c=db()
    try:
        username=(os.getenv('ADMIN_USERNAME') or 'admin').strip()
        password=os.getenv('ADMIN_PASSWORD') or ''
        admin_row=c.execute("SELECT id,username FROM users WHERE role='admin' ORDER BY id ASC LIMIT 1").fetchone()
        if not admin_row:
            # Create the first admin only when no admin account exists.
            if not password:
                password=secrets.token_urlsafe(18)
                print('INITIAL ADMIN PASSWORD was generated. Set ADMIN_PASSWORD in Render Environment.')
            c.execute("INSERT INTO users(username,password_hash,role,active) VALUES(?,?,'admin',1)",(username,phash(password)))
            c.commit()
        else:
            # Do not overwrite an existing admin password on every restart.
            # This allows the admin to securely change their password in the dashboard.
            print('Existing admin credentials retained; use the dashboard to change the password.')
    finally:
        c.close()

@app.get('/')
def home(): return FileResponse(BASE/'static'/'index.html')
@app.get('/health')
def health(): return {'ok':True}
@app.post('/api/login')
def login(x:Login):
    c=db(); u=c.execute('SELECT * FROM users WHERE username=?',(x.username.strip(),)).fetchone(); c.close()
    if not u or not u['active'] or not verify(x.password,u['password_hash']): raise HTTPException(401,'Incorrect username or password')
    return {'token':token(u['id'],u['role']),'user':{'id':u['id'],'username':u['username'],'role':u['role']}}
@app.get('/api/me')
def me(u=Depends(current_user)): return u
@app.post('/api/me/change-password')
def change_admin_password(x:AdminPasswordChange,u=Depends(admin)):
    c=db()
    try:
        row=c.execute('SELECT password_hash FROM users WHERE id=? AND role=\'admin\'',(u['id'],)).fetchone()
        if not row or not verify(x.current_password,row['password_hash']):
            raise HTTPException(400,'Current password is incorrect.')
        if x.current_password == x.new_password:
            raise HTTPException(400,'New password must be different from the current password.')
        c.execute('UPDATE users SET password_hash=? WHERE id=?',(phash(x.new_password),u['id']))
        c.commit()
        return {'ok':True,'message':'Admin password changed successfully.'}
    finally:
        c.close()
@app.get('/api/admin/config')
def config(u=Depends(admin)):
    urls=airtable_urls()
    try: limit=int(get_setting('max_messages_per_user_per_day','50'))
    except Exception: limit=50
    return {'airtable_urls':urls,'airtable_url':'\n'.join(urls),'airtable_configured':bool(urls),'max_messages_per_user_per_day':limit}
@app.put('/api/admin/config')
def set_config(x:AirtableConfig,u=Depends(admin)):
    urls=list(dict.fromkeys(v.strip() for v in x.urls if v.strip()))
    for url in urls:
        if not url.startswith(('https://airtable.com/','https://www.airtable.com/')): raise HTTPException(400,'Every Airtable URL must start with https://airtable.com/ or https://www.airtable.com/.')
    if not 1<=x.max_messages_per_user_per_day<=5000: raise HTTPException(400,'Daily HR message limit must be between 1 and 5000.')
    c=db()
    c.execute('INSERT INTO settings(key,value) VALUES(\'airtable_urls\',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(json.dumps(urls),))
    c.execute('INSERT INTO settings(key,value) VALUES(\'max_messages_per_user_per_day\',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(str(x.max_messages_per_user_per_day),))
    c.commit(); c.close(); clear_candidate_cache(); return {'ok':True,'airtable_urls':urls,'max_messages_per_user_per_day':x.max_messages_per_user_per_day}
@app.get('/api/admin/users')
def users(u=Depends(admin)):
    c=db(); rows=c.execute("SELECT id,username,role,active,created_at FROM users WHERE role='hr' ORDER BY id DESC").fetchall(); c.close(); return [dict(r) for r in rows]
@app.post('/api/admin/users')
def create_user(x:UserCreate,u=Depends(admin)):
    if not re.fullmatch(r'[A-Za-z0-9_.-]{3,40}',x.username): raise HTTPException(400,'Username must be 3–40 characters (letters, numbers, dot, underscore or hyphen).')
    c=db()
    try: c.execute("INSERT INTO users(username,password_hash,role) VALUES(?,?,'hr')",(x.username,phash(x.password))); c.commit()
    except (sqlite3.IntegrityError, PostgresIntegrityError): c.close(); raise HTTPException(409,'Username already exists')
    c.close(); return {'ok':True}
@app.patch('/api/admin/users/{uid}')
def update_user(uid:int,x:UserUpdate,u=Depends(admin)):
    c=db(); r=c.execute("SELECT id FROM users WHERE id=? AND role='hr'",(uid,)).fetchone()
    if not r: c.close(); raise HTTPException(404,'User not found')
    if x.password is not None:
        if len(x.password)<8: c.close(); raise HTTPException(400,'Password must be at least 8 characters')
        c.execute('UPDATE users SET password_hash=? WHERE id=?',(phash(x.password),uid))
    if x.active is not None:c.execute('UPDATE users SET active=? WHERE id=?',(int(x.active),uid))
    c.commit(); c.close(); return {'ok':True}
@app.get('/api/candidates')
def candidates(
    specialization: str = '',
    state: str = '',
    city: str = '',
    spec: Optional[str] = None,
    refresh: bool = False,
    u=Depends(current_user),
):
    # `spec` is accepted as a backward-compatible alias for older dashboards.
    specialization = (specialization or spec or '').strip()

    # Build dropdown options from the complete source list BEFORE filtering.
    # This prevents a selected filter from removing other available options.
    all_data = [row_for(r) for r in records(force_refresh=refresh)]
    specs = sorted({v for x in all_data for v in x['specialization']}, key=str.casefold)
    states = sorted({v for x in all_data for v in x['state']}, key=str.casefold)
    cities = sorted({v for x in all_data for v in x['city']}, key=str.casefold)

    filtered = all_data
    if specialization:
        wanted = norm(specialization)
        filtered = [
            x for x in filtered
            if any(norm(v) == wanted for v in x['specialization'])
        ]
    if state:
        wanted = norm(state)
        filtered = [x for x in filtered if any(norm(v) == wanted for v in x['state'])]
    if city:
        wanted = norm(city)
        filtered = [x for x in filtered if any(norm(v) == wanted for v in x['city'])]

    safe_candidates = [
        {k: v for k, v in x.items() if k not in ('raw', 'phone')}
        for x in filtered
    ]
    return {
        'options': {'specializations': specs, 'states': states, 'cities': cities},
        'candidates': safe_candidates,
        'total_candidates': len(safe_candidates),
    }

# Temporary per-process CSV uploads. Files are kept in memory and never written to disk.
_csv_uploads = {}
_csv_upload_lock = threading.Lock()

def _csv_norm(v): return re.sub(r'[^a-z0-9]+','',str(v or '').lower())
def _csv_field(row, aliases):
    lookup={_csv_norm(k):v for k,v in row.items()}
    for a in aliases:
        if _csv_norm(a) in lookup and str(lookup[_csv_norm(a)] or '').strip(): return lookup[_csv_norm(a)]
    for k,v in row.items():
        nk=_csv_norm(k)
        if any(_csv_norm(a) in nk for a in aliases) and str(v or '').strip(): return v
    return ''
def _csv_candidate(row):
    from matcher import phone
    name=_csv_field(row,['Candidate Name','Name of Candidates','Full Name','Name','Candidate'])
    mobile=_csv_field(row,['WhatsApp Number','Mobile Number','Mobile No','Phone Number','Phone','Mobile','Contact Number','Contact'])
    spec=_csv_field(row,['Specialization','Specialisation','Specialty','Speciality','Department'])
    state=_csv_field(row,['State','Location State','Candidate State','Preferred State'])
    city=_csv_field(row,['City','Location City','Preferred City'])
    # Consent is confirmed by the sender at campaign time; it is not inferred from a CSV column.
    return {'name':str(name or '').strip(),'phone':phone(mobile),'specialization':str(spec or '').strip(),'state':str(state or '').strip(),'city':str(city or '').strip(),'opt_in':False,'_raw':row}

@app.post('/api/csv/upload')
async def upload_candidate_csv(file: UploadFile=File(...), u=Depends(current_user)):
    if not file.filename.lower().endswith('.csv'): raise HTTPException(400,'Please upload a .csv file exported from Airtable.')
    raw=await file.read(5*1024*1024+1)
    if len(raw)>5*1024*1024: raise HTTPException(413,'CSV file must be 5 MB or smaller.')
    try:
        text=raw.decode('utf-8-sig')
        reader=csv.DictReader(io.StringIO(text))
        headers=reader.fieldnames or []
        rows=list(reader)
    except Exception as exc: raise HTTPException(400,'Could not read CSV: '+str(exc))
    if not headers or not rows: raise HTTPException(400,'CSV has no header or candidate records.')
    if len(rows)>2000: raise HTTPException(400,'Maximum 2,000 rows per CSV upload.')
    upload_id=secrets.token_urlsafe(18)
    with _csv_upload_lock:
        _csv_uploads[upload_id]={'user_id':u['id'],'rows':rows,'created':time.time()}
        for key in list(_csv_uploads):
            if time.time()-_csv_uploads[key]['created']>1800: _csv_uploads.pop(key,None)
    preview=[_csv_candidate(r) for r in rows[:8]]
    return {'upload_id':upload_id,'headers':headers,'total_rows':len(rows),'preview':[{'name':x['name'],'phone_present':bool(x['phone']),'specialization':x['specialization'],'state':x['state'],'city':x['city'],'opt_in':False} for x in preview]}

@app.post('/api/csv/preview')
def preview_candidate_csv(x:CsvPreviewIn,u=Depends(current_user)):
    item=_csv_uploads.get(x.upload_id)
    if not item or item['user_id']!=u['id']: raise HTTPException(404,'CSV upload expired. Please upload it again.')
    candidates=[_csv_candidate(r) for r in item['rows']]
    seen=set(); result=[]
    for i,cand in enumerate(candidates):
        digits=re.sub(r'\D','',cand['phone'])
        duplicate=digits in seen if digits else False
        if digits: seen.add(digits)
        result.append({'id':str(i),'name':cand['name'] or 'Unnamed candidate','phone_display':cand['phone'],'specialization':cand['specialization'],'state':cand['state'],'city':cand['city'],'phone_valid':len(digits)>=12,'opt_in':False,'duplicate':duplicate,'sendable':bool(cand['phone']) and len(digits)>=12 and not duplicate})
    item['candidates']=candidates
    return {'total':len(result),'sendable':sum(1 for x in result if x['sendable']),'candidates':result}

@app.post('/api/csv/send')
def send_candidate_csv(x:CsvSendIn,u=Depends(current_user)):
    item=_csv_uploads.get(x.upload_id)
    if not item or item['user_id']!=u['id']: raise HTTPException(404,'CSV upload expired. Please upload it again.')
    if not x.candidate_ids: raise HTTPException(400,'Select at least one eligible CSV candidate.')
    if not x.confirm_opt_in: raise HTTPException(400,'Confirm that every selected contact has explicitly opted in to receive WhatsApp messages.')
    if not x.template_name.strip(): raise HTTPException(400,'Approved DoubleTick template name is required.')
    from matcher import send_doubletick_candidate_message
    rows=item.get('candidates') or [_csv_candidate(r) for r in item['rows']]
    wanted=set(x.candidate_ids); chosen=[]
    for i,cand in enumerate(rows):
        digits=re.sub(r'\D','',cand['phone'])
        if str(i) in wanted:
            if not cand['phone'] or len(digits)<12: raise HTTPException(400,'Selected CSV contains an invalid phone number.')
            chosen.append((i,cand))
    if len(chosen)!=len(wanted): raise HTTPException(400,'Some selected CSV rows are invalid. Preview the CSV again.')
    c=db()
    try:
        approved=c.execute("SELECT id FROM templates WHERE user_id=? AND template_name=? AND approval_status='APPROVED'",(u['id'],x.template_name.strip())).fetchone()
        if not approved: raise HTTPException(400,'Choose a template that is approved and saved under your account.')
        # Reject duplicate phone numbers even if a client manually submits disabled rows.
        seen_numbers=set()
        for _,candidate in chosen:
            digits=re.sub(r'\D','',candidate['phone'])
            if digits in seen_numbers: raise HTTPException(400,'Duplicate phone numbers are not allowed in one CSV campaign.')
            seen_numbers.add(digits)
        limit=int(get_setting('max_messages_per_user_per_day','50'))
        if u['role']!='admin':
            used=c.execute("SELECT COUNT(*) n FROM logs WHERE user_id=? AND date(created_at,'+5 hours','+30 minutes')=date('now','+5 hours','+30 minutes')",(u['id'],)).fetchone()['n']
            remaining=max(0,limit-used)
            if len(chosen)>remaining: raise HTTPException(429,f'Daily WhatsApp limit reached. You can attempt {remaining} more message(s) today.')
        results=[]
        for i,cand in chosen:
            status='FAILED'; detail=''; provider_id=''
            # Construct the same candidate shape expected by matcher.py.
            payload={'Name':cand['name'],'Mobile Number':cand['phone'],'Specialization':cand['specialization'],'State':cand['state'],'City':cand['city']}
            try:
                response=send_doubletick_candidate_message(payload,x.template_name.strip(),x.language.strip() or 'en')
                provider_id=_find_provider_message_id(response); provider_status,provider_error=_find_provider_status(response)
                status=provider_status if provider_status in ('SENT','DELIVERED','READ','ENQUEUED') else 'ENQUEUED'
                if provider_status=='FAILED': status='FAILED'; detail=provider_error or json.dumps(response,ensure_ascii=False)[:1500]
                else: detail=json.dumps(response,ensure_ascii=False)[:1500]
            except Exception as exc: detail=str(exc)[:1500]
            c.execute("INSERT INTO logs(user_id,candidate_name,phone,kind,template_name,status,detail,provider_message_id,status_updated_at) VALUES(?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)",(u['id'],cand['name'],cand['phone'],'csv','CSV: '+x.template_name.strip(),status,detail,provider_id))
            c.commit(); results.append({'name':cand['name'],'status':status,'detail':detail})
        return {'ok':True,'summary':{'total':len(results),'sent':sum(z['status'] in ('SENT','DELIVERED','READ','ENQUEUED') for z in results),'failed':sum(z['status']=='FAILED' for z in results)},'results':results}
    finally: c.close()

@app.get('/api/templates')
def templates(u=Depends(current_user)):
    c=db(); rows=c.execute('SELECT * FROM templates WHERE user_id=? ORDER BY id DESC',(u['id'],)).fetchall(); c.close(); return [dict(r) for r in rows]

@app.post('/api/templates/upload-image')
async def upload_template_image(file:UploadFile=File(...), u=Depends(current_user)):
    """Upload a JPG/PNG sample to DoubleTick for an IMAGE template header."""
    filename=(file.filename or 'template-image').strip()
    key=os.getenv('DOUBLETICK_API_KEY','').strip()
    if not key:
        raise HTTPException(503,'DOUBLETICK_API_KEY is not configured on the backend.')
    content_type=(file.content_type or '').lower()
    if content_type not in ('image/jpeg','image/png') and not filename.lower().endswith(('.jpg','.jpeg','.png')):
        raise HTTPException(400,'Please upload a JPG or PNG image.')
    data=await file.read(5*1024*1024+1)
    if not data:
        raise HTTPException(400,'Selected image is empty.')
    if len(data)>5*1024*1024:
        raise HTTPException(413,'Image must be 5 MB or smaller.')
    if content_type not in ('image/jpeg','image/png'):
        content_type='image/png' if filename.lower().endswith('.png') else 'image/jpeg'
    try:
        resp=requests.post('https://public.doubletick.io/media/upload',
            headers={'Authorization':key,'Accept':'application/json'},
            files={'file':(filename,data,content_type)},timeout=45)
    except requests.RequestException as exc:
        raise HTTPException(502,'DoubleTick media upload connection failed: '+str(exc))
    try:
        body=resp.json()
    except Exception:
        body={'raw':resp.text[:1000]}
    if not resp.ok:
        raise HTTPException(resp.status_code,f'DoubleTick media upload failed: {body}')
    media_url=body.get('mediaUrl') or body.get('url')
    if not media_url:
        raise HTTPException(502,f'DoubleTick upload response did not contain mediaUrl: {body}')
    return {'ok':True,'media_url':media_url,'expires_in':body.get('expiresIn'),'filename':filename}

@app.post('/api/templates')
def save_template(x:TemplateIn,u=Depends(current_user)):
    if x.kind not in ('approved','custom'): raise HTTPException(400,'Template kind must be approved or custom')
    if not x.name.strip() or not x.content.strip(): raise HTTPException(400,'Template name and content are required')
    if len(x.content.strip())>1024: raise HTTPException(400,'WhatsApp template body must be 1024 characters or fewer')
    if x.language.strip() not in ('en','hi','en_US','hi_IN'): raise HTTPException(400,'Use a supported language code such as en or hi')
    if x.category.upper() not in ('MARKETING','UTILITY'): raise HTTPException(400,'Category must be MARKETING or UTILITY')
    if x.header_media_url and x.header_text.strip(): raise HTTPException(400,'Use either an image header or a text header, not both.')
    if x.header_media_url and not x.header_media_url.startswith('https://'): raise HTTPException(400,'Uploaded image URL must be HTTPS.')
    c=db(); c.execute('INSERT INTO templates(user_id,name,kind,content,template_name,language,category,approval_status,header_text,footer_text,header_media_url) VALUES(?,?,?,?,?,?,?,?,?,?,?)',(u['id'],x.name.strip(),x.kind,x.content.strip(),x.template_name.strip(),x.language.strip(),x.category.upper(),'DRAFT',x.header_text.strip(),x.footer_text.strip(),x.header_media_url.strip())); c.commit(); c.close(); return {'ok':True}


def doubletick_headers():
    key=os.getenv('DOUBLETICK_API_KEY','').strip()
    if not key: raise HTTPException(503,'DOUBLETICK_API_KEY is not configured on the backend.')
    return {'Authorization':key,'Accept':'application/json','Content-Type':'application/json'}

def dt_sender_number():
    raw=os.getenv('DOUBLETICK_WABA_NUMBER','').strip() or os.getenv('DOUBLETICK_SENDER_NUMBER','').strip()
    digits=re.sub(r'\D','',raw)
    if not digits: raise HTTPException(503,'Set DOUBLETICK_WABA_NUMBER (or DOUBLETICK_SENDER_NUMBER) in backend environment variables.')
    return digits

def dt_error(resp):
    try: body=resp.json()
    except Exception: body={'raw':resp.text[:1000]}
    return body

@app.post('/api/templates/{tid}/submit')
def submit_template(tid:int,u=Depends(current_user)):
    c=db(); row=c.execute('SELECT * FROM templates WHERE id=? AND user_id=?',(tid,u['id'])).fetchone()
    if not row: c.close(); raise HTTPException(404,'Template draft not found')
    if row['approval_status'] in ('PENDING','APPROVED'):
        c.close(); raise HTTPException(409,f"Template is already {row['approval_status']}")
    name=(row['template_name'] or row['name']).strip().lower()
    name=re.sub(r'[^a-z0-9_]+','_',name).strip('_')
    if not name: c.close(); raise HTTPException(400,'Template name must contain English letters or numbers')
    if len(row['content'])>1024: c.close(); raise HTTPException(400,'Body must be 1024 characters or fewer')
    if row['header_text'] and len(row['header_text'])>60: c.close(); raise HTTPException(400,'Header must be 60 characters or fewer')
    if row['footer_text'] and len(row['footer_text'])>60: c.close(); raise HTTPException(400,'Footer must be 60 characters or fewer')
    components={'body':{'text':row['content']}}
    if row['header_media_url']: components['header']={'format':'IMAGE','example':{'link':row['header_media_url']}}
    elif row['header_text']: components['header']={'format':'TEXT','text':row['header_text']}
    if row['footer_text']: components['footer']={'text':row['footer_text']}
    payload={'name':name,'language':row['language'] or 'en','category':row['category'] or 'MARKETING','allowCategoryUpdate':True,'wabaNumbers':[dt_sender_number()],'components':components}
    try:
        resp=requests.post('https://public.doubletick.io/template',headers=doubletick_headers(),json=payload,timeout=30)
    except requests.RequestException as exc:
        c.close(); raise HTTPException(502,'DoubleTick template submission connection failed: '+str(exc))
    body=dt_error(resp)
    if not resp.ok:
        c.close(); raise HTTPException(resp.status_code,f'DoubleTick template submission failed: {body}')
    c.execute("UPDATE templates SET template_name=?,approval_status='PENDING',provider_detail=?,submitted_at=CURRENT_TIMESTAMP WHERE id=? AND user_id=?",(name,json.dumps(body,ensure_ascii=False)[:1500],tid,u['id']))
    c.commit(); c.close(); return {'ok':True,'status':'PENDING','template_name':name,'provider_response':body}

def _walk_template_items(value):
    """Normalize the different list wrappers returned by DoubleTick."""
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    if isinstance(value, dict):
        for key in ('templates', 'data', 'results', 'items', 'result', 'payload'):
            child=value.get(key)
            if isinstance(child, (list, dict)):
                found=_walk_template_items(child)
                if found:
                    return found
        if value.get('name') and value.get('status'):
            return [value]
    return []

@app.post('/api/templates/refresh-status')
def refresh_template_status(u=Depends(current_user)):
    c=db()
    rows=c.execute("SELECT id,template_name,language FROM templates WHERE user_id=? AND COALESCE(template_name,'')<>''",(u['id'],)).fetchall()
    if not rows:
        c.close(); return {'ok':True,'updated':0,'matched':0,'message':'No submitted templates to refresh.'}
    try:
        resp=requests.get('https://public.doubletick.io/v2/templates',headers=doubletick_headers(),params={'status':'ALL','allWabaPhoneNumbers':'true'},timeout=30)
    except requests.RequestException as exc:
        c.close(); raise HTTPException(502,'Could not fetch DoubleTick template status: '+str(exc))
    body=dt_error(resp)
    if not resp.ok:
        c.close(); raise HTTPException(resp.status_code,f'DoubleTick template status failed: {body}')
    items=_walk_template_items(body)
    updated=0; matched=0; status_summary={}
    for row in rows:
        tname=str(row['template_name'] or '').strip().casefold()
        tlang=str(row['language'] or 'en').strip().casefold()
        named=[x for x in items if str(x.get('name') or x.get('templateName') or '').strip().casefold()==tname]
        match=next((x for x in named if str(x.get('language') or x.get('templateLanguage') or 'en').strip().casefold()==tlang),None)
        # Some DoubleTick accounts return locale-qualified codes (e.g. en_US) while
        # older saved drafts contain the base code (en). Only use this fallback when unique.
        if not match:
            same_family=[x for x in named if str(x.get('language') or x.get('templateLanguage') or 'en').strip().casefold().split('_')[0].split('-')[0] == tlang.split('_')[0].split('-')[0]]
            if len(same_family)==1: match=same_family[0]
        if match:
            matched+=1
            status=str(match.get('status') or match.get('approvalStatus') or match.get('newStatus') or '').strip().upper()
            if status in ('APPROVED','REJECTED','PENDING','PAUSED'):
                c.execute('UPDATE templates SET approval_status=?,provider_detail=? WHERE id=?',(status,json.dumps(match,ensure_ascii=False)[:1500],row['id']))
                updated+=1; status_summary[status]=status_summary.get(status,0)+1
    c.commit(); c.close()
    return {'ok':True,'updated':updated,'matched':matched,'statuses':status_summary,
            'message':f'DoubleTick returned {len(items)} template(s); matched {matched} of your {len(rows)} saved template(s).'}

@app.delete('/api/templates/{tid}')
def delete_template(tid:int,u=Depends(current_user)):
    c=db(); c.execute('DELETE FROM templates WHERE id=? AND user_id=?',(tid,u['id'])); c.commit(); c.close(); return {'ok':True}
def _find_provider_message_id(value):
    """Find a DoubleTick message ID in proxy response objects."""
    if isinstance(value, dict):
        for key in ('messageId','dtMessageId','message_id','id'):
            candidate=value.get(key)
            if isinstance(candidate,(str,int)) and str(candidate).strip():
                # Avoid mistaking generic template/customer IDs for message IDs.
                if key != 'id' or any(k in value for k in ('status','to','recipient','message')):
                    return str(candidate).strip()
        for child in value.values():
            found=_find_provider_message_id(child)
            if found: return found
    elif isinstance(value, list):
        for child in value:
            found=_find_provider_message_id(child)
            if found: return found
    return ''

def _is_frequency_capping(value):
    """Recognize provider errors that explicitly indicate WhatsApp frequency capping."""
    if isinstance(value, (dict, list)):
        raw = json.dumps(value, ensure_ascii=False, default=str)
    else:
        raw = str(value or '')
    text = raw.lower().replace('-', ' ').replace('_', ' ')
    markers = (
        'frequency capping', 'frequency cap', 'frequency limit',
        'message frequency', 'recipient frequency limit',
        'too many messages to this recipient', 'too many messages to the recipient',
        'recipient has reached the limit', 'recipient reached the limit',
        'healthy ecosystem engagement', '131049', 'marketing message limit',
    )
    return any(marker in text for marker in markers)


def _find_provider_status(value):
    if isinstance(value, dict):
        status=str(value.get('status') or value.get('messageStatus') or '').strip().upper()
        if status in ('FAILED','ENQUEUED','PROCESSING','SENT','DELIVERED','READ'):
            return status, str(value.get('errorMessage') or value.get('failMessage') or value.get('error') or '')
        for child in value.values():
            found=_find_provider_status(child)
            if found[0]: return found
    elif isinstance(value, list):
        for child in value:
            found=_find_provider_status(child)
            if found[0]: return found
    return '', ''

def _doubletick_webhook_auth(request: Request, x_webhook_secret: Optional[str] = None):
    """Validate the configured webhook token without logging the token itself."""
    import logging
    logger = logging.getLogger("uvicorn.error")
    expected = os.getenv('DOUBLETICK_WEBHOOK_SECRET', '').strip()
    if not expected:
        logger.error("DoubleTick webhook secret is not configured")
        raise HTTPException(503, 'DOUBLETICK_WEBHOOK_SECRET is not configured.')
    supplied = (request.query_params.get('token') or x_webhook_secret or '').strip()
    if not supplied or not hmac.compare_digest(supplied, expected):
        logger.warning("DoubleTick webhook authentication failed")
        raise HTTPException(401, 'Invalid webhook token')


def recipient_digits_for_audit(d):
    try:
        v = d.get('to') or d.get('recipient') or d.get('phone') or ''
        return re.sub(r'\D', '', str(v if not isinstance(v, dict) else v.get('phone', '')))
    except Exception:
        return ''


def _dt_audit(event, message_id, phone, raw_status, normalized, detail, payload, result, log_id=None):
    """Record every webhook hit so missing/ignored events can be diagnosed. Never raises."""
    try:
        c = db()
        try:
            text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, default=str)
            c.execute(
                "INSERT INTO doubletick_webhook_events(event_type,provider_message_id,recipient_phone,provider_status,normalized_status,detail,payload_json,matched_log_id,processing_result) VALUES(?,?,?,?,?,?,?,?,?)",
                (event or 'UNKNOWN', message_id or '', phone or '', raw_status or '', normalized or '',
                 str(detail or '')[:1500], text[:6000], log_id, result))
            c.commit()
        finally:
            c.close()
    except Exception:
        import logging
        logging.getLogger("uvicorn.error").exception("DoubleTick webhook audit write failed")



def _recent_same_incoming(c, phone, content, meta_source):
    """True if the same text from the same phone was saved in the last 3 minutes by the
    OTHER webhook format (Meta-raw ids start with 'wamid.', DoubleTick ids do not)."""
    from datetime import datetime, timedelta, timezone
    since=(datetime.now(timezone.utc)-timedelta(seconds=180)).strftime('%Y-%m-%d %H:%M:%S')
    if meta_source:
        sql="SELECT id,media_url FROM chat_messages WHERE phone=? AND direction='IN' AND content=? AND created_at>=? AND provider_message_id NOT LIKE ? LIMIT 1"
    else:
        sql="SELECT id,media_url FROM chat_messages WHERE phone=? AND direction='IN' AND content=? AND created_at>=? AND provider_message_id LIKE ? LIMIT 1"
    return c.execute(sql,(phone,content,since,'wamid.%')).fetchone()

def _meta_message_text(m):
    t=str(m.get('type') or 'text').lower()
    obj=m.get(t) if isinstance(m.get(t),dict) else {}
    if t=='text': return str(obj.get('body') or '')
    if t=='reaction': return ''
    if t=='button': return str(obj.get('text') or obj.get('payload') or '')
    if t=='interactive':
        r=obj.get('button_reply') or obj.get('list_reply') or {}
        return str(r.get('title') or '')
    caption=str(obj.get('caption') or '')
    fn=str(obj.get('filename') or '')
    if fn: return f'📄 {fn}'+('\n'+caption if caption else '')
    return caption

def _handle_meta_raw_payload(payload, logger):
    """Raw Meta/WhatsApp Cloud envelope: {object:'whatsapp_business_account', entry:[{changes:[{value:{...}}]}]}.
    Status entries are ignored (DoubleTick's own status webhook already covers them, and error
    texts in them must never be saved as candidate replies). Real customer messages are saved."""
    saved=skipped=statuses=0
    c=db()
    try:
        for entry in payload.get('entry') or []:
            for ch in (entry.get('changes') or []) if isinstance(entry,dict) else []:
                val=ch.get('value') if isinstance(ch,dict) else None
                if not isinstance(val,dict): continue
                statuses+=len(val.get('statuses') or [])
                names={}
                for ct in val.get('contacts') or []:
                    if isinstance(ct,dict):
                        names[re.sub(r'\D','',str(ct.get('wa_id') or ''))]=str(((ct.get('profile') or {}).get('name')) or '')
                for m in val.get('messages') or []:
                    if not isinstance(m,dict): continue
                    phone=re.sub(r'\D','',str(m.get('from') or ''))
                    mtype=str(m.get('type') or 'text').upper()
                    text=_meta_message_text(m)
                    if mtype=='REACTION' or len(phone)<10: continue
                    if not text: text=f'[{mtype}]'
                    mid=str(m.get('id') or '').strip()
                    if mid and c.execute("SELECT id FROM chat_messages WHERE provider_message_id=? LIMIT 1",(mid,)).fetchone():
                        skipped+=1; continue
                    if _recent_same_incoming(c,phone,text[:5000],True):
                        skipped+=1; continue
                    if DATABASE_URL:
                        owner=c.execute("SELECT user_id,candidate_name FROM logs WHERE RIGHT(REGEXP_REPLACE(COALESCE(phone,''),'[^0-9]','','g'),10)=? ORDER BY id DESC LIMIT 1",(phone[-10:],)).fetchone()
                    else:
                        owner=c.execute("SELECT user_id,candidate_name FROM logs WHERE substr(replace(replace(replace(phone,'+',''),' ',''),'-',''),-10)=? ORDER BY id DESC LIMIT 1",(phone[-10:],)).fetchone()
                    name=str((owner['candidate_name'] if owner and owner['candidate_name'] else '') or names.get(phone) or phone)[:200]
                    c.execute("INSERT INTO chat_messages(user_id,phone,candidate_name,direction,message_type,content,media_url,provider_message_id) VALUES(?,?,?,?,?,?,?,?)",
                              (owner['user_id'] if owner else None,phone,name,'IN',mtype,text[:5000],'',mid))
                    saved+=1
        c.commit()
    except Exception:
        c.rollback(); logger.exception("Meta raw webhook processing failed"); raise HTTPException(500,'Could not process webhook')
    finally:
        c.close()
    _dt_audit('META_RAW','','','','',f'messages_saved={saved} duplicates={skipped} statuses_ignored={statuses}',payload,
              'meta_raw_messages_saved' if saved else ('meta_raw_status_ignored' if statuses else 'meta_raw_nothing'))
    return {'ok':True,'format':'meta_raw','saved':saved,'duplicates':skipped,'statuses_ignored':statuses}

@app.get('/api/webhooks/doubletick')
async def doubletick_webhook_verify(request: Request,
                                    x_webhook_secret: Optional[str] = Header(None)):
    """Support provider URL-verification/health checks that use GET.
    Add &debug=1 to see the last 30 webhook hits (same token required)."""
    _doubletick_webhook_auth(request, x_webhook_secret)
    if request.query_params.get('debug'):
        c = db()
        try:
            rows = c.execute("SELECT id,event_type,recipient_phone,provider_status,processing_result,detail,created_at,payload_json FROM doubletick_webhook_events ORDER BY id DESC LIMIT 30").fetchall()
            inc = c.execute("SELECT COUNT(*) n FROM chat_messages WHERE direction='IN'").fetchone()
        finally:
            c.close()
        events = []
        for r in rows:
            d = dict(r); d['created_at'] = str(d.get('created_at')); d['payload_json'] = str(d.get('payload_json') or '')[:600]
            events.append(d)
        return {'ok': True, 'incoming_messages_in_db': inc['n'] if inc else 0, 'last_events': events}
    return {'ok': True, 'webhook': 'DoubleTick', 'method': 'GET',
            'message': 'Webhook URL is reachable. Send events using POST.'}


def _dt_get_nested(obj, *paths):
    for path in paths:
        value = obj
        for key in path.split('.'):
            if isinstance(value, dict):
                value = value.get(key)
            elif isinstance(value, list) and key.isdigit() and int(key) < len(value):
                value = value[int(key)]
            else:
                value = None
                break
        if value not in (None, ''):
            return value
    return None


@app.post('/api/webhooks/doubletick')
async def doubletick_webhook(request: Request, x_webhook_secret: Optional[str] = Header(None)):
    """Receive DoubleTick webhooks; accept JSON and form-encoded provider payloads."""
    import logging
    from urllib.parse import parse_qs
    logger = logging.getLogger("uvicorn.error")
    _doubletick_webhook_auth(request, x_webhook_secret)

    raw_body = await request.body()
    content_type = request.headers.get('content-type', '').lower()
    payload = None
    if raw_body:
        try:
            payload = json.loads(raw_body.decode('utf-8-sig'))
        except Exception as json_exc:
            # Some webhook senders encode the event as form data instead of JSON.
            try:
                decoded = raw_body.decode('utf-8-sig')
                if 'application/x-www-form-urlencoded' in content_type or '=' in decoded:
                    form = parse_qs(decoded, keep_blank_values=True)
                    payload = {k: (v[-1] if v else '') for k, v in form.items()}
                    for key in ('data', 'payload', 'message', 'body'):
                        value = payload.get(key)
                        if isinstance(value, str) and value.strip().startswith(('{', '[')):
                            try: payload[key] = json.loads(value)
                            except Exception: pass
                else:
                    logger.error("DoubleTick webhook invalid body: content_type=%s bytes=%s preview=%r",
                                 content_type, len(raw_body), decoded[:300])
                    raise HTTPException(400, 'Webhook body must be valid JSON or form data')
            except HTTPException:
                raise
            except Exception:
                logger.error("DoubleTick webhook body could not be parsed: content_type=%s bytes=%s error=%s",
                             content_type, len(raw_body), str(json_exc))
                raise HTTPException(400, 'Webhook body must be valid JSON or form data')
    else:
        logger.warning("DoubleTick webhook received an empty POST body; content_type=%s", content_type)
        _dt_audit('EMPTY_BODY','','','','','Empty POST body; content_type='+content_type,'', 'ignored_empty')
        return {'ok': True, 'ignored': True, 'reason': 'Empty webhook body'}

    if isinstance(payload, list):
        if len(payload) == 1 and isinstance(payload[0], dict):
            payload = payload[0]
        else:
            logger.info("DoubleTick webhook acknowledged list payload (%s items)", len(payload))
            _dt_audit('LIST_PAYLOAD','','','','','',raw_body.decode('utf-8','replace'),'ignored_list')
            return {'ok': True, 'ignored': True, 'reason': 'Unsupported list payload'}
    if not isinstance(payload, dict):
        logger.info("DoubleTick webhook acknowledged non-object payload")
        _dt_audit('NON_OBJECT','','','','','',raw_body.decode('utf-8','replace'),'ignored_non_object')
        return {'ok': True, 'ignored': True, 'reason': 'Payload is not an object'}

    if payload.get('object') == 'whatsapp_business_account' and isinstance(payload.get('entry'), list):
        return _handle_meta_raw_payload(payload, logger)

    # Unwrap nested provider envelopes. DoubleTick integrations can deliver
    # the event directly or inside data/payload/body/eventData.
    event_data = payload
    for _ in range(5):
        if not isinstance(event_data, dict):
            break
        nested_found = None
        for wrapper_key in ('data', 'payload', 'body', 'eventData', 'event_data'):
            nested = event_data.get(wrapper_key)
            if isinstance(nested, str):
                try:
                    nested = json.loads(nested)
                except Exception:
                    continue
            if isinstance(nested, dict):
                event_data = nested
                nested_found = nested
                break
            if isinstance(nested, list) and nested and isinstance(nested[0], dict):
                event_data = nested[0]
                nested_found = event_data
                break
        if nested_found is None:
            break

    def _walk_dicts(value):
        """Yield dictionaries at every nesting level without assuming a fixed schema."""
        if isinstance(value, dict):
            yield value
            for child in value.values():
                yield from _walk_dicts(child)
        elif isinstance(value, list):
            for child in value:
                yield from _walk_dicts(child)

    all_objects = list(_walk_dicts(event_data))
    def _first_value(keys):
        for obj in all_objects:
            for key in keys:
                value = obj.get(key)
                if value not in (None, ''):
                    return value
        return None

    event = str(_first_value(('event', 'eventType', 'event_type', 'webhookEvent')) or '').strip().upper()
    logger.info("DoubleTick webhook received: event=%s content_type=%s top_keys=%s event_keys=%s",
                event or 'UNKNOWN', content_type, list(payload.keys())[:30],
                list(event_data.keys())[:30])

    # Find the actual message object, including messages[0] and nested data shapes.
    msg = None
    for obj in all_objects:
        candidate = obj.get('message')
        if isinstance(candidate, list):
            candidate = next((x for x in candidate if isinstance(x, dict)), None)
        if isinstance(candidate, dict):
            msg = candidate
            break
        if isinstance(candidate, str) and candidate.strip():
            msg = {'text': candidate}
            break
    if msg is None:
        msg = event_data if isinstance(event_data, dict) else {}

    contact = next((o for o in all_objects if isinstance(o.get('contact'), dict)), {})
    customer_obj = next((o for o in all_objects if isinstance(o.get('customer'), dict)), {})
    sender_obj = next((o for o in all_objects if isinstance(o.get('sender'), dict)), {})

    phone_value = _first_value((
        'from', 'customerPhone', 'customer_phone', 'phone', 'waId', 'wa_id',
        'senderPhone', 'sender_phone', 'mobile', 'mobileNumber', 'phoneNumber',
        'phone_number', 'recipientPhone'
    )) or ''
    if isinstance(phone_value, dict):
        phone_value = (phone_value.get('phone') or phone_value.get('wa_id') or
                       phone_value.get('number') or phone_value.get('id') or '')
    phone_digits = re.sub(r'\D', '', str(phone_value))

    text_value = (msg.get('text') or msg.get('caption') or msg.get('body') or
                  msg.get('messageText') or msg.get('message') or
                  _first_value(('messageText', 'text', 'caption', 'body', 'textBody')) or '')
    if isinstance(text_value, dict):
        text_value = text_value.get('body') or text_value.get('text') or json.dumps(text_value, ensure_ascii=False)
    if isinstance(text_value, list):
        text_value = json.dumps(text_value, ensure_ascii=False)

    media_url = str(msg.get('url') or msg.get('mediaUrl') or msg.get('media_url') or
                    msg.get('link') or _first_value(('mediaUrl', 'media_url', 'mediaLink', 'media_link', 'fileUrl')) or '')
    doc_obj = msg.get('document') if isinstance(msg.get('document'), dict) else {}
    if not media_url:
        media_url = str(doc_obj.get('url') or doc_obj.get('link') or '')
    if not media_url and str(msg.get('type') or msg.get('messageType') or 'TEXT').upper() not in ('TEXT', 'BUTTON', 'INTERACTIVE', 'TEMPLATE'):
        def _scan_url(o, depth=0):
            if depth > 4: return ''
            if isinstance(o, dict):
                for k, v in o.items():
                    if k in ('text', 'body', 'caption', 'contact', 'customer', 'sender', 'senderUser', 'context'): continue
                    r = _scan_url(v, depth + 1)
                    if r: return r
            elif isinstance(o, list):
                for v in o:
                    r = _scan_url(v, depth + 1)
                    if r: return r
            elif isinstance(o, str) and o.startswith(('http://', 'https://')):
                return o
            return ''
        media_url = _scan_url(msg)
    file_name = str(msg.get('fileName') or msg.get('filename') or msg.get('file_name') or
                    doc_obj.get('filename') or doc_obj.get('fileName') or '').strip()
    message_type = str(msg.get('type') or msg.get('messageType') or
                       _first_value(('messageType', 'message_type')) or 'TEXT').upper()
    direction = str(_first_value(('direction', 'messageDirection', 'message_direction')) or '').upper()
    incoming_event = any(term in event for term in
                         ('MESSAGE_RECEIVED', 'INCOMING', 'INBOUND', 'MESSAGE_IN',
                          'MESSAGES.UPSERT', 'MESSAGE.NEW', 'NEW_MESSAGE', 'RECEIVED'))
    # DoubleTick's CleverTap Incoming Messages webhook uses a flat payload:
    # {from, wabaNumber, messageId, timestamp, type, text:{body}} and may not
    # include an event/status field. Recognize this provider signature directly.
    clevertap_incoming = bool(
        event_data.get('from') and event_data.get('wabaNumber') and
        event_data.get('messageId') and
        (isinstance(event_data.get('text'), dict) or isinstance(event_data.get('text'), str))
    )
    is_incoming = incoming_event or clevertap_incoming or direction in ('IN', 'INBOUND', 'INCOMING', 'RECEIVED') or (
        bool(phone_digits) and bool(text_value or media_url) and
        not any(term in event for term in ('STATUS', 'DELIVERED', 'READ', 'FAILED', 'TEMPLATE', 'SENT'))
    )

    if is_incoming:
        if not phone_digits:
            logger.warning("DoubleTick incoming event has no sender phone; keys=%s",
                           list(event_data.keys())[:30])
            _dt_audit(event or 'MESSAGE_RECEIVED','','','','','Sender phone missing',payload,'ignored_no_phone')
            return {'ok': True, 'ignored': True, 'reason': 'Sender phone missing'}
        candidate_name = str(contact.get('name') or customer_obj.get('name') or
                             _first_value(('customerName', 'customer_name', 'profileName', 'pushName', 'name')) or
                             phone_digits)[:200]
        if file_name:
            text_value = f'📄 {file_name}' + (f'\n{text_value}' if text_value else '')
        if not text_value:
            text_value = f'[{message_type}]' if media_url else f'[{message_type} message]'
        message_id = str(msg.get('dtMessageId') or msg.get('messageId') or msg.get('id') or
                         _first_value(('dtMessageId', 'messageId', 'message_id', 'wamid')) or '').strip()

        c = db()
        try:
            if DATABASE_URL:
                owner = c.execute(
                    "SELECT user_id,candidate_name FROM logs "
                    "WHERE RIGHT(REGEXP_REPLACE(COALESCE(phone,''),'[^0-9]','','g'),10)=? "
                    "ORDER BY id DESC LIMIT 1", (phone_digits[-10:],)
                ).fetchone()
            else:
                owner = c.execute(
                    "SELECT user_id,candidate_name FROM logs "
                    "WHERE substr(replace(replace(replace(phone,'+',''),' ',''),'-',''),-10)=? "
                    "ORDER BY id DESC LIMIT 1", (phone_digits[-10:],)
                ).fetchone()
            user_id = owner['user_id'] if owner else None
            if owner and owner['candidate_name']:
                candidate_name = str(owner['candidate_name'])[:200]
            duplicate = None
            if message_id:
                duplicate = c.execute(
                    "SELECT id FROM chat_messages WHERE provider_message_id=? "
                    "AND provider_message_id<>'' LIMIT 1", (message_id,)
                ).fetchone()
            if not duplicate:
                twin = _recent_same_incoming(c, phone_digits, str(text_value)[:5000], False)
                if twin:
                    # The Meta-raw copy has no download link; the DoubleTick copy does. Keep one row, add the link.
                    if media_url and not (twin['media_url'] or ''):
                        c.execute("UPDATE chat_messages SET media_url=?,message_type=? WHERE id=?",
                                  (media_url[:2000], message_type, twin['id']))
                        c.commit()
                    duplicate = {'id': twin['id']}
            if not duplicate:
                c.execute(
                    "INSERT INTO chat_messages(user_id,phone,candidate_name,direction,"
                    "message_type,content,media_url,provider_message_id) VALUES(?,?,?,?,?,?,?,?)",
                    (user_id, phone_digits, candidate_name, 'IN', message_type,
                     str(text_value)[:5000], media_url[:2000], message_id)
                )
                c.commit()
            _dt_audit(event or 'MESSAGE_RECEIVED', message_id, phone_digits, '', 'IN',
                      'assigned_user_id=%s' % user_id, payload,
                      'incoming_duplicate' if duplicate else ('incoming_saved_assigned' if user_id else 'incoming_saved_unassigned_admin_only'))
            logger.info("DoubleTick incoming saved=%s assigned=%s type=%s phone_last4=%s",
                        not bool(duplicate), bool(user_id), message_type, phone_digits[-4:])
            return {'ok': True, 'event': 'MESSAGE_RECEIVED',
                    'saved': not bool(duplicate), 'assigned': bool(user_id)}
        except Exception:
            c.rollback()
            logger.exception("DoubleTick incoming message database processing failed")
            raise HTTPException(500, 'Could not save incoming WhatsApp message')
        finally:
            c.close()

    # Template approval events.
    if event in ('TEMPLATE_UPDATE', 'TEMPLATE_STATUS_UPDATE') or event_data.get('newStatus'):
        template_name = str(event_data.get('templateName') or event_data.get('name') or '').strip()
        language = str(event_data.get('templateLanguage') or event_data.get('language') or 'en').strip()
        status = str(event_data.get('newStatus') or event_data.get('status') or '').strip().upper()
        if not template_name or status not in ('APPROVED', 'REJECTED', 'PENDING', 'PAUSED'):
            logger.info("DoubleTick incomplete template event acknowledged: keys=%s",
                        list(event_data.keys())[:30])
            return {'ok': True, 'ignored': True, 'reason': 'Incomplete template status event'}
        c = db()
        try:
            c.execute(
                "UPDATE templates SET approval_status=?,provider_detail=? "
                "WHERE LOWER(template_name)=LOWER(?) AND LOWER(language)=LOWER(?)",
                (status, json.dumps(event_data, ensure_ascii=False)[:1500], template_name, language)
            )
            c.commit()
        finally:
            c.close()
        return {'ok': True, 'event': 'TEMPLATE_STATUS_UPDATE',
                'template_name': template_name, 'language': language, 'status': status}

    # Outbound delivery/read/failure status events. Persist every event so an
    # unmatched webhook is never silently lost after returning HTTP 200.
    message_id = str(_first_value(('dtMessageId', 'messageId', 'message_id', 'wamid', 'providerMessageId')) or '').strip()
    raw_status = str(_first_value(('status', 'messageStatus', 'deliveryStatus', 'message_status')) or '').strip().upper()
    if not raw_status:
        raw_status = {'MESSAGE_SENT': 'SENT', 'MESSAGE_DELIVERED': 'DELIVERED',
                      'MESSAGE_READ': 'READ', 'MESSAGE_FAILED': 'FAILED'}.get(event, '')
    is_status_event = event in ('MESSAGE_STATUS_UPDATE', 'MESSAGE_STATUS',
                                'MESSAGE_SENT', 'MESSAGE_DELIVERED', 'MESSAGE_READ', 'MESSAGE_FAILED',
                                'MESSAGE_STATUS_CHANGED', 'MESSAGES.STATUS')
    if is_status_event or (not is_incoming and (message_id or raw_status)):
        status_aliases = {'DELIVERED (DOUBLETICK)': 'DELIVERED', 'DELIVERED_DOUBLETICK': 'DELIVERED',
                          'MESSAGE_SENT': 'SENT', 'MESSAGE_DELIVERED': 'DELIVERED',
                          'MESSAGE_READ': 'READ', 'MESSAGE_FAILED': 'FAILED'}
        status = status_aliases.get(raw_status, raw_status)
        if status not in ('SENT', 'DELIVERED', 'READ', 'FAILED', 'ENQUEUED', 'PROCESSING'):
            logger.info("DoubleTick status event unsupported: event=%s status=%s message_id_present=%s",
                        event, raw_status or 'MISSING', bool(message_id))
            _dt_audit(event, message_id, recipient_digits_for_audit(event_data), raw_status, '', 'Unsupported or missing status', payload, 'ignored_unsupported_status')
            return {'ok': True, 'ignored': True, 'reason': 'Unsupported or missing status', 'provider_status': raw_status}

        recipient_value = _first_value(('to', 'recipient', 'recipientPhone', 'recipient_phone',
                                        'customerPhone', 'customer_phone', 'phoneNumber', 'phone_number', 'mobile')) or ''
        if isinstance(recipient_value, dict):
            recipient_value = recipient_value.get('phone') or recipient_value.get('wa_id') or recipient_value.get('number') or recipient_value.get('id') or ''
        recipient = re.sub(r'\D', '', str(recipient_value))
        failure_detail = _first_value(('failMessage', 'errorMessage', 'error_description',
                                       'failureReason', 'reason', 'error', 'description'))
        detail = str(failure_detail or f'DoubleTick status update: {status}')[:1500]
        frequency_capping = _is_frequency_capping(event_data) or _is_frequency_capping(detail)
        if status == 'FAILED' and frequency_capping:
            status = 'FREQUENCY_CAPPING'
            if not failure_detail:
                detail = 'Message not delivered due to frequency capping (Meta error 131049).'

        # Provider payloads vary: preserve useful candidate/template metadata for
        # fallback rows and for support diagnostics.
        candidate_name = str(_first_value(('customerName', 'customer_name', 'candidateName', 'candidate_name', 'profileName', 'name')) or '').strip()[:200]
        template_value = _first_value(('templateName', 'template_name', 'nameOfTemplate'))
        if not template_value:
            template_obj = _first_value(('templateMessage', 'template'))
            if isinstance(template_obj, dict):
                template_value = template_obj.get('name') or template_obj.get('templateName')
        template_name = str(template_value or 'DoubleTick webhook status')[:200]
        c = db()
        target = None
        result_kind = 'unmatched'
        try:
            # Exact provider message ID is always the strongest key.
            if message_id:
                target = c.execute(
                    'SELECT id,user_id,candidate_name,phone,status FROM logs WHERE provider_message_id=? ORDER BY id DESC LIMIT 1',
                    (message_id,)
                ).fetchone()

            # DoubleTick can send a different ID in the status callback than the
            # initial send response. Match the most recent record for this phone,
            # preferring an attempt that is still awaiting a final status.
            if not target and recipient and len(recipient) >= 10:
                candidates = c.execute(
                    "SELECT id,user_id,candidate_name,phone,status,created_at FROM logs ORDER BY id DESC LIMIT 5000"
                ).fetchall()
                same_phone = []
                for candidate in candidates:
                    digits = re.sub(r'\D', '', str(candidate['phone'] or ''))
                    if digits and digits[-10:] == recipient[-10:]:
                        same_phone.append(candidate)
                pending = next((x for x in same_phone if str(x['status'] or '').upper() in ('PROCESSING','ENQUEUED','UNKNOWN','SENT')), None)
                target = pending or (same_phone[0] if same_phone else None)

            # Do not downgrade a confirmed delivery/read due to an older or
            # duplicated failure callback. Frequency capping can replace queued
            # or sent states, but never a confirmed DELIVERED/READ state.
            saved_status = status
            if target:
                old_status = str(target['status'] or '').upper()
                if old_status in ('DELIVERED', 'READ') and status in ('FAILED', 'FREQUENCY_CAPPING', 'ENQUEUED', 'PROCESSING', 'SENT'):
                    saved_status = old_status
                    result_kind = 'matched_ignored_downgrade'
                else:
                    c.execute(
                        'UPDATE logs SET status=?,detail=?,provider_message_id=CASE WHEN COALESCE(provider_message_id,\'\')=\'\' AND ?<>\'\' THEN ? ELSE provider_message_id END,status_updated_at=CURRENT_TIMESTAMP WHERE id=?',
                        (saved_status, detail, message_id, message_id, target['id'])
                    )
                    result_kind = 'matched_updated'
                log_id = target['id']
            else:
                log_id = None
                # If the original attempt cannot be found, create a visible
                # reconciliation row rather than returning 200 and losing the
                # recipient/status. Assign it to the phone's latest owner; if no
                # owner exists, assign it to the first admin account.
                if recipient and len(recipient) >= 10:
                    owner = None
                    for candidate in c.execute("SELECT user_id,phone,candidate_name FROM logs ORDER BY id DESC LIMIT 5000").fetchall():
                        digits = re.sub(r'\D', '', str(candidate['phone'] or ''))
                        if digits and digits[-10:] == recipient[-10:]:
                            owner = candidate
                            break
                    if owner:
                        owner_id = owner['user_id']
                        if not candidate_name:
                            candidate_name = str(owner['candidate_name'] or '')[:200]
                    else:
                        admin_row = c.execute("SELECT id FROM users WHERE role='admin' ORDER BY id LIMIT 1").fetchone()
                        fallback_user = admin_row or c.execute("SELECT id FROM users ORDER BY id LIMIT 1").fetchone()
                        owner_id = fallback_user['id'] if fallback_user else None
                    if owner_id is not None:
                        cur = c.execute(
                            "INSERT INTO logs(user_id,candidate_name,phone,kind,template_name,status,detail,provider_message_id,status_updated_at) VALUES(?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)",
                            (owner_id, candidate_name or recipient, recipient, 'webhook_reconciled', template_name, status, detail, message_id)
                        )
                        log_id = cur.lastrowid
                        result_kind = 'reconciliation_row_created'

            # Store event audit record. Payload is bounded to keep DB rows sane.
            payload_text = json.dumps(payload, ensure_ascii=False, default=str)[:12000]
            c.execute(
                "INSERT INTO doubletick_webhook_events(event_type,provider_message_id,recipient_phone,provider_status,normalized_status,detail,payload_json,matched_log_id,processing_result) VALUES(?,?,?,?,?,?,?,?,?)",
                (event or 'UNKNOWN', message_id, recipient, raw_status, saved_status if target else status,
                 detail, payload_text, log_id, result_kind)
            )
            c.commit()
            logger.info("DoubleTick status processed event=%s provider_status=%s saved_status=%s result=%s log_id=%s message_id_present=%s recipient_last4=%s",
                        event or 'UNKNOWN', raw_status, saved_status if target else status,
                        result_kind, log_id, bool(message_id), recipient[-4:])
        except Exception:
            c.rollback()
            logger.exception("DoubleTick status event database processing failed event=%s status=%s message_id_present=%s recipient_last4=%s",
                             event or 'UNKNOWN', raw_status, bool(message_id), recipient[-4:])
            raise HTTPException(500, 'Could not persist DoubleTick status event')
        finally:
            c.close()
        return {'ok': True, 'event': 'MESSAGE_STATUS_UPDATE', 'matched': bool(target),
                'saved_record': bool(log_id), 'result': result_kind, 'status': saved_status if target else status,
                'provider_status': raw_status, 'message_id': message_id,
                'recipient_last4': recipient[-4:]}

    logger.info("DoubleTick unsupported event acknowledged: event=%s keys=%s",
                event or 'UNKNOWN', list(event_data.keys())[:30])
    _dt_audit(event or 'UNKNOWN', '', '', '', '', 'Unsupported event', payload, 'ignored_unsupported_event')
    return {'ok': True, 'ignored': True, 'event': event or 'UNKNOWN'}

@app.post('/api/send')
def send(x:SendIn,u=Depends(current_user)):
    if not x.candidate_ids: raise HTTPException(400,'Select at least one candidate')
    if x.kind not in ('approved','custom'): raise HTTPException(400,'Invalid message type')
    if x.kind=='custom':
        raise HTTPException(501,'Custom message sending is not supported by the supplied proxy endpoint. Outside the 24-hour window, use an approved WhatsApp template.')
    if not x.template_name.strip(): raise HTTPException(400,'Approved DoubleTick template name is required')
    all_rows=records(); selected=set(x.candidate_ids); found=[]
    for r in all_rows:
        rr=row_for(r)
        if rr['id'] in selected: found.append(r)
    if len(found)!=len(selected): raise HTTPException(400,'Some selected candidates are no longer in the configured Airtable views. Refresh the list.')
    c=db()
    try:
        limit=int(get_setting('max_messages_per_user_per_day','50'))
        if u['role']!='admin':
            # Count every attempt (including failed sends) to prevent retries bypassing the cap.
            used=c.execute("SELECT COUNT(*) n FROM logs WHERE user_id=? AND date(created_at,'+5 hours','+30 minutes')=date('now','+5 hours','+30 minutes')",(u['id'],)).fetchone()['n']
            remaining=max(0,limit-used)
            if len(found)>remaining:
                c.close(); raise HTTPException(429,f'Daily WhatsApp limit reached. You can attempt {remaining} more message(s) today; your limit is {limit} per day.')
        # Reserve all attempts in one write transaction so simultaneous requests cannot exceed the cap.
        c.execute('BEGIN IMMEDIATE')
        if DATABASE_URL and u['role']!='admin':
            c.execute('SELECT pg_advisory_xact_lock(?)',(int(u['id']),))
        if u['role']!='admin':
            used=c.execute("SELECT COUNT(*) n FROM logs WHERE user_id=? AND date(created_at,'+5 hours','+30 minutes')=date('now','+5 hours','+30 minutes')",(u['id'],)).fetchone()['n']
            remaining=max(0,limit-used)
            if len(found)>remaining:
                c.rollback(); c.close(); raise HTTPException(429,f'Daily WhatsApp limit reached. You can attempt {remaining} more message(s) today; your limit is {limit} per day.')
        ids=[]
        for r in found:
            f=candidate_fields(r)
            cur=c.execute("INSERT INTO logs(user_id,candidate_name,phone,kind,template_name,status,detail) VALUES(?,?,?,?,?,'PROCESSING','Reserved for sending')",(u['id'],str(f.get('name') or ''),str(f.get('phone') or ''),'approved',x.template_name.strip()))
            ids.append(cur.lastrowid)
        c.commit()
    except HTTPException:
        raise
    except Exception:
        c.close(); raise
    results=[]
    for r,log_id in zip(found,ids):
        f=candidate_fields(r); status='FAILED'; detail=''; provider_id=''
        try:
            response=send_doubletick_candidate_message(r,x.template_name.strip(),x.language.strip() or 'en')
            provider_id=_find_provider_message_id(response)
            provider_status,provider_error=_find_provider_status(response)
            if provider_status=='FAILED':
                detail=provider_error or json.dumps(response,ensure_ascii=False)[:1500]
                status='FREQUENCY_CAPPING' if _is_frequency_capping(response) else 'FAILED'
            elif provider_status in ('SENT','DELIVERED','READ','ENQUEUED'):
                status=provider_status; detail=json.dumps(response,ensure_ascii=False)[:1500]
            else:
                # HTTP success from the proxy means accepted, not necessarily delivered.
                status='ENQUEUED'; detail=json.dumps(response,ensure_ascii=False)[:1500]
        except Exception as e:
            detail=str(e)[:1500]
            if _is_frequency_capping(detail):
                status='FREQUENCY_CAPPING'
        # Commit each candidate immediately. A later timeout/restart must not roll back earlier results.
        current=c.execute('SELECT status FROM logs WHERE id=?',(log_id,)).fetchone()
        current_status=str(current['status'] or '') if current else ''
        final_states=('SENT','DELIVERED','READ','FAILED','FREQUENCY_CAPPING')
        if current_status not in final_states:
            c.execute('UPDATE logs SET status=?,detail=?,provider_message_id=?,status_updated_at=CURRENT_TIMESTAMP WHERE id=?',(status,detail,provider_id,log_id))
        elif provider_id:
            c.execute('UPDATE logs SET provider_message_id=? WHERE id=?',(provider_id,log_id))
        c.commit()
        results.append({'name':str(f.get('name') or ''),'status':current_status if current_status in final_states else status,'detail':detail,'message_id':provider_id})
    c.close()
    return {'ok':True,'summary':{'total':len(results),'sent':sum(z['status'] in ('SENT','DELIVERED','READ','ENQUEUED') for z in results),'failed':sum(z['status']=='FAILED' for z in results),'frequency_capping':sum(z['status']=='FREQUENCY_CAPPING' for z in results),'enqueued':sum(z['status']=='ENQUEUED' for z in results)},'results':results}


def _reply_e164(value):
    digits=re.sub(r'\D','',str(value or ''))
    if len(digits)==10: digits='91'+digits
    return '+'+digits if 10<=len(digits)<=15 else ''

@app.post('/api/chats/{phone}/reply')
def send_chat_reply(phone:str, x:ChatReplyIn, u=Depends(current_user)):
    """Send a free-form WhatsApp reply through DoubleTick (24-hour session only)."""
    digits=re.sub(r'\D','',phone)
    message=x.message.strip()
    if not digits or not message: raise HTTPException(400,'Phone number and reply message are required.')
    api_key=os.getenv('DOUBLETICK_API_KEY','').strip()
    sender=_reply_e164(os.getenv('DOUBLETICK_SENDER_NUMBER') or os.getenv('DOUBLETICK_WABA_NUMBER') or '')
    recipient=_reply_e164(digits)
    if not api_key: raise HTTPException(503,'DOUBLETICK_API_KEY is not configured on the server.')
    if not sender: raise HTTPException(503,'Set DOUBLETICK_SENDER_NUMBER (your WhatsApp sender number) in Render.')
    if not recipient: raise HTTPException(400,'Candidate phone number must be a valid international number.')
    c=db()
    if u['role']!='admin':
        if DATABASE_URL:
            allowed=c.execute("SELECT 1 FROM logs WHERE user_id=? AND RIGHT(REGEXP_REPLACE(COALESCE(phone,''),'[^0-9]','','g'),10)=? LIMIT 1",(u['id'],digits[-10:])).fetchone()
        else:
            allowed=c.execute("SELECT 1 FROM logs WHERE user_id=? AND substr(replace(replace(replace(phone,'+',''),' ',''),'-',''),-10)=? LIMIT 1",(u['id'],digits[-10:])).fetchone()
        if not allowed:
            c.close(); raise HTTPException(403,'This conversation is not assigned to your HR account.')
    msg_id=str(uuid.uuid4())
    payload={'to':recipient,'from':sender,'messageId':msg_id,'content':{'text':message}}
    try:
        resp=requests.post('https://public.doubletick.io/whatsapp/message/text',
            headers={'Authorization':api_key,'Content-Type':'application/json','Accept':'application/json'},json=payload,timeout=30)
        try: body=resp.json()
        except Exception: body={'raw':resp.text[:1500]}
    except requests.RequestException as exc:
        c.close(); raise HTTPException(502,f'DoubleTick connection failed: {exc}')
    if resp.status_code not in (200,201,202):
        c.close(); detail=body.get('message') or body.get('error') or body
        raise HTTPException(resp.status_code if 400<=resp.status_code<500 else 502,
            f'DoubleTick rejected reply. Free-form replies require an active 24-hour customer service window. Provider response: {detail}')
    provider_id=_find_provider_message_id(body) or msg_id
    if DATABASE_URL:
        name_row=c.execute("SELECT candidate_name FROM logs WHERE RIGHT(REGEXP_REPLACE(COALESCE(phone,''),'[^0-9]','','g'),10)=? ORDER BY id DESC LIMIT 1",(digits[-10:],)).fetchone()
    else:
        name_row=c.execute("SELECT candidate_name FROM logs WHERE substr(replace(replace(replace(phone,'+',''),' ',''),'-',''),-10)=? ORDER BY id DESC LIMIT 1",(digits[-10:],)).fetchone()
    candidate_name=name_row['candidate_name'] if name_row else ''
    c.execute("INSERT INTO chat_messages(user_id,phone,candidate_name,direction,message_type,content,media_url,provider_message_id) VALUES(?,?,?,?,?,?,?,?)",
        (u['id'],digits,candidate_name,'OUT','TEXT',message,'',provider_id))
    c.commit(); c.close()
    return {'ok':True,'status':'SENT','message_id':provider_id,'provider':body}


def _chat_phone_allowed(c,digits,u):
    if u['role']=='admin': return True
    if DATABASE_URL:
        row=c.execute("SELECT 1 FROM logs WHERE user_id=? AND RIGHT(REGEXP_REPLACE(COALESCE(phone,''),'[^0-9]','','g'),10)=? LIMIT 1",(u['id'],digits[-10:])).fetchone()
    else:
        row=c.execute("SELECT 1 FROM logs WHERE user_id=? AND substr(replace(replace(replace(phone,'+',''),' ',''),'-',''),-10)=? LIMIT 1",(u['id'],digits[-10:])).fetchone()
    return bool(row)

@app.post('/api/chats/{phone}/read')
def mark_chat_read(phone:str,u=Depends(current_user)):
    digits=re.sub(r'\D','',phone)
    if not digits: raise HTTPException(400,'Invalid phone number')
    c=db()
    try:
        if not _chat_phone_allowed(c,digits,u): raise HTTPException(403,'This conversation is not assigned to your HR account.')
        if u['role']=='admin': c.execute("UPDATE chat_messages SET is_read=1 WHERE phone=? AND direction='IN'",(digits,))
        else: c.execute("UPDATE chat_messages SET is_read=1 WHERE phone=? AND user_id=? AND direction='IN'",(digits,u['id']))
        c.commit(); return {'ok':True,'marked_read':True}
    finally: c.close()

@app.get('/api/admin/internal-notes')
def admin_all_internal_notes(q:str='',hr_id:int=0,limit:int=500,u=Depends(current_user)):
    if u['role']!='admin':
        raise HTTPException(403,'Admin access required.')
    limit=max(1,min(int(limit or 500),1000))
    c=db()
    try:
        where=[]; params=[]
        if hr_id>0:
            where.append('n.user_id=?'); params.append(hr_id)
        if q.strip():
            term='%'+q.strip()+'%'
            where.append("(n.phone LIKE ? OR n.note LIKE ? OR COALESCE(us.username,'') LIKE ? OR COALESCE((SELECT MAX(cm.candidate_name) FROM chat_messages cm WHERE cm.phone=n.phone),'') LIKE ?)")
            params.extend([term,term,term,term])
        clause=(' WHERE '+' AND '.join(where)) if where else ''
        rows=c.execute("SELECT n.id,n.user_id,n.phone,n.note,n.created_at,COALESCE(us.username,'Unknown') username,COALESCE((SELECT MAX(cm.candidate_name) FROM chat_messages cm WHERE cm.phone=n.phone),'') candidate_name FROM internal_notes n LEFT JOIN users us ON us.id=n.user_id"+clause+" ORDER BY n.id DESC LIMIT ?",tuple(params+[limit])).fetchall()
        hrs=c.execute("SELECT id,username FROM users WHERE role='hr' ORDER BY username").fetchall()
        return {'notes':[dict(x) for x in rows],'hr_users':[dict(x) for x in hrs],'total':len(rows)}
    finally:
        c.close()


@app.get('/api/chats/{phone}/notes')
def get_internal_notes(phone:str,u=Depends(current_user)):
    digits=re.sub(r'\D','',phone)
    if not digits: raise HTTPException(400,'Invalid phone number')
    c=db()
    try:
        if not _chat_phone_allowed(c,digits,u): raise HTTPException(403,'This conversation is not assigned to your HR account.')
        rows=c.execute("SELECT n.id,n.user_id,n.phone,n.note,n.created_at,COALESCE(us.username,'Unknown') username FROM internal_notes n LEFT JOIN users us ON us.id=n.user_id WHERE n.phone=? ORDER BY n.id DESC LIMIT 200",(digits,)).fetchall()
        return [dict(x) for x in rows]
    finally: c.close()

@app.post('/api/chats/{phone}/notes')
async def add_internal_note(phone:str,request:Request,u=Depends(current_user)):
    digits=re.sub(r'\D','',phone)
    if not digits: raise HTTPException(400,'Invalid phone number')
    try: payload=await request.json()
    except Exception: payload={}
    note=str((payload or {}).get('note') or '').strip()
    if not note: raise HTTPException(400,'Please enter an internal note.')
    if len(note)>3000: raise HTTPException(400,'Internal note must be 3000 characters or less.')
    c=db()
    try:
        if not _chat_phone_allowed(c,digits,u): raise HTTPException(403,'This conversation is not assigned to your HR account.')
        c.execute("INSERT INTO internal_notes(user_id,phone,note) VALUES(?,?,?)",(u['id'],digits,note))
        c.commit(); return {'ok':True,'message':'Internal note saved.'}
    finally: c.close()


@app.get('/api/analytics')
def analytics(days: int = 30, user_id: int = 0, u=Depends(current_user)):
    """Daily WhatsApp message analytics in India time (Asia/Kolkata).

    Campaign sends are counted from logs; incoming messages and chat replies
    are counted from chat_messages. This avoids double-counting campaign sends.
    HR users only receive their own assigned records; admins receive all records.
    """
    from datetime import datetime, timedelta, timezone
    try:
        from zoneinfo import ZoneInfo
        india_tz = ZoneInfo('Asia/Kolkata')
    except Exception:
        india_tz = timezone(timedelta(hours=5, minutes=30))

    days = max(1, min(int(days or 30), 90))
    today = datetime.now(india_tz).date()
    start_day = today - timedelta(days=days - 1)
    # Read a little extra to safely cover UTC timestamps at the India-day boundary.
    cutoff = datetime.combine(start_day, datetime.min.time(), tzinfo=india_tz).astimezone(timezone.utc).replace(tzinfo=None)

    def local_day(value):
        if value is None:
            return None
        try:
            if isinstance(value, datetime):
                dt = value
            else:
                raw = str(value).strip().replace('Z', '+00:00')
                dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                # Database CURRENT_TIMESTAMP values are stored as UTC-naive.
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(india_tz).date()
        except Exception:
            return None

    c = db()
    if u['role'] == 'admin':
        log_rows = c.execute(
            "SELECT l.user_id,l.status,l.created_at,COALESCE(us.username,'Unknown user') username "
            "FROM logs l LEFT JOIN users us ON us.id=l.user_id WHERE l.created_at>=?",
            (cutoff.strftime('%Y-%m-%d %H:%M:%S'),)
        ).fetchall()
        chat_rows = c.execute(
            "SELECT user_id,direction,created_at FROM chat_messages WHERE created_at>=?",
            (cutoff.strftime('%Y-%m-%d %H:%M:%S'),)
        ).fetchall()
        users = c.execute("SELECT id,username FROM users WHERE role='hr' ORDER BY username").fetchall()
        if user_id:
            log_rows = [r for r in log_rows if str(r['user_id']) == str(user_id)]
            chat_rows = [r for r in chat_rows if str(r['user_id']) == str(user_id)]
            users = [r for r in users if str(r['id']) == str(user_id)]
    else:
        log_rows = c.execute(
            "SELECT l.user_id,l.status,l.created_at,? username FROM logs l WHERE l.user_id=? AND l.created_at>=?",
            (u['username'], u['id'], cutoff.strftime('%Y-%m-%d %H:%M:%S'))
        ).fetchall()
        chat_rows = c.execute(
            "SELECT user_id,direction,created_at FROM chat_messages WHERE user_id=? AND created_at>=?",
            (u['id'], cutoff.strftime('%Y-%m-%d %H:%M:%S'))
        ).fetchall()
        users = [{'id': u['id'], 'username': u['username']}]
    c.close()

    daily = {}
    by_user = {}
    for offset in range(days):
        d = start_day + timedelta(days=offset)
        key = d.isoformat()
        daily[key] = {'date': key, 'incoming': 0, 'outgoing': 0, 'total': 0, 'failed': 0}

    def add_user(uid, username):
        key = str(uid) if uid is not None else 'unassigned'
        if key not in by_user:
            by_user[key] = {'user_id': uid, 'username': username or ('Unassigned' if uid is None else 'Unknown user'),
                            'incoming': 0, 'outgoing': 0, 'total': 0, 'failed': 0}
        return by_user[key]

    # Outbound campaign/template sends are recorded in logs.
    for row in log_rows:
        d = local_day(row['created_at'])
        if d not in (start_day + timedelta(days=i) for i in range(days)):
            continue
        key = d.isoformat(); status = str(row['status'] or '').upper()
        daily[key]['outgoing'] += 1
        daily[key]['total'] += 1
        if status in ('FAILED', 'ERROR', 'REJECTED'):
            daily[key]['failed'] += 1
        user_stat = add_user(row['user_id'], row['username'])
        user_stat['outgoing'] += 1; user_stat['total'] += 1
        if status in ('FAILED', 'ERROR', 'REJECTED'):
            user_stat['failed'] += 1

    # Incoming webhook messages and one-to-one chat replies live in chat_messages.
    for row in chat_rows:
        d = local_day(row['created_at'])
        if d is None or d < start_day or d > today:
            continue
        key = d.isoformat(); direction = str(row['direction'] or '').upper()
        if direction not in ('IN', 'OUT'):
            continue
        bucket = 'incoming' if direction == 'IN' else 'outgoing'
        daily[key][bucket] += 1; daily[key]['total'] += 1
        uid = row['user_id']
        username = next((x['username'] for x in users if str(x['id']) == str(uid)), None) if uid is not None else None
        user_stat = add_user(uid, username)
        user_stat[bucket] += 1; user_stat['total'] += 1

    daily_rows = list(daily.values())
    result = {
        'days': days,
        'from': start_day.isoformat(),
        'to': today.isoformat(),
        'timezone': 'Asia/Kolkata',
        'summary': {
            'incoming': sum(x['incoming'] for x in daily_rows),
            'outgoing': sum(x['outgoing'] for x in daily_rows),
            'total': sum(x['total'] for x in daily_rows),
            'failed': sum(x['failed'] for x in daily_rows),
        },
        'daily': daily_rows,
    }
    if u['role'] == 'admin':
        # Keep active HR users visible even if they have not sent messages in this period.
        for hr in users:
            add_user(hr['id'], hr['username'])
        result['users'] = sorted(by_user.values(), key=lambda x: (x['total'], x['username']), reverse=True)
        result['selected_user'] = next((dict(x) for x in users if user_id and str(x['id']) == str(user_id)), None)
    return result


@app.get('/api/chats')
def chats(u=Depends(current_user)):
    c=db()
    if u['role']=='admin':
        rows=c.execute("SELECT phone,MAX(candidate_name) candidate_name,MAX(created_at) last_at,COUNT(*) message_count,SUM(CASE WHEN direction='IN' AND COALESCE(is_read,0)=0 THEN 1 ELSE 0 END) unread_count FROM chat_messages GROUP BY phone ORDER BY last_at DESC LIMIT 500").fetchall()
        sent=c.execute("SELECT phone,MAX(candidate_name) candidate_name,MAX(created_at) last_at,COUNT(*) message_count FROM logs WHERE created_at>=? GROUP BY phone ORDER BY last_at DESC LIMIT 500",(_chat_cutoff(),)).fetchall()
    else:
        rows=c.execute("SELECT phone,MAX(candidate_name) candidate_name,MAX(created_at) last_at,COUNT(*) message_count,SUM(CASE WHEN direction='IN' AND COALESCE(is_read,0)=0 THEN 1 ELSE 0 END) unread_count FROM chat_messages WHERE user_id=? GROUP BY phone ORDER BY last_at DESC LIMIT 300",(u['id'],)).fetchall()
        sent=c.execute("SELECT phone,MAX(candidate_name) candidate_name,MAX(created_at) last_at,COUNT(*) message_count FROM logs WHERE user_id=? AND created_at>=? GROUP BY phone ORDER BY last_at DESC LIMIT 300",(u['id'],_chat_cutoff())).fetchall()
    merged={re.sub(r'\D','',str(r['phone'] or '')):dict(r) for r in rows}
    for r in sent:
        key=re.sub(r'\D','',str(r['phone'] or ''))
        if key: merged[key]=({**dict(r), 'unread_count':0} if key not in merged else {**merged[key], 'last_at':max(str(merged[key].get('last_at') or ''),str(r['last_at'] or ''))})
    c.close(); return sorted(merged.values(),key=lambda x:str(x.get('last_at') or ''),reverse=True)

@app.get('/api/chats/{phone}')
def chat_detail(phone:str,u=Depends(current_user)):
    digits=re.sub(r'\D','',phone)
    if not digits: raise HTTPException(400,'Invalid phone number')
    c=db()
    if u['role']=='admin':
        incoming=c.execute('SELECT id,user_id,phone,candidate_name,direction,message_type,content,media_url,provider_message_id,created_at FROM chat_messages WHERE phone=? ORDER BY id ASC LIMIT 1000',(digits,)).fetchall()
        outgoing=c.execute("SELECT id,user_id,phone,candidate_name,'OUT' direction,kind message_type,template_name content,'' media_url,provider_message_id,created_at,status,detail FROM logs WHERE REPLACE(REPLACE(REPLACE(phone,'+',''),' ',''),'-','') LIKE ? AND created_at>=? ORDER BY id ASC LIMIT 1000",('%'+digits[-10:],_chat_cutoff())).fetchall()
    else:
        incoming=c.execute('SELECT id,user_id,phone,candidate_name,direction,message_type,content,media_url,provider_message_id,created_at FROM chat_messages WHERE phone=? AND user_id=? ORDER BY id ASC LIMIT 1000',(digits,u['id'])).fetchall()
        outgoing=c.execute("SELECT id,user_id,phone,candidate_name,'OUT' direction,kind message_type,template_name content,'' media_url,provider_message_id,created_at,status,detail FROM logs WHERE user_id=? AND REPLACE(REPLACE(REPLACE(phone,'+',''),' ',''),'-','') LIKE ? AND created_at>=? ORDER BY id ASC LIMIT 1000",(u['id'],'%'+digits[-10:],_chat_cutoff())).fetchall()
    items=[dict(x) for x in incoming]+[dict(x) for x in outgoing]
    items.sort(key=lambda x:str(x.get('created_at') or ''))
    c.close(); return items

@app.get('/api/history')
def history(status: str = '', user_id: int = 0, from_date: str = '', to_date: str = '',
            q: str = '', u=Depends(current_user)):
    c=db()
    # Mark abandoned reservations so they do not remain stuck as PROCESSING forever.
    if DATABASE_URL:
        c.execute("UPDATE logs SET status='UNKNOWN',detail='Previous send was interrupted before a provider result was saved. Check DoubleTick before retrying to avoid duplicate messages.',status_updated_at=CURRENT_TIMESTAMP WHERE status='PROCESSING' AND created_at < CURRENT_TIMESTAMP - INTERVAL '15 minutes'")
    else:
        c.execute("UPDATE logs SET status='UNKNOWN',detail='Previous send was interrupted before a provider result was saved. Check DoubleTick before retrying to avoid duplicate messages.',status_updated_at=CURRENT_TIMESTAMP WHERE status='PROCESSING' AND datetime(created_at) < datetime('now','-15 minutes')")

    where=[]; params=[]
    if u['role'] == 'admin':
        base = "SELECT l.*,u.username FROM logs l LEFT JOIN users u ON u.id=l.user_id"
        if user_id:
            where.append("l.user_id=?"); params.append(user_id)
    else:
        base = "SELECT l.*,? AS username FROM logs l"
        params.append(u['username'])
        where.append("l.user_id=?"); params.append(u['id'])
    visible_history_statuses = ('SENT', 'DELIVERED', 'READ', 'FREQUENCY_CAPPING')
    if status:
        normalized_status = status.strip().upper()
        if normalized_status not in visible_history_statuses:
            c.close()
            return []
        where.append("UPPER(COALESCE(l.status,''))=?"); params.append(normalized_status)
    else:
        where.append("UPPER(COALESCE(l.status,'')) IN ('SENT','DELIVERED','READ','FREQUENCY_CAPPING')")
    if from_date:
        where.append("DATE(l.created_at)>=DATE(?)"); params.append(from_date)
    if to_date:
        where.append("DATE(l.created_at)<=DATE(?)"); params.append(to_date)
    if q:
        like='%'+q.strip().lower()+'%'
        where.append("(LOWER(COALESCE(l.candidate_name,'')) LIKE ? OR LOWER(COALESCE(l.phone,'')) LIKE ? OR LOWER(COALESCE(l.template_name,'')) LIKE ? OR LOWER(COALESCE(l.detail,'')) LIKE ?)")
        params.extend([like,like,like,like])
    sql=base+(" WHERE "+" AND ".join(where) if where else "")+" ORDER BY l.id DESC LIMIT 5000"
    rows=c.execute(sql,tuple(params)).fetchall()
    users=[]
    if u['role']=='admin':
        users=[dict(x) for x in c.execute("SELECT id,username FROM users WHERE role='hr' ORDER BY username").fetchall()]
    c.commit(); c.close()
    result=[dict(r) for r in rows]
    # Keep the existing array response contract; the frontend loads the user list separately from /admin/users.
    return result

app.mount('/static',StaticFiles(directory=BASE/'static'),name='static')
