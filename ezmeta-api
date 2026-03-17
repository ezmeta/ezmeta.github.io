import json
import os
import random
import requests
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List

# ============================================================
# EZMETA API SERVER v4.0 — Supabase Integration
# Run: python3 -X utf8 -m uvicorn ezmeta_server_v4:app --reload --port 8888
# ============================================================

# Supabase config from environment variables
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def supabase_get(table, filters=None):
    """GET from Supabase REST API"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if filters:
        url += "?" + "&".join([f"{k}=eq.{v}" for k,v in filters.items()])
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    try:
        r = requests.get(url, headers=headers)
        return r.json()
    except:
        return None

def supabase_post(table, data):
    """INSERT into Supabase"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    try:
        r = requests.post(url, headers=headers, json=data)
        return r.json()
    except:
        return None

def supabase_patch(table, filters, data):
    """UPDATE in Supabase"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    url = f"{SUPABASE_URL}/rest/v1/{table}?" + "&".join([f"{k}=eq.{v}" for k,v in filters.items()])
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    try:
        r = requests.patch(url, headers=headers, json=data)
        return r.json()
    except:
        return None

def supabase_delete(table, filters):
    """DELETE from Supabase"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    url = f"{SUPABASE_URL}/rest/v1/{table}?" + "&".join([f"{k}=eq.{v}" for k,v in filters.items()])
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    try:
        r = requests.delete(url, headers=headers)
        return True
    except:
        return False

USE_SUPABASE = bool(SUPABASE_URL and SUPABASE_KEY)

app = FastAPI(title="EZMeta API v2.0", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CLIENTS_FILE = "clients.json"
CONFIG_FILE  = "ezmeta_config.json"
DATA_FILE    = "ezmeta_data.json"

# ============================================================
# CLIENT MANAGEMENT
# ============================================================

def load_clients():
    if USE_SUPABASE:
        rows = supabase_get("clients")
        if rows is not None:
            return {"clients": rows}
    # Fallback to file
    if os.path.exists(CLIENTS_FILE):
        with open(CLIENTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"clients": []}

def save_clients(data):
    if not USE_SUPABASE:
        with open(CLIENTS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

def get_client(client_id: str):
    if USE_SUPABASE:
        rows = supabase_get("clients", {"id": client_id})
        if rows:
            return rows[0]
        return None
    data = load_clients()
    for c in data["clients"]:
        if c["id"] == client_id:
            return c
    return None

# ============================================================
# CONFIG
# ============================================================

def load_config():
    if USE_SUPABASE:
        rows = supabase_get("config", {"id": 1})
        if rows:
            return rows[0]
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {
        "mode": "DEMO",
        "pause_ctr": 1.0,
        "scale_roas": 4.0,
        "freq_alert": 3.5,
        "budget_warn": 80,
        "scale_pct": 20,
        "max_budget": 200,
    }

def save_config(cfg):
    if USE_SUPABASE:
        cfg["updated_at"] = datetime.now().isoformat()
        supabase_patch("config", {"id": 1}, cfg)
    else:
        with open(CONFIG_FILE, "w") as f:
            json.dump(cfg, f, indent=2)

# ============================================================
# META API
# ============================================================

def fetch_meta_campaigns(token, account_id):
    url = f"https://graph.facebook.com/v19.0/{account_id}/campaigns"
    params = {
        "fields": "id,name,status,daily_budget,lifetime_budget,objective",
        "access_token": token,
        "limit": 20
    }
    try:
        r = requests.get(url, params=params)
        data = r.json()
        if "error" in data:
            return [], data["error"]["message"]
        return data.get("data", []), None
    except Exception as e:
        return [], str(e)

def fetch_insights(token, campaign_id):
    url = f"https://graph.facebook.com/v19.0/{campaign_id}/insights"
    params = {
        "fields": "impressions,clicks,spend,ctr,cpc,cpm,actions,action_values,frequency",
        "date_preset": "last_7d",
        "access_token": token
    }
    try:
        r = requests.get(url, params=params)
        data = r.json()
        if "error" in data:
            return None
        items = data.get("data", [])
        return items[0] if items else None
    except:
        return None

def pause_campaign(token, campaign_id):
    url = f"https://graph.facebook.com/v19.0/{campaign_id}"
    try:
        r = requests.post(url, data={"status": "PAUSED", "access_token": token})
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def scale_budget(token, campaign_id, new_budget_cents):
    url = f"https://graph.facebook.com/v19.0/{campaign_id}"
    try:
        r = requests.post(url, data={"daily_budget": str(new_budget_cents), "access_token": token})
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def send_telegram(bot_token, chat_id, message):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        )
        return r.json()
    except Exception as e:
        return {"error": str(e)}

# ============================================================
# DUMMY DATA
# ============================================================

DUMMY_CAMPAIGNS = [
    {"id":"camp_001","name":"Skincare Bundle Raya","status":"ACTIVE","daily_budget":6500,"monthly_budget":195000,"monthly_spend":168000,"spend_today":6320,"spend_7d":[280,310,295,320,305,340,330],"roas":5.2,"roas_7d":[4.8,5.0,5.1,5.3,5.2,5.4,5.2],"ctr":4.8,"ctr_7d":[4.5,4.6,4.7,4.9,4.8,4.8,4.8],"cpc":120,"clicks":1240,"impressions":25800,"frequency":2.1,"frequency_7d":[1.8,1.9,2.0,2.0,2.1,2.1,2.1],"conversions":38,"cpm":2450},
    {"id":"camp_002","name":"Retargeting — Cart","status":"ACTIVE","daily_budget":3000,"monthly_budget":90000,"monthly_spend":87500,"spend_today":2980,"spend_7d":[280,285,290,295,298,300,298],"roas":6.8,"roas_7d":[6.5,6.6,6.7,6.8,6.9,6.8,6.8],"ctr":5.1,"ctr_7d":[5.0,5.0,5.1,5.1,5.2,5.1,5.1],"cpc":95,"clicks":980,"impressions":19200,"frequency":2.8,"frequency_7d":[2.4,2.5,2.6,2.7,2.7,2.8,2.8],"conversions":29,"cpm":1550},
    {"id":"camp_003","name":"Hijab Premium V2","status":"ACTIVE","daily_budget":8000,"monthly_budget":240000,"monthly_spend":195000,"spend_today":7450,"spend_7d":[700,720,730,740,740,745,745],"roas":3.1,"roas_7d":[4.2,3.9,3.7,3.5,3.3,3.2,3.1],"ctr":2.1,"ctr_7d":[3.8,3.5,3.2,2.9,2.6,2.3,2.1],"cpc":165,"clicks":820,"impressions":39000,"frequency":4.8,"frequency_7d":[2.8,3.2,3.6,4.0,4.3,4.6,4.8],"conversions":18,"cpm":1910},
    {"id":"camp_004","name":"Tudung Raya V3","status":"ACTIVE","daily_budget":5000,"monthly_budget":150000,"monthly_spend":142000,"spend_today":4810,"spend_7d":[460,465,468,470,472,475,481],"roas":0.8,"roas_7d":[0.9,0.85,0.83,0.82,0.81,0.80,0.80],"ctr":0.3,"ctr_7d":[0.35,0.33,0.32,0.31,0.31,0.30,0.30],"cpc":340,"clicks":142,"impressions":47300,"frequency":3.2,"frequency_7d":[2.0,2.3,2.6,2.8,3.0,3.1,3.2],"conversions":1,"cpm":1017},
    {"id":"camp_005","name":"Men Grooming Kit","status":"LEARNING","daily_budget":4000,"monthly_budget":120000,"monthly_spend":48000,"spend_today":1860,"spend_7d":[150,160,170,175,180,185,186],"roas":2.4,"roas_7d":[1.8,2.0,2.1,2.2,2.3,2.4,2.4],"ctr":1.8,"ctr_7d":[1.4,1.5,1.6,1.6,1.7,1.8,1.8],"cpc":195,"clicks":320,"impressions":17800,"frequency":1.4,"frequency_7d":[1.0,1.1,1.1,1.2,1.3,1.3,1.4],"conversions":8,"cpm":2697},
    {"id":"camp_006","name":"Dropship Viral Jan","status":"ACTIVE","daily_budget":10000,"monthly_budget":300000,"monthly_spend":289000,"spend_today":9500,"spend_7d":[900,920,930,940,945,948,950],"roas":4.0,"roas_7d":[4.1,4.0,4.1,4.0,4.0,4.0,4.0],"ctr":3.2,"ctr_7d":[3.2,3.2,3.2,3.2,3.2,3.2,3.2],"cpc":140,"clicks":1520,"impressions":47500,"frequency":2.3,"frequency_7d":[2.0,2.1,2.1,2.2,2.2,2.3,2.3],"conversions":42,"cpm":2000},
]

# ============================================================
# AI ENGINE
# ============================================================

def run_ai_engine(campaigns, cfg):
    rules = {
        "pause_ctr": float(cfg.get("pause_ctr", 1.0)),
        "scale_roas": float(cfg.get("scale_roas", 4.0)),
        "scale_by": float(cfg.get("scale_pct", 20)) / 100,
        "max_budget": float(cfg.get("max_budget", 200)) * 100,
        "freq_alert": float(cfg.get("freq_alert", 3.5)),
        "budget_warn": float(cfg.get("budget_warn", 80)),
    }

    recommendations, budget_alerts, winners, fatigued = [], [], [], []

    for c in campaigns:
        spend_rm = c["spend_today"] / 100
        budget_rm = c["daily_budget"] / 100

        if c["ctr"] < rules["pause_ctr"] and c["spend_today"] >= 10000 and c["status"] == "ACTIVE":
            recommendations.append({"type":"PAUSE","campaign_id":c["id"],"campaign_name":c["name"],"reason":f"CTR {c['ctr']}% di bawah threshold {rules['pause_ctr']}%","detail":f"Spend RM {spend_rm:.0f} tanpa performance","confidence":94,"priority":"HIGH","new_budget":None})
        elif c["roas"] >= rules["scale_roas"] and c["status"] == "ACTIVE":
            nb = min(c["daily_budget"] * (1 + rules["scale_by"]), rules["max_budget"])
            recommendations.append({"type":"SCALE","campaign_id":c["id"],"campaign_name":c["name"],"reason":f"ROAS {c['roas']}× melebihi threshold {rules['scale_roas']}×","detail":f"Cadang naik budget RM {budget_rm:.0f} → RM {nb/100:.0f}/hari","confidence":88,"priority":"HIGH","new_budget":nb})

        if c["frequency"] >= rules["freq_alert"] and c["status"] == "ACTIVE":
            recommendations.append({"type":"FREQUENCY_WARNING","campaign_id":c["id"],"campaign_name":c["name"],"reason":f"Frequency {c['frequency']} melebihi threshold {rules['freq_alert']}","detail":"Cadang refresh creative atau expand audience","confidence":85,"priority":"MEDIUM","new_budget":None})

        if c["monthly_budget"] > 0:
            pct = (c["monthly_spend"] / c["monthly_budget"]) * 100
            days_left = max(30 - datetime.now().day, 1)
            remaining = (c["monthly_budget"] - c["monthly_spend"]) / 100
            if pct >= 95:
                budget_alerts.append({"level":"CRITICAL","campaign_name":c["name"],"pct_used":round(pct,1),"spend_rm":c["monthly_spend"]/100,"monthly_rm":c["monthly_budget"]/100,"remaining_rm":remaining,"message":f"Budget hampir habis! {pct:.0f}%","suggestion":f"Hanya RM {remaining:.0f} berbaki untuk {days_left} hari."})
            elif pct >= rules["budget_warn"]:
                budget_alerts.append({"level":"WARNING","campaign_name":c["name"],"pct_used":round(pct,1),"spend_rm":c["monthly_spend"]/100,"monthly_rm":c["monthly_budget"]/100,"remaining_rm":remaining,"message":f"Budget {pct:.0f}% digunakan","suggestion":f"Boleh spend RM {remaining/days_left:.0f}/hari untuk {days_left} hari."})

        if c["status"] == "ACTIVE":
            sc = 0; reasons = []
            if c["roas"] >= 4.5: sc += 40; reasons.append(f"ROAS {c['roas']}×")
            if c["ctr"] >= 4.0: sc += 30; reasons.append(f"CTR {c['ctr']}%")
            r7 = c.get("roas_7d",[])
            if len(r7)>=3 and r7[-1]>=r7[-3]: sc+=20; reasons.append("ROAS trend menaik")
            c7 = c.get("ctr_7d",[])
            if len(c7)>=3 and c7[-1]>=c7[-3]: sc+=10; reasons.append("CTR stabil")
            if sc >= 60:
                winners.append({"campaign_id":c["id"],"campaign_name":c["name"],"score":sc,"roas":c["roas"],"ctr":c["ctr"],"reasons":reasons,"recommendation":f"Scale budget +20% segera."})

        fs = 0; signals = []
        c7 = c.get("ctr_7d",[])
        if len(c7)>=4:
            cs = sum(c7[:3])/3; ce = c7[-1]
            if cs>0:
                drop = ((cs-ce)/cs)*100
                if drop>=25: fs+=40; signals.append(f"CTR turun {drop:.0f}% ({cs:.1f}%→{ce:.1f}%)")
        if c["frequency"]>=rules["freq_alert"]: fs+=35; signals.append(f"Frequency {c['frequency']}")
        f7 = c.get("frequency_7d",[])
        if len(f7)>=3 and (f7[-1]-f7[0])>=1.5: fs+=25; signals.append(f"Frequency naik {f7[-1]-f7[0]:.1f}×")
        if fs>=35:
            fatigued.append({"campaign_id":c["id"],"campaign_name":c["name"],"fatigue_score":fs,"severity":"KRITIKAL" if fs>=70 else "SEDERHANA","signals":signals,"recommendation":"Tukar creative dalam 24–48 jam."})

    total_spend = sum(c["spend_today"] for c in campaigns)/100
    total_clicks = sum(c["clicks"] for c in campaigns)
    total_conv = sum(c["conversions"] for c in campaigns)
    avg_roas = sum(c["roas"] for c in campaigns)/len(campaigns) if campaigns else 0
    avg_ctr = sum(c["ctr"] for c in campaigns)/len(campaigns) if campaigns else 0
    ms = sum(c["monthly_spend"] for c in campaigns)/100
    mb = sum(c["monthly_budget"] for c in campaigns)/100

    stats = {
        "total_spend_rm": round(total_spend,2),
        "total_clicks": total_clicks,
        "total_conversions": total_conv,
        "avg_roas": round(avg_roas,2),
        "avg_ctr": round(avg_ctr,2),
        "avg_cpc_rm": round(sum(c["cpc"] for c in campaigns)/len(campaigns)/100,2) if campaigns else 0,
        "active_campaigns": sum(1 for c in campaigns if c["status"]=="ACTIVE"),
        "monthly_spend_rm": round(ms,2),
        "monthly_budget_rm": round(mb,2),
        "monthly_pct_used": round((ms/mb)*100,1) if mb>0 else 0,
    }

    def health(c):
        s=0; b={}
        p=30 if c["roas"]>=5 else 25 if c["roas"]>=4 else 18 if c["roas"]>=3 else 10 if c["roas"]>=2 else 0
        s+=p; b["ROAS"]={"score":p,"max":30,"value":f"{c['roas']}×","status":"Baik" if p>=20 else "Sederhana" if p>=10 else "Lemah"}
        p=25 if c["ctr"]>=4 else 18 if c["ctr"]>=2 else 10 if c["ctr"]>=1 else 0
        s+=p; b["CTR"]={"score":p,"max":25,"value":f"{c['ctr']}%","status":"Baik" if p>=18 else "Sederhana" if p>=10 else "Lemah"}
        p=20 if c["frequency"]<=2.5 else 12 if c["frequency"]<=3.5 else 5 if c["frequency"]<=4.5 else 0
        s+=p; b["Frequency"]={"score":p,"max":20,"value":str(c["frequency"]),"status":"Baik" if p>=15 else "Sederhana" if p>=8 else "Refresh"}
        cpc=c["cpc"]/100; p=15 if cpc<=1 else 10 if cpc<=1.5 else 5 if cpc<=2.5 else 0
        s+=p; b["CPC"]={"score":p,"max":15,"value":f"RM {cpc:.2f}","status":"Baik" if p>=10 else "Sederhana" if p>=5 else "Tinggi"}
        p=10 if c["conversions"]>=20 else 7 if c["conversions"]>=10 else 4 if c["conversions"]>=5 else 0
        s+=p; b["Conversions"]={"score":p,"max":10,"value":str(c["conversions"]),"status":"Baik" if p>=7 else "Sederhana" if p>=4 else "Rendah"}
        return {"total":s,"grade":"A" if s>=80 else "B" if s>=65 else "C" if s>=50 else "D","breakdown":b}

    health_scores = {c["id"]: health(c) for c in campaigns}

    today = datetime.now()
    chart_data = [{"date":(today-timedelta(days=13-i)).strftime("%d/%m"),"spend":round(280+random.uniform(-30,50)+i*4,2),"roas":round(3.8+random.uniform(-0.4,0.6),2)} for i in range(14)]

    alerts = []
    for a in budget_alerts: alerts.append({"level":a["level"],"message":a["message"],"time":"Baru sahaja"})
    for r in recommendations:
        if r["type"]=="PAUSE": alerts.append({"level":"CRITICAL","message":f"Zero performance — {r['campaign_name']}","time":"Tadi"})
    for f in fatigued:
        if f["signals"]: alerts.append({"level":"WARNING","message":f"Fatigue [{f['severity']}] — {f['campaign_name']}: {f['signals'][0]}","time":"1 jam lepas"})

    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    report = f"=== EZMeta Report {now} ===\nSpend: RM {stats['total_spend_rm']} | ROAS: {stats['avg_roas']}×\nCampaigns: {len(campaigns)} | Active: {stats['active_campaigns']}\nRecommendations: {len(recommendations)}"
    weekly = f"=== Weekly Report ===\nSpend est: RM {stats['total_spend_rm']*7:.2f}\nROAS: {stats['avg_roas']}× | Conv: {stats['total_conversions']}"

    return {
        "generated_at": datetime.now().isoformat(),
        "mode": cfg.get("mode","DEMO"),
        "stats": stats,
        "campaigns": campaigns,
        "recommendations": sorted(recommendations, key=lambda x: x.get("confidence",0), reverse=True),
        "budget_alerts": budget_alerts,
        "winners": sorted(winners, key=lambda x: x["score"], reverse=True),
        "fatigued": sorted(fatigued, key=lambda x: x["fatigue_score"], reverse=True),
        "alerts": alerts,
        "health_scores": health_scores,
        "chart_data": chart_data,
        "report": report,
        "weekly_report": weekly,
    }

def build_campaigns_from_meta(token, account_id):
    raw, err = fetch_meta_campaigns(token, account_id)
    if not raw:
        return [], err
    campaigns = []
    for c in raw:
        ins = fetch_insights(token, c["id"])
        spend = float(ins.get("spend",0)) if ins else 0
        clicks = int(ins.get("clicks",0)) if ins else 0
        impressions = int(ins.get("impressions",0)) if ins else 0
        ctr = float(ins.get("ctr",0)) if ins else 0
        cpc = float(ins.get("cpc",0)) if ins else 0
        frequency = float(ins.get("frequency",0)) if ins else 0
        conversions = 0; conv_value = 0
        if ins:
            for action in ins.get("actions",[]):
                if action["action_type"] in ["purchase","lead"]: conversions+=int(action["value"])
            for av in ins.get("action_values",[]):
                if av["action_type"]=="purchase": conv_value+=float(av["value"])
        roas = round(conv_value/spend,2) if spend>0 else 0
        daily_budget = int(c.get("daily_budget",0)) or int(c.get("lifetime_budget",0))//30
        campaigns.append({
            "id":c["id"],"name":c["name"],"status":c.get("status","ACTIVE"),
            "daily_budget":daily_budget,"monthly_budget":daily_budget*30,
            "monthly_spend":int(spend*100*4),"spend_today":int(spend*100/7),
            "spend_7d":[round(spend/7+random.uniform(-10,10),2) for _ in range(7)],
            "roas":roas,"roas_7d":[round(roas+random.uniform(-0.3,0.3),2) for _ in range(7)],
            "ctr":round(ctr,2),"ctr_7d":[round(ctr+random.uniform(-0.3,0.3),2) for _ in range(7)],
            "cpc":int(cpc*100),"clicks":clicks,"impressions":impressions,
            "frequency":round(frequency,1),"frequency_7d":[round(frequency+random.uniform(-0.2,0.2),2) for _ in range(7)],
            "conversions":conversions,"cpm":0,
        })
    return campaigns, None

# ============================================================
# ROUTES — ROOT
# ============================================================

@app.get("/")
def root():
    clients = load_clients()
    return {
        "status": "EZMeta API v4.0 Running",
        "database": "Supabase" if USE_SUPABASE else "Local JSON",
        "total_clients": len(clients["clients"]),
        "active_clients": sum(1 for c in clients["clients"] if c.get("status")=="ACTIVE"),
        "time": datetime.now().isoformat()
    }

# ============================================================
# ROUTES — DATA (per client or default)
# ============================================================

@app.get("/data")
def get_data(client_id: Optional[str] = None):
    cfg = load_config()

    # If client_id specified, use that client's token
    if client_id:
        client = get_client(client_id)
        if not client:
            raise HTTPException(status_code=404, detail=f"Client {client_id} not found")
        token = client.get("access_token","")
        account_id = client.get("ad_account_id","")
        mode = "LIVE" if token and token != "LETAK_TOKEN_SINI" else "DEMO"
    else:
        token = cfg.get("access_token","")
        account_id = cfg.get("ad_account_id","")
        mode = cfg.get("mode","DEMO")

    campaigns = []
    if mode == "LIVE" and token and account_id:
        campaigns, err = build_campaigns_from_meta(token, account_id)

    if not campaigns:
        campaigns = DUMMY_CAMPAIGNS

    result = run_ai_engine(campaigns, cfg)
    if client_id:
        result["client_id"] = client_id

    with open(DATA_FILE, "w") as f:
        json.dump(result, f, indent=2)

    return result

@app.get("/data/all")
def get_all_clients_data():
    """Scan semua clients sekaligus — untuk admin overview"""
    clients_data = load_clients()
    cfg = load_config()
    results = []

    for client in clients_data["clients"]:
        if client.get("status") != "ACTIVE":
            continue
        token = client.get("access_token","")
        account_id = client.get("ad_account_id","")
        mode = "LIVE" if token and token != "LETAK_TOKEN_SINI" else "DEMO"

        campaigns = []
        error = None
        if mode == "LIVE" and token and account_id:
            campaigns, error = build_campaigns_from_meta(token, account_id)

        if not campaigns:
            campaigns = DUMMY_CAMPAIGNS

        engine_result = run_ai_engine(campaigns, cfg)

        results.append({
            "client_id": client["id"],
            "client_name": client["name"],
            "business": client.get("business",""),
            "plan": client.get("plan","STARTER"),
            "mode": mode,
            "error": error,
            "stats": engine_result["stats"],
            "campaigns_count": len(campaigns),
            "recommendations_count": len(engine_result["recommendations"]),
            "winners_count": len(engine_result["winners"]),
            "alerts_count": len(engine_result["alerts"]),
            "generated_at": engine_result["generated_at"],
        })

    total_spend = sum(r["stats"]["total_spend_rm"] for r in results)
    total_campaigns = sum(r["campaigns_count"] for r in results)
    total_alerts = sum(r["alerts_count"] for r in results)

    return {
        "generated_at": datetime.now().isoformat(),
        "total_clients": len(results),
        "total_spend_rm": round(total_spend, 2),
        "total_campaigns": total_campaigns,
        "total_alerts": total_alerts,
        "clients": results
    }

# ============================================================
# ROUTES — CLIENT MANAGEMENT
# ============================================================

@app.get("/clients")
def list_clients():
    data = load_clients()
    # Mask tokens
    safe = []
    for c in data["clients"]:
        sc = dict(c)
        if sc.get("access_token") and sc["access_token"] != "LETAK_TOKEN_SINI":
            sc["access_token"] = "****" + sc["access_token"][-8:]
        safe.append(sc)
    return {"clients": safe, "total": len(safe)}

class ClientCreate(BaseModel):
    name: str
    email: Optional[str] = ""
    phone: Optional[str] = ""
    business: Optional[str] = ""
    plan: Optional[str] = "STARTER"
    access_token: Optional[str] = "LETAK_TOKEN_SINI"
    ad_account_id: Optional[str] = ""
    chat_id: Optional[str] = ""
    telegram_name: Optional[str] = ""
    monthly_fee_rm: Optional[float] = 49
    notes: Optional[str] = ""
    temp_password: Optional[str] = ""

@app.post("/clients")
def add_client(client: ClientCreate):
    data = load_clients()
    new_id = f"client_{len(data['clients'])+1:03d}"
    new_client = {
        "id": new_id,
        "name": client.name,
        "email": client.email.lower() if client.email else "",
        "phone": client.phone or "",
        "business": client.business,
        "plan": client.plan,
        "status": "ACTIVE",
        "access_token": client.access_token,
        "ad_account_id": client.ad_account_id,
        "chat_id": client.chat_id,
        "telegram_name": client.telegram_name,
        "onboard_date": datetime.now().strftime("%Y-%m-%d"),
        "monthly_fee_rm": client.monthly_fee_rm,
        "notes": client.notes,
        "scan_times": ["08:00", "14:00", "21:00"]
    }
    if USE_SUPABASE:
        result = supabase_post("clients", new_client)
        if result is None:
            raise HTTPException(status_code=500, detail="Failed to save to database")
    else:
        data["clients"].append(new_client)
        save_clients(data)
    return {"status": "ok", "client_id": new_id, "message": f"Client {client.name} added!"}

@app.put("/clients/{client_id}")
def update_client(client_id: str, updates: dict):
    if USE_SUPABASE:
        updates["last_updated"] = datetime.now().isoformat()
        result = supabase_patch("clients", {"id": client_id}, updates)
        if result is None:
            raise HTTPException(status_code=404, detail="Client not found")
        return {"status": "ok", "message": f"Client {client_id} updated"}
    data = load_clients()
    for i, c in enumerate(data["clients"]):
        if c["id"] == client_id:
            data["clients"][i].update(updates)
            save_clients(data)
            return {"status": "ok", "message": f"Client {client_id} updated"}
    raise HTTPException(status_code=404, detail="Client not found")

@app.delete("/clients/{client_id}")
def remove_client(client_id: str):
    if USE_SUPABASE:
        result = supabase_delete("clients", {"id": client_id})
        return {"status": "ok", "message": f"Client {client_id} removed"}
    data = load_clients()
    before = len(data["clients"])
    data["clients"] = [c for c in data["clients"] if c["id"] != client_id]
    if len(data["clients"]) == before:
        raise HTTPException(status_code=404, detail="Client not found")
    save_clients(data)
    return {"status": "ok", "message": f"Client {client_id} removed"}

# ============================================================
# SELF-REGISTER
# ============================================================

class RegisterRequest(BaseModel):
    name: str
    email: str
    business: str
    phone: str
    plan: str
    ad_account_id: Optional[str] = ""
    access_token: Optional[str] = ""
    chat_id: Optional[str] = ""
    message: Optional[str] = ""

@app.post("/register")
def register_client(req: RegisterRequest):
    data = load_clients()
    cfg = load_config()

    # Check if email already registered
    for c in data["clients"]:
        if c.get("email","").lower() == req.email.lower():
            raise HTTPException(status_code=400, detail="Email ini sudah didaftarkan")

    # Generate client ID and temp password
    import random, string
    new_id = f"client_{len(data['clients'])+1:03d}"
    temp_pass = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    fee = 49 if req.plan=="STARTER" else 99 if req.plan=="PRO" else 199

    new_client = {
        "id": new_id,
        "name": req.name,
        "email": req.email.lower(),
        "business": req.business,
        "phone": req.phone,
        "plan": req.plan,
        "status": "PENDING",
        "access_token": req.access_token or "LETAK_TOKEN_SINI",
        "ad_account_id": req.ad_account_id or "",
        "chat_id": req.chat_id or "",
        "telegram_name": "",
        "onboard_date": datetime.now().strftime("%Y-%m-%d"),
        "monthly_fee_rm": fee,
        "notes": req.message or "",
        "temp_password": temp_pass,
        "registered_at": datetime.now().isoformat(),
        "scan_times": ["08:00", "14:00", "21:00"]
    }

    if USE_SUPABASE:
        supabase_post("clients", new_client)
    else:
        data["clients"].append(new_client)
        save_clients(data)

    # Notify admin via Telegram
    bot_token = cfg.get("bot_token","")
    chat_id = cfg.get("chat_id","")
    if bot_token and chat_id:
        msg = f"🔔 <b>Pendaftaran Baru EZMeta!</b>\n\n"
        msg += f"👤 Nama: {req.name}\n"
        msg += f"📧 Email: {req.email}\n"
        msg += f"🏢 Bisnes: {req.business}\n"
        msg += f"📱 Phone: {req.phone}\n"
        msg += f"📦 Plan: {req.plan} (RM {fee}/bln)\n"
        msg += f"🆔 Client ID: {new_id}\n"
        if req.message:
            msg += f"💬 Mesej: {req.message}\n"
        msg += f"\n⚡ Approve dalam dashboard EZMeta!"
        send_telegram(bot_token, chat_id, msg)

    return {
        "status": "ok",
        "message": "Pendaftaran berjaya! Admin akan approve dalam masa 24 jam.",
        "client_id": new_id,
        "plan": req.plan,
        "fee": fee
    }

@app.post("/clients/{client_id}/approve")
def approve_client(client_id: str):
    data = load_clients()
    cfg = load_config()
    for i, c in enumerate(data["clients"]):
        if c["id"] == client_id:
            if c["status"] != "PENDING":
                raise HTTPException(status_code=400, detail="Client bukan dalam status PENDING")
            if USE_SUPABASE:
                supabase_patch("clients", {"id": client_id}, {"status": "ACTIVE", "approved_at": datetime.now().isoformat()})
            else:
                data["clients"][i]["status"] = "ACTIVE"
                data["clients"][i]["approved_at"] = datetime.now().isoformat()
                save_clients(data)

            # Notify client via Telegram
            bot_token = cfg.get("bot_token","")
            client_chat = c.get("chat_id","")
            if bot_token and client_chat:
                msg = f"✅ <b>Akaun EZMeta Anda Diluluskan!</b>\n\n"
                msg += f"Selamat datang ke EZMeta, {c['name']}!\n\n"
                msg += f"📦 Plan: {c['plan']}\n"
                msg += f"💰 Fee: RM {c['monthly_fee_rm']}/bulan\n\n"
                msg += f"🔑 Login: ezmeta.github.io\n"
                msg += f"📧 Email: {c.get('email','')}\n"
                msg += f"🔒 Password sementara: {c.get('temp_password','')}\n\n"
                msg += f"Sila tukar password selepas login pertama."
                send_telegram(bot_token, client_chat, msg)

            return {"status":"ok","message":f"Client {c['name']} diluluskan!","temp_password":c.get("temp_password","")}
    raise HTTPException(status_code=404, detail="Client not found")

@app.post("/clients/{client_id}/reject")
def reject_client(client_id: str, reason: Optional[str] = ""):
    data = load_clients()
    cfg = load_config()
    for i, c in enumerate(data["clients"]):
        if c["id"] == client_id:
            data["clients"][i]["status"] = "REJECTED"
            data["clients"][i]["reject_reason"] = reason
            save_clients(data)

            # Notify client
            bot_token = cfg.get("bot_token","")
            client_chat = c.get("chat_id","")
            if bot_token and client_chat:
                msg = f"❌ Maaf, pendaftaran EZMeta anda tidak dapat diluluskan.\n"
                if reason: msg += f"Sebab: {reason}\n"
                msg += f"Hubungi kami untuk maklumat lanjut."
                send_telegram(bot_token, client_chat, msg)

            return {"status":"ok","message":f"Client {c['name']} ditolak"}
    raise HTTPException(status_code=404, detail="Client not found")

@app.post("/clients/{client_id}/update-credentials")
def update_client_credentials(client_id: str, updates: dict):
    """Client update their own token and account details"""
    allowed_fields = ["access_token","ad_account_id","chat_id","telegram_name","phone"]
    safe_updates = {k: v for k, v in updates.items() if k in allowed_fields}
    safe_updates["last_updated"] = datetime.now().isoformat()
    if USE_SUPABASE:
        supabase_patch("clients", {"id": client_id}, safe_updates)
        return {"status":"ok","message":"Credentials dikemaskini!"}
    data = load_clients()
    for i, c in enumerate(data["clients"]):
        if c["id"] == client_id:
            data["clients"][i].update(safe_updates)
            save_clients(data)
            return {"status":"ok","message":"Credentials dikemaskini!"}
    raise HTTPException(status_code=404, detail="Client not found")

@app.get("/clients/pending")
def get_pending_clients():
    data = load_clients()
    pending = [c for c in data["clients"] if c.get("status") == "PENDING"]
    return {"pending": pending, "count": len(pending)}

# ============================================================
# ROUTES — SCAN & ALERTS
# ============================================================

@app.post("/scan/all")
def scan_all_clients():
    """Scan semua clients dan hantar Telegram alerts"""
    clients_data = load_clients()
    cfg = load_config()
    results = []

    for client in clients_data["clients"]:
        if client.get("status") != "ACTIVE":
            continue

        token = client.get("access_token","")
        account_id = client.get("ad_account_id","")
        chat_id = client.get("chat_id","")
        bot_token = cfg.get("bot_token","")
        mode = "LIVE" if token and token != "LETAK_TOKEN_SINI" else "DEMO"

        campaigns = []
        if mode == "LIVE" and token and account_id:
            campaigns, _ = build_campaigns_from_meta(token, account_id)
        if not campaigns:
            campaigns = DUMMY_CAMPAIGNS

        engine = run_ai_engine(campaigns, cfg)
        stats = engine["stats"]

        # Build Telegram message
        msg = f"📊 <b>EZMeta — {client['name']}</b>\n"
        msg += f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
        msg += f"💰 Spend: RM {stats['total_spend_rm']:.2f}\n"
        msg += f"📈 ROAS: {stats['avg_roas']}×\n"
        msg += f"🎯 CTR: {stats['avg_ctr']}%\n"
        msg += f"🔄 Conv: {stats['total_conversions']}\n\n"

        if engine["winners"]:
            msg += f"🏆 <b>Winning Ads ({len(engine['winners'])})</b>\n"
            for w in engine["winners"][:2]:
                msg += f"  ⭐ {w['campaign_name']} — Score {w['score']}/100\n"
            msg += "\n"

        if engine["fatigued"]:
            msg += f"😴 <b>Creative Fatigue ({len(engine['fatigued'])})</b>\n"
            for f in engine["fatigued"][:2]:
                msg += f"  ⚠️ {f['campaign_name']} [{f['severity']}]\n"
            msg += "\n"

        if engine["recommendations"]:
            msg += f"🤖 <b>AI Recommendations ({len(engine['recommendations'])})</b>\n"
            for r in engine["recommendations"][:3]:
                emoji = "⏸" if r["type"]=="PAUSE" else "🚀" if r["type"]=="SCALE" else "⚠️"
                msg += f"  {emoji} {r['campaign_name']}: {r['reason']}\n"

        msg += f"\n<i>Mode: {mode}</i>"

        # Send to Telegram
        tg_result = None
        if bot_token and chat_id:
            tg_result = send_telegram(bot_token, chat_id, msg)

        results.append({
            "client_id": client["id"],
            "client_name": client["name"],
            "mode": mode,
            "campaigns_scanned": len(campaigns),
            "recommendations": len(engine["recommendations"]),
            "telegram_sent": tg_result is not None and "error" not in str(tg_result),
        })

    return {
        "status": "ok",
        "scanned": len(results),
        "time": datetime.now().isoformat(),
        "results": results
    }

# ============================================================
# ROUTES — ACTION
# ============================================================

class ActionRequest(BaseModel):
    type: str
    campaign_id: str
    client_id: Optional[str] = None
    new_budget: Optional[float] = None

@app.post("/action")
def execute_action(req: ActionRequest):
    cfg = load_config()

    # Get token — from client or config
    if req.client_id:
        client = get_client(req.client_id)
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        token = client.get("access_token","")
    else:
        token = cfg.get("access_token","")

    if not token or token == "LETAK_TOKEN_SINI":
        raise HTTPException(status_code=400, detail="Access token not configured")

    if req.type == "PAUSE":
        result = pause_campaign(token, req.campaign_id)
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        return {"status":"ok","message":f"✅ Campaign paused successfully","result":result}

    elif req.type == "SCALE":
        if not req.new_budget:
            raise HTTPException(status_code=400, detail="new_budget required")
        result = scale_budget(token, req.campaign_id, int(req.new_budget))
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        return {"status":"ok","message":f"✅ Budget updated successfully","result":result}

    raise HTTPException(status_code=400, detail="Unknown action type")

# ============================================================
# ROUTES — CONFIG
# ============================================================

@app.get("/config")
def get_config():
    cfg = load_config()
    safe = dict(cfg)
    if safe.get("access_token"): safe["access_token"] = "****" + safe["access_token"][-8:]
    if safe.get("bot_token"): safe["bot_token"] = "****" + safe["bot_token"][-6:]
    return safe

class ConfigUpdate(BaseModel):
    mode: Optional[str] = None
    access_token: Optional[str] = None
    ad_account_id: Optional[str] = None
    bot_token: Optional[str] = None
    chat_id: Optional[str] = None
    pause_ctr: Optional[float] = None
    scale_roas: Optional[float] = None
    freq_alert: Optional[float] = None
    budget_warn: Optional[float] = None
    scale_pct: Optional[float] = None
    max_budget: Optional[float] = None

@app.post("/config")
def update_config(update: ConfigUpdate):
    cfg = load_config()
    cfg.update(update.model_dump(exclude_none=True))
    save_config(cfg)
    return {"status":"ok","message":"Config updated"}

# ============================================================
# AUTH
# ============================================================

class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/auth/login")
def login(req: LoginRequest):
    data = load_clients()
    username = req.username.lower().strip()
    
    # Check admin accounts
    ADMINS = {
        "admin": {"password": "ezmeta2026", "name": "Admin EZMeta", "role": "admin", "isAdmin": True},
        "pokcik": {"password": "ezmeta123", "name": "Muhammad Al Hafiz", "role": "admin", "isAdmin": True},
    }
    
    if username in ADMINS and ADMINS[username]["password"] == req.password:
        admin = ADMINS[username]
        return {
            "status": "ok",
            "user": {
                "username": username,
                "name": admin["name"],
                "role": admin["role"],
                "isAdmin": True,
                "client_id": None,
                "avatar": username[:2].upper()
            }
        }
    
    # Check client accounts (login by email or client_id)
    for client in data["clients"]:
        email_match = client.get("email","").lower() == username
        id_match = client.get("id","").lower() == username
        
        if (email_match or id_match) and client.get("status") == "ACTIVE":
            # Check password (temp_password or custom password)
            if req.password == client.get("temp_password","") or req.password == client.get("password",""):
                return {
                    "status": "ok",
                    "user": {
                        "username": username,
                        "name": client["name"],
                        "role": "client",
                        "isAdmin": False,
                        "client_id": client["id"],
                        "plan": client.get("plan","STARTER"),
                        "business": client.get("business",""),
                        "avatar": client["name"][:2].upper()
                    }
                }
            else:
                raise HTTPException(status_code=401, detail="Password salah")
        
        if (email_match or id_match) and client.get("status") == "PENDING":
            raise HTTPException(status_code=403, detail="Akaun anda belum diluluskan. Sila tunggu 24 jam.")
        
        if (email_match or id_match) and client.get("status") == "REJECTED":
            raise HTTPException(status_code=403, detail="Permohonan anda telah ditolak. Hubungi admin.")
    
    raise HTTPException(status_code=401, detail="Username atau password salah")

@app.post("/auth/change-password")
def change_password(client_id: str, old_password: str, new_password: str):
    data = load_clients()
    for i, c in enumerate(data["clients"]):
        if c["id"] == client_id:
            if old_password not in [c.get("temp_password",""), c.get("password","")]:
                raise HTTPException(status_code=401, detail="Password lama salah")
            if USE_SUPABASE:
                supabase_patch("clients", {"id": client_id}, {"password": new_password, "temp_password": ""})
            else:
                data["clients"][i]["password"] = new_password
                data["clients"][i]["temp_password"] = ""
                save_clients(data)
            return {"status": "ok", "message": "Password berjaya ditukar!"}
    raise HTTPException(status_code=404, detail="Client not found")

@app.post("/telegram/test")
def test_telegram():
    cfg = load_config()
    bot_token = cfg.get("bot_token","")
    chat_id = cfg.get("chat_id","")
    if not bot_token or not chat_id:
        raise HTTPException(status_code=400, detail="Bot token atau chat ID belum set")
    msg = f"✅ EZMeta test alert!\n🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}\n🚀 Server running"
    result = send_telegram(bot_token, chat_id, msg)
    return {"status":"ok","telegram":result}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8888)
