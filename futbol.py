Ejecutar:  python futbol.py
Abrir DT:  http://TU_IP:8000
Jugador:   http://TU_IP:8000/player?id=9   (cambiar el 9 por el dorsal)
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, Response
from typing import Dict, List
from datetime import datetime
import sqlite3, os, json, socket, uvicorn

app = FastAPI()
DB = "futbol.db"

# ---------- BASE DE DATOS ----------
def init_db():
    con = sqlite3.connect(DB)
    con.executescript("""
    CREATE TABLE IF NOT EXISTS partidos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rival TEXT, fecha TEXT,
        goles INTEGER DEFAULT 0, errados INTEGER DEFAULT 0, tiros INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS posiciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        partido_id INTEGER, jugador TEXT, x REAL, y REAL, timestamp TEXT);
    CREATE TABLE IF NOT EXISTS movimientos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        partido_id INTEGER, jugador TEXT,
        direccion TEXT, magnitud REAL, x REAL, y REAL, timestamp TEXT);
    CREATE TABLE IF NOT EXISTS desmarques (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        partido_id INTEGER, jugador TEXT, tipo TEXT, x REAL, y REAL, timestamp TEXT);
    CREATE TABLE IF NOT EXISTS eventos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        partido_id INTEGER, jugador TEXT, tipo TEXT, timestamp TEXT);
    """)
    con.commit(); con.close()

init_db()

def db_exec(q, p=()):
    con = sqlite3.connect(DB); cur = con.cursor()
    cur.execute(q, p); con.commit(); last = cur.lastrowid; con.close()
    return last

def db_query(q, p=()):
    con = sqlite3.connect(DB); con.row_factory = sqlite3.Row
    cur = con.cursor(); cur.execute(q, p)
    rows = [dict(r) for r in cur.fetchall()]; con.close()
    return rows

# ---------- ESTADO ----------
players: Dict[str, dict] = {}
current_match_id = None
coach_connections: List[WebSocket] = []

def nuevo_jugador(pid):
    return {"id": pid, "position": {"x": 50, "y": 50}, "history": [],
            "movements": [], "desmarques": [],
            "goles": 0, "errados": 0, "tiros": 0}

# ---------- VISTAS ----------
@app.get("/")
async def coach_view():
    return HTMLResponse(COACH_HTML)

@app.get("/player")
async def player_view():
    return HTMLResponse(PLAYER_HTML)

@app.get("/manifest.json")
async def manifest():
    return Response(MANIFEST, media_type="application/manifest+json")

@app.get("/sw.js")
async def sw():
    return Response(SW, media_type="application/javascript")

# ---------- WEBSOCKET DT ----------
@app.websocket("/ws/coach")
async def coach_ws(ws: WebSocket):
    await ws.accept()
    coach_connections.append(ws)
    await ws.send_json({"type": "init", "players": players, "match_id": current_match_id})
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        if ws in coach_connections: coach_connections.remove(ws)

async def broadcast(msg):
    for ws in list(coach_connections):
        try: await ws.send_json(msg)
        except: pass

# ---------- WEBSOCKET JUGADOR ----------
@app.websocket("/ws/player/{player_id}")
async def player_ws(ws: WebSocket, player_id: str):
    await ws.accept()
    if player_id not in players:
        players[player_id] = nuevo_jugador(player_id)
    try:
        while True:
            data = json.loads(await ws.receive_text())
            x, y = float(data["x"]), float(data["y"])
            ts = data.get("timestamp", datetime.now().isoformat())
            p = players[player_id]
            dx = x - p["position"]["x"]

            if current_match_id and abs(dx) >= 2:
                direccion = "derecha->izquierda" if dx < 0 else "izquierda->derecha"
                p["movements"].append({"direccion": direccion,
                    "magnitud": round(abs(dx), 1), "x": x, "y": y, "timestamp": ts})
                db_exec("""INSERT INTO movimientos
                    (partido_id,jugador,direccion,magnitud,x,y,timestamp)
                    VALUES (?,?,?,?,?,?,?)""",
                    (current_match_id, player_id, direccion, round(abs(dx),1), x, y, ts))

            p["position"] = {"x": x, "y": y}
            p["history"].append({"x": x, "y": y})
            if current_match_id:
                db_exec("""INSERT INTO posiciones
                    (partido_id,jugador,x,y,timestamp) VALUES (?,?,?,?,?)""",
                    (current_match_id, player_id, x, y, ts))

            await broadcast({"type": "position", "player_id": player_id, "player": p})
    except WebSocketDisconnect:
        pass

# ---------- API ----------
@app.post("/partido/iniciar")
async def iniciar_partido(req: Request):
    global current_match_id
    data = await req.json()
    current_match_id = db_exec("INSERT INTO partidos (rival,fecha) VALUES (?,?)",
        (data.get("rival","Sin rival"), datetime.now().isoformat()))
    for pid in players:
        players[pid].update({"history": [], "movements": [], "desmarques": [],
            "goles": 0, "errados": 0, "tiros": 0, "position": {"x": 50, "y": 50}})
    await broadcast({"type": "new_match", "match_id": current_match_id, "players": players})
    return {"match_id": current_match_id}

@app.post("/evento")
async def evento(req: Request):
    if not current_match_id: return {"error": "sin partido"}
    data = await req.json()
    pid = str(data["jugador"]); tipo = data["tipo"]
    ts = datetime.now().isoformat()
    if pid not in players: players[pid] = nuevo_jugador(pid)
    p = players[pid]

    if tipo == "desmarque":
        entry = {"tipo": data.get("subtipo","-"),
                 "x": p["position"]["x"], "y": p["position"]["y"], "timestamp": ts}
        p["desmarques"].append(entry)
        db_exec("INSERT INTO desmarques (partido_id,jugador,tipo,x,y,timestamp) VALUES (?,?,?,?,?,?)",
                (current_match_id, pid, entry["tipo"], entry["x"], entry["y"], ts))
    elif tipo in ("gol","errado"):
        if tipo == "gol": p["goles"] += 1
        else: p["errados"] += 1
        p["tiros"] += 1
        db_exec("INSERT INTO eventos (partido_id,jugador,tipo,timestamp) VALUES (?,?,?,?)",
                (current_match_id, pid, tipo, ts))
    await broadcast({"type": "event", "player_id": pid, "player": p})
    return {"status": "ok"}

@app.get("/historial")
async def historial():
    return db_query("SELECT * FROM partidos ORDER BY fecha DESC")

@app.get("/partido/{pid}/stats")
async def stats(pid: int, jugador: str = "9"):
    partido = db_query("SELECT * FROM partidos WHERE id=?", (pid,))
    movs = db_query("SELECT * FROM movimientos WHERE partido_id=? AND jugador=?", (pid, jugador))
    desm = db_query("SELECT * FROM desmarques WHERE partido_id=? AND jugador=?", (pid, jugador))
    pos  = db_query("SELECT * FROM posiciones  WHERE partido_id=? AND jugador=?", (pid, jugador))
    izq = sum(1 for m in movs if m["direccion"].endswith("izquierda"))
    return {"partido": partido[0] if partido else None,
            "movimientos_total": len(movs),
            "mov_der_a_izq": izq, "mov_izq_a_der": len(movs)-izq,
            "desmarques_por_tipo": {t: sum(1 for d in desm if d["tipo"]==t)
                                    for t in set(d["tipo"] for d in desm)} if desm else {},
            "mapa_calor": [{"x":p["x"],"y":p["y"]} for p in pos]}

@app.get("/comparar")
async def comparar(ids: str, jugador: str = "9"):
    return [await stats(int(i), jugador) for i in ids.split(",")]

@app.get("/partido/{pid}/pdf")
async def pdf(pid: int, jugador: str = "9"):
    s = await stats(pid, jugador)
    p = s["partido"]
    ef = round(p['goles']/p['tiros']*100) if p['tiros'] else 0
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <title>Informe jugador {jugador}</title>
    <style>body{{font-family:sans-serif;max-width:700px;margin:30px auto;padding:20px}}
    h1{{border-bottom:3px solid #4caf50;padding-bottom:8px}}
    .stat{{display:inline-block;margin:10px 20px 10px 0;text-align:center}}
    .stat b{{font-size:32px;color:#4caf50;display:block}}
    table{{width:100%;border-collapse:collapse;margin-top:15px}}
    th,td{{border:1px solid #ccc;padding:8px;text-align:left}}
    th{{background:#f0f0f0}}
    @media print{{button{{display:none}}}}</style></head><body>
    <h1>⚽ Informe del jugador {jugador}</h1>
    <p><b>Partido:</b> vs {p['rival']} — {p['fecha'][:16].replace('T',' ')}</p>
    <div class="stat"><b>{p['goles']}</b>Goles</div>
    <div class="stat"><b>{p['errados']}</b>Errados</div>
    <div class="stat"><b>{p['tiros']}</b>Tiros</div>
    <div class="stat"><b>{ef}%</b>Efectividad</div>
    <h2>Movimientos laterales</h2>
    <p>Total: <b>{s['movimientos_total']}</b> —
       →izquierda: <b>{s['mov_der_a_izq']}</b> —
       →derecha: <b>{s['mov_izq_a_der']}</b></p>
    <h2>Desmarques</h2>
    <table><tr><th>Tipo</th><th>Cantidad</th></tr>
    {''.join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k,v in s['desmarques_por_tipo'].items())}
    </table>
    <br><button onclick="window.print()">🖨️ Imprimir / Guardar PDF</button>
    </body></html>"""
    return HTMLResponse(html)

# ---------- HTML EMBEBIDO ----------
COACH_HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Panel DT</title>
<style>
body{font-family:sans-serif;margin:10px;background:#1a1a1a;color:#eee}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.card{background:#2a2a2a;padding:12px;border-radius:8px}
canvas{background:#2e7d32;border-radius:6px;width:100%}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:5px;border-bottom:1px solid #444;text-align:left}
.stat{font-size:24px;font-weight:bold;color:#4caf50}
.stat.err{color:#f44336}
.btn{padding:8px 12px;margin:2px;border:none;border-radius:6px;cursor:pointer;
     font-weight:bold;color:white;font-size:13px}
.btn-desm{background:#2196f3}.btn-gol{background:#4caf50}
.btn-err{background:#f44336}.btn-main{background:#ff9800;color:black}
input,select{padding:8px;border-radius:6px;border:1px solid #555;
             background:#333;color:white;margin:2px}
@media(max-width:700px){.grid{grid-template-columns:1fr}}
</style></head><body>
<h1>⚽ Panel DT</h1>
<div class="card">
<input id="rival" placeholder="Rival">
<button class="btn btn-main" onclick="iniciar()">▶ Iniciar partido</button>
<select id="focus">
<option value="9">Jugador 9</option><option value="7">Jugador 7</option>
<option value="10">Jugador 10</option><option value="11">Jugador 11</option>
<option value="4">Jugador 4</option><option value="5">Jugador 5</option>
</select>
<span id="match_info">Sin partido</span>
</div>
<div class="grid" style="margin-top:10px">
<div class="card"><h3>Cancha en vivo</h3>
<canvas id="field" width="320" height="480"></canvas></div>
<div class="card"><h3>Eventos del jugador <span id="focusLbl">9</span></h3>
<button class="btn btn-desm" onclick="ev('desmarque','ruptura')">Desmarque ruptura</button>
<button class="btn btn-desm" onclick="ev('desmarque','apoyo')">Desmarque apoyo</button>
<button class="btn btn-desm" onclick="ev('desmarque','espacio')">Desmarque al espacio</button>
<br><button class="btn btn-gol" onclick="ev('gol')">⚽ GOL</button>
<button class="btn btn-err" onclick="ev('errado')">❌ Errado</button>
<h3>Stats</h3>
<div style="display:flex;gap:15px;flex-wrap:wrap">
<div><div class="stat" id="g">0</div><small>Goles</small></div>
<div><div class="stat err" id="e">0</div><small>Errados</small></div>
<div><div class="stat" id="t">0</div><small>Tiros</small></div>
<div><div class="stat" id="ef">0%</div><small>Efectividad</small></div>
</div>
<h4>Jugadores conectados</h4>
<table id="players"><thead><tr><th>ID</th><th>Pos</th></tr></thead><tbody></tbody></table>
</div>
<div class="card" style="grid-column:span 2">
<h3>Historial</h3>
<button class="btn btn-main" onclick="cargarHist()">🔄 Actualizar</button>
<button class="btn btn-main" onclick="comparar()">📊 Comparar</button>
<button class="btn btn-main" onclick="pdf()">📄 Informe</button>
<table id="hist"><thead><tr><th>Sel</th><th>ID</th><th>Rival</th>
<th>Fecha</th><th>G</th><th>E</th><th>T</th></tr></thead><tbody></tbody></table>
<div id="comparacion"></div>
</div>
</div>
<script>
let players={},matchId=null;
const canvas=document.getElementById('field'),ctx=canvas.getContext('2d');
const ws=new WebSocket((location.protocol==='https:'?'wss':'ws')+'://'+location.host+'/ws/coach');
ws.onmessage=(e)=>{const m=JSON.parse(e.data);
if(m.type==='init'){players=m.players;matchId=m.match_id;render();}
else if(m.type==='new_match'){players=m.players;matchId=m.match_id;
document.getElementById('match_info').textContent='Partido #'+matchId;render();}
else if(m.type==='position'||m.type==='event'){players[m.player_id]=m.player;render();}};
document.getElementById('focus').onchange=e=>{
document.getElementById('focusLbl').textContent=e.target.value;render();};
function render(){
const tb=document.querySelector('#players tbody');
tb.innerHTML=Object.values(players).map(p=>'<tr><td>'+p.id+'</td><td>('+
p.position.x.toFixed(0)+','+p.position.y.toFixed(0)+')</td></tr>').join('');
const focus=document.getElementById('focus').value,p=players[focus];
if(p){document.getElementById('g').textContent=p.goles;
document.getElementById('e').textContent=p.errados;
document.getElementById('t').textContent=p.tiros;
document.getElementById('ef').textContent=(p.tiros?Math.round(p.goles/p.tiros*100):0)+'%';}
ctx.clearRect(0,0,320,480);ctx.strokeStyle='#fff';ctx.lineWidth=2;
ctx.strokeRect(10,10,300,460);
ctx.beginPath();ctx.moveTo(10,240);ctx.lineTo(310,240);ctx.stroke();
ctx.beginPath();ctx.arc(160,240,45,0,2*Math.PI);ctx.stroke();
ctx.strokeRect(80,10,160,65);ctx.strokeRect(80,405,160,65);
for(const j of Object.values(players)){
const cx=10+(j.position.x/100)*300,cy=10+((100-j.position.y)/100)*460;
const f=j.id===focus;
if(f&&j.history){ctx.fillStyle='rgba(255,235,59,0.15)';
j.history.slice(-100).forEach(h=>{const hx=10+(h.x/100)*300,hy=10+((100-h.y)/100)*460;
ctx.beginPath();ctx.arc(hx,hy,3,0,2*Math.PI);ctx.fill();});}
ctx.fillStyle=f?'#ffeb3b':'#42a5f5';
ctx.beginPath();ctx.arc(cx,cy,f?9:6,0,2*Math.PI);ctx.fill();
ctx.fillStyle=f?'#000':'#fff';ctx.font='bold 10px sans-serif';
ctx.fillText(j.id,cx-4,cy+3);}}
async function iniciar(){const rival=document.getElementById('rival').value||'Sin rival';
await fetch('/partido/iniciar',{method:'POST',headers:{'Content-Type':'application/json'},
body:JSON.stringify({rival})});}
async function ev(tipo,subtipo){const jugador=document.getElementById('focus').value;
await fetch('/evento',{method:'POST',headers:{'Content-Type':'application/json'},
body:JSON.stringify({jugador,tipo,subtipo})});}
async function cargarHist(){const data=await(await fetch('/historial')).json();
document.querySelector('#hist tbody').innerHTML=data.map(p=>'<tr><td><input type="checkbox" value="'+p.id+'"></td><td>'+p.id+'</td><td>'+p.rival+'</td><td>'+
p.fecha.slice(0,16).replace('T',' ')+'</td><td>'+p.goles+'</td><td>'+p.errados+'</td><td>'+p.tiros+'</td></tr>').join('');}
async function comparar(){const ids=[...document.querySelectorAll('#hist input:checked')].map(c=>c.value).join(',');
const jugador=document.getElementById('focus').value;
if(!ids)return alert('Selecciona partidos');
const data=await(await fetch('/comparar?ids='+ids+'&jugador='+jugador)).json();
document.getElementById('comparacion').innerHTML=data.map(d=>'<div style="background:#333;padding:8px;border-radius:6px;margin-top:8px"><b>#'+
d.partido.id+' vs '+d.partido.rival+'</b> ('+d.partido.fecha.slice(0,10)+')<br>G:'+d.partido.goles+' E:'+d.partido.errados+
' | Movs:'+d.movimientos_total+' (→izq '+d.mov_der_a_izq+' / →der '+d.mov_izq_a_der+')<br>Desmarques: '+
JSON.stringify(d.desmarques_por_tipo)+'</div>').join('');}
function pdf(){const jugador=document.getElementById('focus').value;
if(!matchId)return alert('Sin partido');
window.open('/partido/'+matchId+'/pdf?jugador='+jugador,'_blank');}
cargarHist();
</script></body></html>"""

PLAYER_HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no">
<meta name="theme-color" content="#0a0a0a">
<link rel="manifest" href="/manifest.json">
<title>Jugador GPS</title>
<style>
body{font-family:sans-serif;background:#0a0a0a;color:#eee;margin:0;padding:20px;text-align:center}
h1{font-size:22px}
.big{font-size:60px;font-weight:bold;color:#4caf50;margin:10px}
.info{background:#1a1a1a;padding:12px;border-radius:8px;margin:8px 0;font-size:14px;text-align:left}
button{padding:14px 20px;font-size:16px;border-radius:8px;border:none;
       background:#4caf50;color:white;font-weight:bold;margin:6px}
button.stop{background:#f44336}
.dot{display:inline-block;width:12px;height:12px;border-radius:50%;background:#666;margin-right:6px}
.dot.on{background:#4caf50;animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
</style></head><body>
<h1>⚽ Jugador <span id="pid">?</span></h1>
<div><span class="dot" id="status"></span><span id="statusText">Desconectado</span></div>
<div class="big" id="dist">0</div><div>metros recorridos</div>
<div class="info">
<b>Posición:</b> <span id="pos">-</span><br>
<b>Precisión GPS:</b> <span id="acc">-</span> m<br>
<b>Velocidad:</b> <span id="vel">-</span> km/h<br>
<b>Última act.:</b> <span id="ts">-</span>
</div>
<button id="start">▶ Iniciar</button>
<button id="stop" class="stop">⏸ Detener</button>
<script>
const params=new URLSearchParams(location.search);
const PID=params.get('id')||'9';
document.getElementById('pid').textContent=PID;
let ws=null,watchId=null,lastPos=null,totalDist=0,refA=null,refB=null;
function gpsToField(lat,lon){
if(!refA){refA={lat,lon};return{x:50,y:50};}
if(!refB){refB={lat,lon};return{x:50,y:50};}
const dLat=refB.lat-refA.lat,dLon=refB.lon-refA.lon;
const x=((lon-refA.lon)/dLon)*100,y=((lat-refA.lat)/dLat)*100;
return{x:Math.max(0,Math.min(100,x)),y:Math.max(0,Math.min(100,y))};}
function haversine(p1,p2){const R=6371000;
const f1=p1.lat*Math.PI/180,f2=p2.lat*Math.PI/180;
const df=(p2.lat-p1.lat)*Math.PI/180,dl=(p2.lon-p1.lon)*Math.PI/180;
const a=Math.sin(df/2)**2+Math.cos(f1)*Math.cos(f2)*Math.sin(dl/2)**2;
return 2*R*Math.asin(Math.sqrt(a));}
function conectarWS(){
const proto=location.protocol==='https:'?'wss':'ws';
ws=new WebSocket(proto+'://'+location.host+'/ws/player/'+PID);
ws.onopen=()=>{document.getElementById('status').classList.add('on');
document.getElementById('statusText').textContent='Conectado';};
ws.onclose=()=>{document.getElementById('status').classList.remove('on');
document.getElementById('statusText').textContent='Reconectando...';
setTimeout(conectarWS,2000);};}
function iniciarGPS(){
if(!navigator.geolocation)return alert('GPS no disponible');
watchId=navigator.geolocation.watchPosition((pos)=>{
const{latitude:lat,longitude:lon,accuracy,speed}=pos.coords;const t=pos.timestamp;
if(lastPos)totalDist+=haversine(lastPos,{lat,lon});
lastPos={lat,lon};
const field=gpsToField(lat,lon);
if(ws&&ws.readyState===1)ws.send(JSON.stringify({x:field.x,y:field.y,
lat,lon,accuracy,speed,timestamp:new Date(t).toISOString()}));
document.getElementById('dist').textContent=Math.round(totalDist);
document.getElementById('pos').textContent='('+field.x.toFixed(0)+','+field.y.toFixed(0)+')';
document.getElementById('acc').textContent=accuracy.toFixed(1);
document.getElementById('vel').textContent=speed?(speed*3.6).toFixed(1):'-';
document.getElementById('ts').textContent=new Date(t).toLocaleTimeString();},
(err)=>alert('Error GPS: '+err.message),
{enableHighAccuracy:true,maximumAge:0,timeout:10000});}
function detenerGPS(){if(watchId!==null)navigator.geolocation.clearWatch(watchId);watchId=null;}
document.getElementById('start').onclick=()=>{if(!ws||ws.readyState!==1)conectarWS();iniciarGPS();};
document.getElementById('stop').onclick=detenerGPS;
conectarWS();
if('serviceWorker'in navigator)navigator.serviceWorker.register('/sw.js');
</script></
