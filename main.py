import os
import json
import google.generativeai as genai
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import Response, JSONResponse

# ------------------------------------------------------------
# Konfiguration
# ------------------------------------------------------------
PORT = int(os.getenv("PORT", "8080"))

# Dein Modell aus deinem Code beibehalten:
# (Wenn nicht gesetzt, nehmen wir deinen Wert als Default.)
MODEL_NAME = os.getenv(
    "MODEL",
    "gemini-2.5-flash-preview-native-audio-dialog"
)

# Optional feste Public-URL (z. B. https://<app>.fly.dev). Wenn leer, wird Host aus Request genommen.
PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip()

# Begrüßung (Conversation Relay liest das vor, TTS macht Twilio)
WELCOME_GREETING = os.getenv(
    "WELCOME_GREETING",
    "Hallo! Ich bin Ihr Sprachassistent. Wie kann ich helfen?"
)

# TTS-Provider & Voice für Twilio Conversation Relay
# (Repo-Default war ElevenLabs; du kannst hier aber auch Google/Amazon nehmen)
TTS_PROVIDER = os.getenv("TTS_PROVIDER", "ElevenLabs")
TTS_VOICE = os.getenv("TTS_VOICE", "FGY2WhTYpPnrIDTdsKH5")

# Google API Key (AI Studio)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY (oder GEMINI_API_KEY) ist nicht gesetzt.")

# System-Prompt für Vocaris Solutions Voice-Agent
SYSTEM_PROMPT = os.getenv("SYSTEM_PROMPT", """\
Rolle: Offizieller Voice-Agent von Vocarís Solutions.

Auftrag: Fragen beantworten & überzeugen, Nutzen klar machen und aktiv nach einem Termin zur Beratung fragen.

Harte Fakten (nur diese nennen):

Terminbuchung: Kalender auf Website – Mo–Fr 07:00–15:00, Sa 07:00–13:00

E-Mail: info@vocaris-solutions.de

Preise (monatlich):
- Starter: 3.000 Min / €300 (≈10 ct/Min)
- Professional: 6.000 Min / €500 (≈8,3 ct/Min) – beliebtester Plan
- Enterprise: 10.000 Min / €750 (≈7,5 ct/Min)
- Überkontingent: 0,10 €/Min; keine Einrichtungskosten, keine Mindestlaufzeit, monatlich kündbar.

Unser Team entwickelt individuelle Voice-Agenten für Ihr Unternehmen mit Verknüpfung beliebiger Dienste. Beispiele: Buchungssysteme, SMS, Email, CRM-Systeme, Datenbanken, Termine buchen, Leads qualifizieren und vieles mehr.

Nutzen: 24/7 erreichbar, Fallback für Sicherheit, automatische Buchungen & Sofortantworten, bis zu 85% Kostensenkung (je nach Einsatz), weniger verpasste Anrufe/Wartezeiten, höhere Conversion. Kunde entscheidet selbst, ob der Voice-Agent nur bei besetzter Leitung Anrufe entgegennehmen soll oder alle Anrufe. Unser Team kümmert sich um Telefonnummer, Anbindung etc., sodass der Kunde selbst nichts mehr machen muss.

Fähigkeiten: 24+ Sprachen (Auto-Erkennung), 30+ Stimmen, <300 ms Antwortlatenz, sehr natürliche Stimme.

Sicherheit: DSGVO-konform, EU-Rechenzentren, End-to-End-Verschlüsselung, Enterprise-Standards.

Service: Beratung → maßgeschneiderte Entwicklung → kostenfreie Nummer & Setup → API-Integration → Tests → Go-Live → 24/7 Support.

Sprich ausschließlich Deutsch. Antworte präzise und überzeugend. Frage aktiv nach einem Beratungstermin.
""")

# ------------------------------------------------------------
# Google Generative AI initialisieren
# ------------------------------------------------------------
genai.configure(api_key=GOOGLE_API_KEY)

# Wichtig: Wir respektieren deinen Modellnamen.
# Hinweis: Falls das Modell in der verwendeten Python-Bibliothek nicht existiert,
# kommt ein API-Fehler zur Laufzeit – das ist bewusst so, denn „MODEL“ soll
# exakt aus deinem Code übernommen werden.
model = genai.GenerativeModel(
    model_name=MODEL_NAME,
    system_instruction=SYSTEM_PROMPT
)

# Offene Chat-Sessions je Call (Conversation Relay sendet einen Call-Kontext)
sessions = {}

app = FastAPI()


async def gemini_response(chat_session, user_prompt: str) -> str:
    """
    Holt eine Antwort vom Gemini-Modell.
    Conversation Relay liefert bereits die Transkription (Text).
    Wir generieren Text zurück (Speech übernimmt Twilio).
    """
    # Async API verwenden, wenn verfügbar:
    if hasattr(chat_session, "send_message_async"):
        resp = await chat_session.send_message_async(user_prompt)
    else:
        # Fallback synchron (selten nötig)
        resp = chat_session.send_message(user_prompt)
    # Unterschiedliche Response-Objekte je Bibliotheksversion:
    if hasattr(resp, "text"):
        return resp.text or ""
    if isinstance(resp, str):
        return resp
    return str(resp)


def _compute_ws_url_from_request(req: Request) -> str:
    """
    Baue die WSS-URL für Conversation Relay.
    Bevorzugt PUBLIC_URL, sonst Host-Header von Fly/Proxy.
    """
    if PUBLIC_URL:
        base = PUBLIC_URL
    else:
        # X-Forwarded-Host (Fly), sonst Request-Host
        host = req.headers.get("x-forwarded-host") or req.url.hostname
        scheme = "https"
        base = f"{scheme}://{host}"
    # CR erwartet WSS:
    # Fly stellt automatisch TLS bereit; gleiche Domain, Pfad /ws
    ws_url = base.replace("http://", "https://").replace("https://", "wss://") + "/ws"
    return ws_url


# ------------------------------------------------------------
# Twilio: TwiML-Endpoint
# ------------------------------------------------------------
#
# Wichtig: Twilio ruft POST /twiml auf. Wir geben TwiML zurück, das den
# Conversation Relay WebSocket verbindet.
@app.post("/twiml")
async def twiml_endpoint(request: Request):
    """
    Liefert TwiML, das Twilio Conversation Relay auf unseren WebSocket zeigt.
    """
    ws_url = _compute_ws_url_from_request(request)

    # ConversationRelay: Twilio übernimmt STT + TTS.
    # Wir liefern Text über den WS zurück (type="text", token="...", last=true).
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <ConversationRelay
      url="{ws_url}"
      welcomeGreeting="{WELCOME_GREETING}"
      ttsProvider="{TTS_PROVIDER}"
      voice="{TTS_VOICE}" />
  </Connect>
</Response>"""
    return Response(content=xml, media_type="text/xml")


# Healthcheck (praktisch für Fly)
@app.get("/health")
async def health():
    return JSONResponse(
        {
            "ok": True,
            "service": "Twilio Conversation Relay + Gemini",
            "model": MODEL_NAME,
        }
    )


# ------------------------------------------------------------
# Twilio: WebSocket-Endpoint für Conversation Relay
# ------------------------------------------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Conversation Relay schickt JSON-Events:
      - setup: { callSid, ... }
      - prompt: { voicePrompt: "..." }  (finale User-Transkription)
      - interrupt, user_speech_started, user_speech_stopped, ...
    Wir antworten mit:
      - { "type": "text", "token": "...", "last": true }
        (Twilio macht TTS und spielt ab)
    """
    await websocket.accept()
    call_sid = None

    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)

            mtype = msg.get("type")
            if mtype == "setup":
                call_sid = msg.get("callSid") or call_sid
                # pro Call neue Chat-Session
                sessions[call_sid] = model.start_chat(history=[])
                print(f"[{call_sid}] setup – Session gestartet")

            elif mtype == "prompt":
                if not call_sid or call_sid not in sessions:
                    print(f"[?] prompt ohne gültige Session, wird ignoriert")
                    continue
                user_prompt = msg.get("voicePrompt") or ""
                print(f"[{call_sid}] prompt: {user_prompt!r}")

                chat_session = sessions[call_sid]
                try:
                    answer = await gemini_response(chat_session, user_prompt)
                except Exception as e:
                    print(f"[{call_sid}] Gemini-Fehler: {e}")
                    answer = "Entschuldigung, hier ist etwas schiefgelaufen."

                # Vollständige Textantwort an Twilio (CR macht TTS)
                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "text",
                            "token": answer,
                            "last": True,  # komplette Antwort (keine Token-Streams nötig)
                        }
                    )
                )
                print(f"[{call_sid}] → {answer!r}")

            elif mtype in ("interrupt", "user_speech_started", "user_speech_stopped"):
                # Optionales Logging; CR steuert Pausen/Barge-in
                print(f"[{call_sid}] event: {mtype}")

            else:
                # Unbekannte/optionale Events ignorieren
                print(f"[{call_sid}] unknown: {mtype} – payload: {msg}")

    except WebSocketDisconnect:
        print(f"[{call_sid}] websocket closed")
        if call_sid and call_sid in sessions:
            sessions.pop(call_sid, None)
            print(f"[{call_sid}] session removed")


