import argparse
import os
import time
from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv

load_dotenv(override=True)

app = FastAPI()
app.state.start_ts = time.time()

# Konfigurierbare Ready-Verzögerung (Sekunden), um Kaltstart abzufangen
READY_DELAY_SEC = float(os.getenv("TWI_ML_READY_DELAY_SEC", "3"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_ws_url(host: str) -> str:
    env = os.getenv("ENV", "local").lower()
    if env == "production":
        # Pipecat Cloud Pfad; lokal nutzen wir unseren eigenen WS
        return "wss://api.pipecat.daily.co/ws/twilio"
    return f"wss://{host}/ws"


def _build_parameters(from_number: str, to_number: str) -> list[str]:
    params: list[str] = []
    env = os.getenv("ENV", "local").lower()
    if env == "production":
        agent = os.getenv("AGENT_NAME")
        org = os.getenv("ORGANIZATION_NAME")
        if not agent or not org:
            raise HTTPException(
                status_code=500,
                detail="AGENT_NAME und ORGANIZATION_NAME müssen in production gesetzt sein",
            )
        service_host = f"{agent}.{org}"
        params.append(f'<Parameter name="_pipecatCloudServiceHost" value="{service_host}"/>')
    params.append(f'<Parameter name="from" value="{from_number}"/>')
    params.append(f'<Parameter name="to" value="{to_number}"/>')
    return params


def _twiml(host: str, from_number: str, to_number: str) -> str:
    ws = _get_ws_url(host)
    params = "\n      ".join(_build_parameters(from_number, to_number))
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Pause length="1"/>
  <Connect>
    <Stream url="{ws}">
      {params}
    </Stream>
  </Connect>
  <Pause length="20"/>
</Response>"""


def _twiml_wait_and_redirect() -> str:
    return """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<Response>
  <Pause length=\"1\"/>
  <Redirect method=\"POST\">/</Redirect>
  <Pause length=\"5\"/>
</Response>"""


@app.get("/health")
async def health():
    return JSONResponse({"ok": True, "service": "pipecat-twilio-gemini"})


@app.post("/")
async def twilio_webhook(request: Request):
    form = await request.form()
    call_sid = form.get("CallSid", "")
    from_number = form.get("From", "")
    to_number = form.get("To", "")

    host = request.headers.get("host")
    if not host:
        raise HTTPException(status_code=400, detail="Host-Header fehlt")

    # Health-Gate: innerhalb der ersten READY_DELAY_SEC Sekunden nur warten/redirecten
    if (time.time() - app.state.start_ts) < READY_DELAY_SEC:
        xml = _twiml_wait_and_redirect()
    else:
        xml = _twiml(host, from_number, to_number)
    return HTMLResponse(content=xml, media_type="application/xml")


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    # Twilio sendet ein Subprotokoll (z. B. vnd.twilio.media.streams). Explizit akzeptieren.
    offered = websocket.headers.get("sec-websocket-protocol", "")
    subprotocol = None
    if "vnd.twilio.media.streams" in offered:
        subprotocol = "vnd.twilio.media.streams"
    elif offered:
        subprotocol = offered.split(",")[0].strip()
    await websocket.accept(subprotocol=subprotocol)
    try:
        from bot import bot
        from pipecat.runner.types import WebSocketRunnerArguments

        runner_args = WebSocketRunnerArguments(websocket=websocket)
        runner_args.handle_sigint = False

        env = os.getenv("ENV", "local").lower()
        if env == "local":
            await bot(runner_args, False)
        else:
            await bot(runner_args)
    except Exception:
        return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--test", action="store_true", default=False)
    args, _ = parser.parse_known_args()
    app.state.testing = args.test

    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))


