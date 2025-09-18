import argparse
import asyncio
import os
import time
from typing import Any, Dict, Optional

from urllib.parse import quote
=======


import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from starlette.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv

load_dotenv(override=True)

app = FastAPI()
app.state.start_ts = time.time()
app.state.telnyx_stream_started: set[str] = set()

PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip()
TELEPHONY_WS_URL = os.getenv("TELEPHONY_WS_URL", "").strip()
TELNYX_API_KEY = os.getenv("TELNYX_API_KEY", "").strip()
TELNYX_API_BASE_URL = os.getenv("TELNYX_API_BASE_URL", "https://api.telnyx.com/v2").rstrip("/")
TELNYX_STREAM_TRACK = (os.getenv("TELNYX_STREAM_TRACK", "both") or "both").strip() or "both"
TELNYX_STREAM_TYPE = (os.getenv("TELNYX_STREAM_TYPE", "audio") or "audio").strip() or "audio"
TELNYX_AUTO_ANSWER = os.getenv("TELNYX_AUTO_ANSWER", "true").lower() not in {"false", "0", "no"}
TELNYX_TIMEOUT = httpx.Timeout(10.0)

PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip()
TELEPHONY_WS_URL = os.getenv("TELEPHONY_WS_URL", "").strip()
TELNYX_API_KEY = os.getenv("TELNYX_API_KEY", "").strip()
TELNYX_API_BASE_URL = os.getenv("TELNYX_API_BASE_URL", "https://api.telnyx.com/v2").rstrip("/")
TELNYX_STREAM_TRACK = (os.getenv("TELNYX_STREAM_TRACK", "both") or "both").strip() or "both"
TELNYX_STREAM_TYPE = (os.getenv("TELNYX_STREAM_TYPE", "audio") or "audio").strip() or "audio"
TELNYX_AUTO_ANSWER = os.getenv("TELNYX_AUTO_ANSWER", "true").lower() not in {"false", "0", "no"}
TELNYX_TIMEOUT = httpx.Timeout(10.0)

# Konfigurierbare Ready-Verzögerung (Sekunden), um Kaltstart abzufangen
READY_DELAY_SEC = float(os.getenv("TWI_ML_READY_DELAY_SEC", "3"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_ws_url(host: Optional[str]) -> str:
    if TELEPHONY_WS_URL:
        return TELEPHONY_WS_URL

    base = PUBLIC_URL
    env = os.getenv("ENV", "local").lower()
    if not base:
        if not host:
            raise HTTPException(status_code=400, detail="Host-Header fehlt")
        scheme = "http" if env == "local" else "https"
        base = f"{scheme}://{host}"

    base = base.rstrip("/")
    if base.startswith("ws://") or base.startswith("wss://"):
        ws_base = base
    elif base.startswith("http://"):
        ws_base = "ws://" + base[len("http://") :]
    elif base.startswith("https://"):
        ws_base = "wss://" + base[len("https://") :]
    else:
        ws_base = f"wss://{base.lstrip('/')}"

    return f"{ws_base}/ws"


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


def _twiml(host: Optional[str], from_number: str, to_number: str) -> str:
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


def _extract_request_host(request: Request) -> Optional[str]:
    return request.headers.get("x-forwarded-host") or request.headers.get("host")


def _is_telnyx_request(request: Request) -> bool:
    content_type = (request.headers.get("content-type") or "").lower()
    if request.headers.get("telnyx-signature-ed25519"):
        return True
    return "application/json" in content_type


async def _telnyx_call_control_action(
    call_control_id: str,
    action: str,
    payload: Optional[Dict[str, Any]] = None,
) -> bool:
    if not TELNYX_API_KEY:
        logger.warning(
            "Telnyx-Aktion {} übersprungen: TELNYX_API_KEY ist nicht gesetzt.",
            action,
        )
        return False


    encoded_call_control_id = quote(str(call_control_id), safe="")
    url = f"{TELNYX_API_BASE_URL}/calls/{encoded_call_control_id}/actions/{action}"
=======
    url = f"{TELNYX_API_BASE_URL}/calls/{call_control_id}/actions/{action}"

    headers = {
        "Authorization": f"Bearer {TELNYX_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=TELNYX_TIMEOUT) as client:
            response = await client.post(url, json=payload or {}, headers=headers)
    except httpx.HTTPError as exc:
        logger.exception("Telnyx-Aktion {} fehlgeschlagen: {}", action, exc)
        return False

    if response.status_code >= 300:
        logger.error(
            "Telnyx-Aktion {} fehlgeschlagen ({}): {}",
            action,
            response.status_code,
            response.text,
        )
        return False

    return True


async def _telnyx_start_stream(payload: Dict[str, Any], host: Optional[str]) -> None:
    call_control_id = payload.get("call_control_id")
    if not call_control_id:
        logger.warning("Telnyx-Event ohne call_control_id: {}", payload)
        return


    started_set = getattr(app.state, "telnyx_stream_started", set())
    if call_control_id in started_set:
        logger.debug("Telnyx-Stream für {} wurde bereits angefordert", call_control_id)
        return

=======

    try:
        ws_url = _get_ws_url(host)
    except HTTPException as exc:
        logger.error(
            "Telnyx-Stream kann nicht gestartet werden: {}",
            exc.detail if hasattr(exc, "detail") else exc,
        )
        return

    logger.info("Starte Telnyx-Stream {} -> {}", call_control_id, ws_url)


=======
    if TELNYX_AUTO_ANSWER:
        answered = await _telnyx_call_control_action(call_control_id, "answer")
        if not answered:
            return


    stream_payload: Dict[str, Any] = {
        "stream_url": ws_url,
        "stream_track": TELNYX_STREAM_TRACK,
        "stream_type": TELNYX_STREAM_TYPE,
    }

    started = await _telnyx_call_control_action(
=======
    await _telnyx_call_control_action(

        call_control_id,
        "stream_start",
        stream_payload,
    )

    if started:
        started_set.add(call_control_id)
=======


@app.get("/health")
async def health():
    return JSONResponse(
        {
            "ok": True,
            "service": "pipecat-telephony-gemini",
            "providers": [
                provider
                for provider in ["telnyx" if TELNYX_API_KEY else None, "twilio"]
                if provider
            ],
        }
    )


async def _handle_twilio_webhook(request: Request) -> HTMLResponse:
    form = await request.form()
    from_number = form.get("From", "")
    to_number = form.get("To", "")

    host = _extract_request_host(request)
    if not host and not (PUBLIC_URL or TELEPHONY_WS_URL):
        raise HTTPException(status_code=400, detail="Host-Header fehlt")

    if (time.time() - app.state.start_ts) < READY_DELAY_SEC:
        xml = _twiml_wait_and_redirect()
    else:
        xml = _twiml(host, from_number, to_number)
    return HTMLResponse(content=xml, media_type="application/xml")


async def _handle_telnyx_webhook(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except Exception as exc:
        logger.exception("Telnyx-Webhook konnte nicht geparst werden: {}", exc)
        raise HTTPException(status_code=400, detail="Ungültiges JSON von Telnyx")

    data = payload.get("data") or {}
    event_type = data.get("event_type") or payload.get("event_type")
    event_payload: Dict[str, Any] = data.get("payload") or {}
    call_control_id = event_payload.get("call_control_id")

    logger.info(
        "Telnyx-Webhook: {} (call_control_id: {})",
        event_type,
        call_control_id,
    )

    host = _extract_request_host(request)

    if event_type == "call.initiated":

        if TELNYX_AUTO_ANSWER:
            if call_control_id:
                asyncio.create_task(
                    _telnyx_call_control_action(call_control_id, "answer")
                )
            else:
                logger.warning(
                    "Telnyx call.initiated ohne call_control_id: {}", payload
                )
    elif event_type == "call.answered":
        asyncio.create_task(_telnyx_start_stream(event_payload, host))
    elif event_type == "call.hangup":
        logger.info("Telnyx-Hangup erhalten für {}", call_control_id)
        getattr(app.state, "telnyx_stream_started", set()).discard(call_control_id)
=======
        asyncio.create_task(_telnyx_start_stream(event_payload, host))
    elif event_type == "call.hangup":
        logger.info("Telnyx-Hangup erhalten für {}", call_control_id)

    elif event_type:
        logger.debug("Telnyx-Event {} empfangen", event_type)
    else:
        logger.warning("Telnyx-Event ohne event_type: {}", payload)

    return JSONResponse({"data": {"result": "ok"}})


@app.post("/")
async def telephony_webhook(request: Request):
    if _is_telnyx_request(request):
        return await _handle_telnyx_webhook(request)
    return await _handle_twilio_webhook(request)


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


