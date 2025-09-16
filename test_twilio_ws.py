import asyncio
import json
import os

import ssl
import websockets


WS_URL = os.getenv("WS_URL", "wss://damp-fire-3341.fly.dev/ws")


async def main():
    # Twilio Media Streams Subprotokoll
    ssl_ctx = ssl.create_default_context()
    # Nur für Testzwecke: Zertifikatsprüfung deaktivieren
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    async with websockets.connect(
        WS_URL,
        subprotocols=["vnd.twilio.media.streams", "audio.twilio.com"],
        ssl=ssl_ctx,
        open_timeout=20,
        close_timeout=5,
        max_size=None,
    ) as ws:
        print("connected")

        # 1) connected event
        connected = {"event": "connected", "protocol": "Call", "version": "1.0"}
        await ws.send(json.dumps(connected))
        print("sent connected")

        # 2) start event mit stream/call ids und media format (PCMU 8kHz mono)
        start = {
            "event": "start",
            "start": {
                "accountSid": os.getenv("TWILIO_ACCOUNT_SID", "AC_test"),
                "streamSid": "MZ00000000000000000000000000000000",
                "callSid": "CA_test",
                "mediaFormat": {"encoding": "audio/x-mulaw", "sampleRate": 8000, "channels": 1},
            },
        }
        await ws.send(json.dumps(start))
        print("sent start")

        # Antworten lesen (einige Frames), danach schließen
        for i in range(10):
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                print("timeout waiting for message")
                break
            if isinstance(msg, (bytes, bytearray)):
                print(f"recv binary len={len(msg)}")
            else:
                # text
                print(f"recv text: {msg[:200]}")


if __name__ == "__main__":
    asyncio.run(main())


