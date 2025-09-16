import os
from dotenv import load_dotenv
from loguru import logger
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import parse_telephony_websocket
from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.services.gemini_multimodal_live import GeminiMultimodalLiveLLMService
from pipecat.services.gemini_multimodal_live.gemini import (
    GeminiMultimodalModalities,
    InputParams,
)
from pipecat.transcriptions.language import Language
from pipecat.transports.base_transport import BaseTransport
from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport

load_dotenv(override=True)

async def run_bot(transport: BaseTransport, handle_sigint: bool):
    # Reines Gemini Live native-audio Setup (ein Modell, keine separate TTS)
    llm = GeminiMultimodalLiveLLMService(
        api_key=os.getenv("GOOGLE_API_KEY"),
        model="models/gemini-2.5-flash-preview-native-audio-dialog",
        system_instruction=os.getenv("SYSTEM_PROMPT", "Du bist ein hilfreicher deutschsprachiger Voice-Agent."),
        params=InputParams(
            modalities=GeminiMultimodalModalities.AUDIO,
            language=Language.DE_DE,
        ),
        transcribe_user_audio=True,
        transcribe_model_audio=True,
    )

    context = OpenAILLMContext(
        [
            {
                "role": "user",
                "content": "Begrüße den Anrufer kurz und frage, wie du helfen kannst.",
            }
        ]
    )
    context_aggregator = llm.create_context_aggregator(context)

    pipeline = Pipeline(
        [
            transport.input(),
            context_aggregator.user(),
            llm,  # LLM (Audio)
            transport.output(),
            context_aggregator.assistant(),
        ]
    )

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=8000,
            audio_out_sample_rate=8000,
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info("Client connected")
        await task.queue_frames([LLMRunFrame()])

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info("Client disconnected")
        await task.cancel()

    runner = PipelineRunner(handle_sigint=handle_sigint, force_gc=True)
    await runner.run(task)


async def bot(runner_args: RunnerArguments, testing: bool | None = False):
    # Robust: Twilio-Initialframes kommen gelegentlich fragmentiert; kurzer Retry hilft beim allerersten Call
    for _ in range(3):
        try:
            transport_type, call_data = await parse_telephony_websocket(runner_args.websocket)
            break
        except Exception:
            await asyncio.sleep(0.2)
    else:
        # Wenn Parsing wiederholt scheitert, breche sauber ab
        return
    logger.info(f"Detected transport: {transport_type}")

    serializer = TwilioFrameSerializer(
        stream_sid=call_data["stream_id"],
        call_sid=call_data["call_id"],
        account_sid=os.getenv("TWILIO_ACCOUNT_SID", ""),
        auth_token=os.getenv("TWILIO_AUTH_TOKEN", ""),
    )

    transport = FastAPIWebsocketTransport(
        websocket=runner_args.websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            add_wav_header=False,
            vad_analyzer=SileroVADAnalyzer(),
            serializer=serializer,
        ),
    )

    await run_bot(transport, runner_args.handle_sigint)


