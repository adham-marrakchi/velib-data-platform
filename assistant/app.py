"""Serveur de l'assistant Vélib' : interface de chat + API /api/chat (Server-Sent Events).

La clé OpenRouter reste côté serveur (variable OPENROUTER_API_KEY) : le navigateur ne la voit jamais.
Lancement : uvicorn assistant.app:app --host 0.0.0.0 --port 8090
"""

import json
import logging
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from assistant.core import OPENROUTER_MODELS, Warehouse, openrouter_chat, run_agent, system_prompt

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("assistant")

STATIC_DIR = Path(__file__).parent / "static"
app = FastAPI(title="Assistant Vélib' Data", docs_url=None, redoc_url=None)
warehouse = Warehouse()


class Turn(BaseModel):
    role: str = Field(pattern="^(user|assistant)$")
    content: str = Field(min_length=1, max_length=4000)


class ChatRequest(BaseModel):
    messages: list[Turn] = Field(min_length=1, max_length=40)


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health():
    """État du service : base joignable, dernier relevé, modèle et clé configurés."""
    try:
        snapshot = warehouse.last_snapshot()
        db_ok = True
    except Exception as exc:
        logger.warning("Base injoignable : %s", exc)
        snapshot, db_ok = None, False
    return {
        "database": db_ok,
        "last_snapshot": snapshot.isoformat() if snapshot else None,
        "model": OPENROUTER_MODELS[0],
        "fallback_models": OPENROUTER_MODELS[1:],
        "api_key_configured": bool(os.getenv("OPENROUTER_API_KEY")),
    }


@app.post("/api/chat")
def chat(request: ChatRequest):
    if request.messages[-1].role != "user":
        raise HTTPException(status_code=422, detail="Le dernier message doit venir de l'utilisateur.")
    try:
        system = system_prompt(warehouse.schema_text(), warehouse.last_snapshot())
    except Exception as exc:
        logger.warning("Base injoignable : %s", exc)
        raise HTTPException(status_code=503, detail="Les données Vélib' sont momentanément indisponibles.")

    turns = [t.model_dump() for t in request.messages]

    def events():
        for event in run_agent(turns, openrouter_chat, warehouse, system):
            yield f"data: {json.dumps(event, ensure_ascii=False, default=str)}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})
