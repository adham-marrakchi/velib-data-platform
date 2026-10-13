# Assistant IA Vélib' : interface de chat + API (FastAPI), LLM via OpenRouter.
# Contexte de build = racine du dépôt (le paquet "assistant" est copié tel quel).
FROM python:3.11-slim

WORKDIR /app
COPY assistant/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && useradd --create-home --uid 1001 assistant

COPY assistant /app/assistant
USER assistant

EXPOSE 8090
CMD ["uvicorn", "assistant.app:app", "--host", "0.0.0.0", "--port", "8090"]
