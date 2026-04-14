"""
Minimal async LLM client for OpenAI-compatible APIs (DeepSeek, Claude).
"""
import json
import logging
from typing import Optional

import httpx

from src.config.settings import settings

logger = logging.getLogger(__name__)


class LLMClient:
    """Async client for OpenAI-compatible chat completion API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
    ):
        self.api_key = api_key or settings.llm_api_key
        self.base_url = (base_url or settings.llm_base_url).rstrip("/")
        self.model = model or settings.llm_model
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=60.0,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    async def complete(
        self,
        prompt: str,
        system: str = "",
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
    ) -> str:
        """Send chat completion request, return assistant message text."""
        client = await self._get_client()
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens or settings.llm_max_tokens,
            "temperature": temperature if temperature is not None else settings.llm_temperature,
        }

        response = await client.post(
            f"{self.base_url}/chat/completions",
            json=payload,
        )
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]

    async def complete_json(self, prompt: str, system: str = "") -> dict | list:
        """Complete and parse JSON from response. Strips markdown fences."""
        text = await self.complete(prompt, system)
        text = text.strip()
        # Strip ```json ... ``` fences
        if text.startswith("```"):
            first_newline = text.find("\n")
            if first_newline != -1:
                text = text[first_newline + 1:]
            else:
                text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        return json.loads(text.strip())

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
