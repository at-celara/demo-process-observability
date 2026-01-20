from __future__ import annotations

import os
import time
from typing import Optional

from rich.console import Console

console = Console()


class LLMClientError(Exception):
    pass


class OpenAIClient:
    def __init__(
        self,
        api_key_env: str,
        model: str,
        temperature: float = 0.0,
        max_output_tokens: int = 500,
        timeout_s: int = 60,
        max_retries: int = 3,
        retry_backoff_s: float = 2.0,
    ) -> None:
        api_key = os.getenv(api_key_env)
        if not api_key:
            raise LLMClientError(
                f"Missing API key. Set environment variable {api_key_env} (e.g., in .env)."
            )
        self.api_key = api_key
        self.model = model
        self.temperature = temperature
        self.max_output_tokens = max_output_tokens
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.retry_backoff_s = retry_backoff_s

        try:
            from openai import OpenAI
        except Exception as e:  # pragma: no cover
            raise LLMClientError("OpenAI SDK not installed. Add 'openai' to dependencies.") from e
        # Initialize client
        self._client = OpenAI(api_key=self.api_key)

    def chat(self, prompt_text: str, system_text: Optional[str] = None) -> str:
        """
        Send a single-turn request via Responses API and return raw text.
        """
        input_text = f"{system_text}\n\n{prompt_text}" if system_text else prompt_text

        attempt = 0
        last_exc: Optional[Exception] = None
        while attempt <= self.max_retries:
            try:
                # Use the Responses API
                resp = self._client.responses.create(
                    model=self.model,
                    input=input_text,
                    temperature=self.temperature,
                    max_output_tokens=self.max_output_tokens,
                    timeout=self.timeout_s,
                )
                # Prefer convenience property when available
                try:
                    text = (resp.output_text or "").strip()
                except Exception:
                    # Fallback: extract concatenated text segments
                    text_parts = []
                    try:
                        for part in getattr(resp, "output", []) or []:
                            for content in getattr(part, "content", []) or []:
                                if getattr(content, "type", "") in ("output_text", "text"):
                                    txt = getattr(content, "text", None)
                                    if isinstance(txt, str):
                                        text_parts.append(txt)
                    except Exception:
                        pass
                    text = "".join(text_parts).strip()
                return text
            except Exception as e:
                last_exc = e
                attempt += 1
                if attempt > self.max_retries:
                    break
                sleep_s = self.retry_backoff_s * (2 ** (attempt - 1))
                time.sleep(sleep_s)
        raise LLMClientError(f"OpenAI chat request failed after retries: {last_exc}")
