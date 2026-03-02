import json
import aiohttp
import requests
from typing import Optional

from task.clients.base import BaseClient
from task.constants import DIAL_ENDPOINT
from task.models.message import Message
from task.models.role import Role


class DialClient(BaseClient):
    _endpoint: str
    _api_key: str

    def __init__(self, deployment_name: str):
        super().__init__(deployment_name)
        self._endpoint = DIAL_ENDPOINT + f"/openai/deployments/{deployment_name}/chat/completions"

    def get_completion(self, messages: list[Message]) -> Message:
        headers = {
            "api-key": self._api_key,
            "Content-Type": "application/json"
        }
        request_data = {
            "messages": [m.to_dict() for m in messages]
        }
        # print request for visibility
        print("Request:", json.dumps(request_data, indent=2))
        response = requests.post(self._endpoint, headers=headers, json=request_data)
        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}: {response.text}")
        data = response.json()
        print("Response:", json.dumps(data, indent=2))

        # try to extract assistant content with fallbacks
        content = None
        try:
            content = data["choices"][0]["message"]["content"]
        except Exception:
            try:
                content = data["choices"][0].get("text")
            except Exception:
                content = json.dumps(data)
        return Message(Role.AI, content)

    async def stream_completion(self, messages: list[Message]) -> Message:
        headers = {
            "api-key": self._api_key,
            "Content-Type": "application/json"
        }
        request_data = {
            "stream": True,
            "messages": [m.to_dict() for m in messages]
        }
        print("Stream request:", json.dumps(request_data, indent=2))

        contents: list[str] = []
        async with aiohttp.ClientSession() as session:
            async with session.post(self._endpoint, headers=headers, json=request_data) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"HTTP {resp.status}: {text}")

                done = False
                # iterate over raw bytes chunks
                async for raw_chunk in resp.content.iter_any():
                    chunk = raw_chunk.decode("utf-8")
                    for line in chunk.splitlines():
                        if not line.startswith("data:"):
                            continue
                        payload = line[len("data:"):].strip()
                        if payload == "[DONE]":
                            done = True
                            break
                        content_piece = self._get_content_snippet(payload)
                        if content_piece:
                            print(content_piece, end="", flush=True)
                            contents.append(content_piece)
                    if done:
                        break
        print()
        assembled = "".join(contents)
        return Message(Role.AI, assembled)

    def _get_content_snippet(self, payload: str) -> Optional[str]:
        """
        Parse a streaming payload (JSON string) and return the content snippet if present.
        Handles typical DIAL streaming chunk shapes: choices[0].delta.content
        Falls back to choices[0].message.content when applicable.
        """
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            return None

        choices = obj.get("choices")
        if not choices or not isinstance(choices, list):
            return None

        choice = choices[0] if choices else {}
        # Prefer delta.content (streaming chunks)
        delta = choice.get("delta", {}) if isinstance(choice, dict) else {}
        if isinstance(delta, dict):
            content = delta.get("content")
            if content:
                return content

        # Fallback to message.content (regular non-stream responses inside chunks)
        message = choice.get("message", {}) if isinstance(choice, dict) else {}
        if isinstance(message, dict):
            content = message.get("content")
            if content:
                return content

        return None
