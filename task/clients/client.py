from aidial_client import Dial, AsyncDial  # optional, kept for compatibility

from task.clients.base import BaseClient
from task.models.message import Message
from task.clients.custom_client import DialClient as CustomDialClient


class DialClient(BaseClient):

    def __init__(self, deployment_name: str):
        super().__init__(deployment_name)
        # Delegate to the custom HTTP implementation for now
        self._sync_client = CustomDialClient(deployment_name)
        self._async_client = CustomDialClient(deployment_name)

    def get_completion(self, messages: list[Message]) -> Message:
        return self._sync_client.get_completion(messages)

    async def stream_completion(self, messages: list[Message]) -> Message:
        return await self._async_client.stream_completion(messages)
