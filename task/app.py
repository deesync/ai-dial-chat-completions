import asyncio

from task.clients.client import DialClient
from task.clients.custom_client import DialClient as CustomDialClient
from task.constants import DEFAULT_SYSTEM_PROMPT, DEPLOYMENT_NAME
from task.models.conversation import Conversation
from task.models.message import Message
from task.models.role import Role

async def start(stream: bool) -> None:
	# try env var first, otherwise prompt
	env_deployment = DEPLOYMENT_NAME.strip()
	if env_deployment:
		deployment_name = env_deployment
		print(f"Using deployment from DIAL_DEPLOYMENT_NAME env var: {deployment_name}")
	else:
		deployment_name = input("Deployment name (required): ").strip()
		if not deployment_name:
			print("Deployment name is required.")
			return

	use_custom = input("Use custom client? (y/N): ").strip().lower().startswith("y")

	# create clients
	client = DialClient(deployment_name)
	custom_client = CustomDialClient(deployment_name)

	# choose active client
	active_client = custom_client if use_custom else client

	# create conversation and add system prompt
	conv = Conversation()
	sys_prompt = input("System prompt (enter to use default): ").strip() or DEFAULT_SYSTEM_PROMPT
	conv.add_message(Message(Role.SYSTEM, sys_prompt))

	print("Enter messages (type 'exit' to quit).")
	while True:
		user_text = input("You: ").strip()
		if user_text.lower() == "exit":
			print("Exiting.")
			break
		# add user message
		user_msg = Message(Role.USER, user_text)
		conv.add_message(user_msg)

		# call appropriate method
		try:
			if stream:
				assistant_msg = await active_client.stream_completion(conv.get_messages())
			else:
				assistant_msg = active_client.get_completion(conv.get_messages())
		except Exception as e:
			print("Error:", e)
			continue

		# print and store assistant message
		print(f"\nAssistant: {assistant_msg.content}")
		conv.add_message(assistant_msg)


asyncio.run(
	start(True)
)
