import asyncio
import websockets

async def echo_client():
    async with websockets.connect("ws://localhost:8000/ws") as websocket:
        while True:
            message = input("Message: ")
            if message.lower() == "exit":
                break
            await websocket.send(message)
            response = await websocket.recv()
            print(f"Received: {response}")

if __name__ == "__main__":
    asyncio.run(echo_client())
