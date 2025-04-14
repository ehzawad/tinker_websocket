from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect
import uvicorn

async def echo(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Echo: {data}")
    except WebSocketDisconnect:
        print("Client disconnected")

app = Starlette(routes=[
    WebSocketRoute("/ws", echo),
])

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
