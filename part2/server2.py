from starlette.applications import Starlette
from starlette.routing import WebSocketRoute, Route
from starlette.websockets import WebSocket, WebSocketDisconnect
from starlette.responses import HTMLResponse
import uvicorn
import json
import uuid
import asyncio
import traceback

# Global dictionary to store all connected clients
clients = {}

# Global list to store chat history (last 50 messages)
chat_history = []
CHAT_HISTORY_LIMIT = 50

# Debug mode - set to True for detailed logging
DEBUG = True

def debug_log(message):
    """Print debug messages if debug mode is enabled"""
    if DEBUG:
        print(f"[DEBUG] {message}")

class ChatConnection:
    def __init__(self, websocket, client_id, username):
        self.websocket = websocket
        self.client_id = client_id
        self.username = username
        self.connected = True
        
    async def send_message(self, message):
        """Send a message to this client with error handling"""
        if not self.connected:
            return False
            
        try:
            await self.websocket.send_text(message)
            return True
        except Exception as e:
            print(f"[ERROR] Failed to send to {self.client_id} ({self.username}): {e}")
            self.connected = False
            return False

async def broadcast(message, exclude_id=None):
    """Broadcast a message to all connected clients"""
    client_ids = list(clients.keys())
    debug_log(f"Broadcasting to {len(client_ids)} clients" + 
             (f" (excluding {exclude_id})" if exclude_id else ""))
    
    # Collect tasks for concurrent execution
    tasks = []
    
    for client_id in client_ids:
        # Skip excluded client
        if exclude_id and client_id == exclude_id:
            continue
            
        client = clients.get(client_id)
        if client:
            debug_log(f"Queueing message to {client.username} ({client_id})")
            tasks.append(client.send_message(message))
    
    # Execute all send tasks concurrently
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Clean up any clients that failed to receive
        for i, success in enumerate(results):
            if not success:
                # We need to find which client this was for
                for client_id in list(clients.keys()):
                    if not clients[client_id].connected:
                        debug_log(f"Removing disconnected client: {client_id}")
                        clients.pop(client_id, None)

async def chat_websocket(websocket: WebSocket):
    """
    Handle WebSocket chat connections
    """
    # Generate client ID
    client_id = str(uuid.uuid4())
    username = None
    client = None
    
    try:
        # Accept connection
        await websocket.accept()
        print(f"[CONNECT] New connection accepted (ID: {client_id})")
        
        # First message is username
        username = await websocket.receive_text()
        username = username.strip()
        
        # Create client object
        client = ChatConnection(websocket, client_id, username)
        clients[client_id] = client
        
        print(f"[REGISTER] Client {client_id} registered as '{username}'")
        debug_log(f"Current clients: {[c.username for c in clients.values()]}")
        
        # Send chat history to the new client
        for hist_msg in chat_history:
            await client.send_message(hist_msg)

        # Welcome message
        welcome_msg = {
            "id": "system",
            "username": "System",
            "message": f"Welcome, {username}! There are {len(clients)} users online."
        }
        await client.send_message(json.dumps(welcome_msg))
        
        # Announcement to all other users
        join_msg = {
            "id": "system",
            "username": "System",
            "message": f"{username} has joined the chat."
        }
        await broadcast(json.dumps(join_msg), exclude_id=client_id)
        
        # Main message loop
        while client.connected:
            try:
                # Receive with timeout to allow for health checks
                message = await asyncio.wait_for(
                    websocket.receive_text(), 
                    timeout=30.0
                )
                
                print(f"[MESSAGE] From {username}: {message}")
                
                # Prepare message payload
                payload = {
                    "id": client_id,
                    "username": username,
                    "message": message
                }
                json_message = json.dumps(payload)
                
                # Add to chat history (only for user messages)
                chat_history.append(json_message)
                if len(chat_history) > CHAT_HISTORY_LIMIT:
                    chat_history.pop(0)
                # Broadcast to all connected clients
                await broadcast(json_message)
                
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                try:
                    # Check if client is still connected
                    pong = await websocket.receive_text()
                    debug_log(f"Received pong from {username}")
                except:
                    # Client didn't respond, mark as disconnected
                    debug_log(f"No response from {username}, marking as disconnected")
                    client.connected = False
                    
    except WebSocketDisconnect:
        print(f"[DISCONNECT] Client {client_id} ({username or 'unknown'}) disconnected")
    except Exception as e:
        print(f"[ERROR] Error handling client {client_id}: {e}")
        print(traceback.format_exc())
    
    finally:
        # Clean up on disconnect
        if client_id in clients:
            # Get username before removal
            left_username = clients[client_id].username
            
            # Remove from clients dict
            del clients[client_id]
            print(f"[DISCONNECT] Client {client_id} removed from active clients")
            
            # Notify others that user has left
            if left_username:
                leave_msg = {
                    "id": "system",
                    "username": "System",
                    "message": f"{left_username} has left the chat."
                }
                await broadcast(json.dumps(leave_msg))

# Simple home page
async def homepage(request):
    return HTMLResponse("""
    <html>
        <head>
            <title>WebSocket Chat</title>
            <style>
                body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
                h1 { color: #333; }
                p { line-height: 1.6; }
                .instructions { background: #f8f9fa; padding: 15px; border-radius: 5px; }
                code { background: #eee; padding: 2px 5px; border-radius: 3px; }
            </style>
        </head>
        <body>
            <h1>WebSocket Chat Server</h1>
            <div class="instructions">
                <p>The chat server is running! You can connect using a WebSocket client to:</p>
                <p><code>ws://localhost:8000/ws</code></p>
                <p>Use one of the provided Python client scripts to join the chat.</p>
            </div>
        </body>
    </html>
    """)

# Create application with routes
app = Starlette(routes=[
    Route("/", homepage),
    WebSocketRoute("/ws", chat_websocket),
])

if __name__ == "__main__":
    print("[SERVER] Starting stable chat server on http://localhost:8000")
    print("[SERVER] WebSocket endpoint available at ws://localhost:8000/ws")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        log_level="info"
    )
