import asyncio
import json
import sys
import threading
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# Global flag to control the client
running = True

# Use websockets library (not websocket-client)
import websockets

async def chat_client():
    """Main chat client function"""
    global running

    uri = "ws://localhost:8000/ws"
    print(f"Connecting to {uri}...")

    try:
        async with websockets.connect(uri) as websocket:
            print(f"{Fore.GREEN}Connected successfully!{Style.RESET_ALL}")

            username = await get_username()
            await websocket.send(username)
            print(f"Logged in as: {username}")
            print(f"{Fore.CYAN}Start typing to chat. Type '/exit' to quit.{Style.RESET_ALL}")

            # Store username locally for use in receiver
            local_username = username

            async def sender():
                global running
                from datetime import datetime
                while running:
                    try:
                        message = await asyncio.get_event_loop().run_in_executor(None, input)
                        if message.lower() in ['/exit', '/quit']:
                            running = False
                            await websocket.close()
                            break
                        if message.strip():
                            await websocket.send(message)
                    except (EOFError, KeyboardInterrupt):
                        running = False
                        await websocket.close()
                        break
                    except Exception as e:
                        print(f"Input error: {e}")
                        running = False
                        await websocket.close()
                        break

            async def receiver():
                global running
                from datetime import datetime
                while running:
                    try:
                        message = await websocket.recv()
                        try:
                            data = json.loads(message)
                            timestamp = datetime.now().strftime('%H:%M:%S')
                            if "username" in data and "message" in data:
                                # System message
                                if data.get("id") == "system":
                                    print(f"{Fore.CYAN}[{timestamp}] [SYSTEM] {data['message']}{Style.RESET_ALL}")
                                # Your own message (show as broadcast, just like others)
                                elif data['username'] == local_username:
                                    print(f"{Fore.GREEN}[{timestamp}] {data['username']}: {data['message']}{Style.RESET_ALL}")
                                # Other user's message
                                else:
                                    print(f"{Fore.GREEN}[{timestamp}] {data['username']}: {data['message']}{Style.RESET_ALL}")
                            else:
                                print(f"[{timestamp}] Received: {message}")
                        except Exception as e:
                            print(f"[ERROR] Failed to parse message: {e}\nRaw: {message}")
                    except websockets.exceptions.ConnectionClosed:
                        print(f"{Fore.RED}Connection closed by server{Style.RESET_ALL}")
                        running = False
                        break
                    except Exception as e:
                        print(f"{Fore.RED}[RECEIVER ERROR] {type(e).__name__}: {e}{Style.RESET_ALL}")
                        import traceback
                        traceback.print_exc()
                        running = False
                        break
                print(f"{Fore.RED}[RECEIVER LOOP EXITED]{Style.RESET_ALL}")

            # Run sender and receiver concurrently
            sender_task = asyncio.create_task(sender())
            receiver_task = asyncio.create_task(receiver())
            await asyncio.wait([sender_task, receiver_task], return_when=asyncio.FIRST_COMPLETED)
            running = False

    except Exception as e:
        print(f"{Fore.RED}Connection error: {e}{Style.RESET_ALL}")
    finally:
        running = False

async def get_username():
    """Get username from user"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: input("Enter your username: "))

if __name__ == "__main__":
    try:
        # Display welcome banner
        print(f"{Fore.CYAN}====================================")
        print(f"{Fore.CYAN}         BASIC CHAT CLIENT          ")
        print(f"{Fore.CYAN}====================================")
        
        # Run the chat client
        asyncio.run(chat_client())
    except KeyboardInterrupt:
        print("\nChat client terminated by user")
    finally:
        # Ensure clean exit
        running = False
