import asyncio
import socket
import threading
import json
import time
import os
import tracemalloc
import re
import hashlib

# Change this!
MY_SERVER_HOST = "example.com"
TCP_PORT = 42440
ADMIN_USERNAME = "ADMIN"

tracemalloc.start()

users_file = 'users.json'
bans_file = 'bans.json'
servers_dir = 'servers'
clients_by_user = {}
clients_by_server = {}

os.makedirs(servers_dir, exist_ok=True)


def log_message(message):
    current_time = time.strftime("%H:%M:%S", time.localtime())
    print(f"[{current_time}] {message}")

def load_users():
    if os.path.exists(users_file):
        with open(users_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_users(users):
    with open(users_file, 'w', encoding='utf-8') as f:
        json.dump(users, f, ensure_ascii=False, indent=4)

def load_servers():
    servers = {}
    for filename in os.listdir(servers_dir):
        if filename.endswith('.json'):
            server_name = filename[:-5]
            with open(os.path.join(servers_dir, filename), 'r', encoding='utf-8') as f:
                servers[server_name] = json.load(f)
    return servers

def save_server(server_name, server_data):
    with open(os.path.join(servers_dir, f'{server_name}.json'), 'w', encoding='utf-8') as f:
        json.dump(server_data, f, ensure_ascii=False, indent=4)

def load_bans():
    if os.path.exists(bans_file):
        with open(bans_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_bans(bans):
    with open(bans_file, 'w', encoding='utf-8') as f:
        json.dump(bans, f, ensure_ascii=False, indent=4)

users = load_users()
bans = load_bans()
servers = load_servers()

default_server = 'general'
if default_server not in servers:
    servers[default_server] = []
    save_server(default_server, servers[default_server])

async def broadcast_message(message, server_name, sender=None):
    if server_name not in servers:
        return

    tasks = []
    server_members = set(servers[server_name])

    for username in server_members:
        ws = clients_by_user.get(username)
        if ws and ws != sender:
            try:
                tasks.append(ws.send(message))
            except Exception as e:
                if not str(e) == "a bytes-like object is required, not 'str'":
                    log_message(f"Error sending message to TCP client: {e}")

    tcp_clients = clients_by_server.get(server_name, [])
    for client in tcp_clients:
        try:
            client.send(message.encode('utf-8'))
        except Exception as e:
            log_message(f"Error sending message to TCP client: {e}")

    if tasks:
        await asyncio.gather(*tasks)


async def send_private_message(sender, recipient, message):
    recipient_ws = clients_by_user.get(recipient)
    
    if isinstance(recipient_ws, socket.socket):
        try:
            recipient_ws.send(f"(Private) {sender}: {message}\n".encode('utf-8'))
            return True
        except Exception as e:
            log_message(f"Error sending private message to TCP client: {e}")
            return False
    
    return False

def get_safe_server_path(server_name):
    safe_name = os.path.basename(server_name)
    return safe_name

commands = {
    "/login": "<username> <password> - Login with username and password.",
    "/register": "<username> <password> - Register a new user.",
    "/create_server": "<server name> - Create a new communication server.",
    "/join_server": "<server name > - Log in to the communication server.",
    "/list_servers": "- Get a list of all available servers for communication.",
    "/members": "- Get list of users on server.",
    "/pm": "<username> <message> - Send a private message to the specified user.",
    "/act": "<action> - Chat action, set your status.",
    "/help": "- Shows this message."
}

def generate_msg_id(sender, recipient, message):
    base = f"{sender}:{recipient}:{message}:{int(time.time())}"
    return hashlib.sha256(base.encode()).hexdigest()

def _send_remote_private_message_sync(sender, recipient, host, message):
    try:
        msg_id = generate_msg_id(sender, recipient, message)
        with socket.create_connection((host, 42439), timeout=5) as s:
            data = {
                'type': 'remote_pm',
                'sender': sender,
                'recipient': recipient,
                'message': message,
                'from_host': MY_SERVER_HOST,
                'msg_id': msg_id
            }
            s.sendall((json.dumps(data) + '\n').encode('utf-8'))
            s.settimeout(5)
            response = b''
            while not response.endswith(b'\n'):
                chunk = s.recv(4096)
                if not chunk:
                    break
                response += chunk
            try:
                resp_json = json.loads(response.decode('utf-8').strip())
                if resp_json.get('status') == 'ok':
                    return True
                else:
                    log_message(f"remote_pm ответ сервера: {resp_json}")
                    return False
            except Exception as e:
                log_message(f"Ошибка разбора ответа remote_pm: {e}")
                return False
    except Exception as e:
        log_message(f"Ошибка отправки remote_pm: {e}")
        return False

async def send_remote_private_message(sender, recipient, host, message):
    return await asyncio.to_thread(_send_remote_private_message_sync, sender, recipient, host, message)

def deliver_remote_pm(sender, recipient, message, server_host=None, from_host=None):
    recipient_ws = clients_by_user.get(recipient)
    log_message(f"[remote_pm] Попытка доставить {recipient}: тип={type(recipient_ws)}, объект={recipient_ws}")

    sender_display = sender
    if from_host:
        sender_display = f"{sender}@{from_host}"
    elif server_host:
        sender_display = f"{sender}@{server_host}"
    pm_text = f"(Private) {sender_display}: {message}"
    if isinstance(recipient_ws, socket.socket):
        try:
            recipient_ws.send(f"{pm_text}\n".encode('utf-8'))
            log_message(f"[remote_pm] Успешно отправлено TCP клиенту {recipient}")
            return True
        except Exception as e:
            log_message(f"Ошибка отправки remote_pm TCP: {e}")
            return False
    else:
        log_message(f"[remote_pm] Не найден подходящий клиент для {recipient} или тип не поддерживается: {type(recipient_ws)}")
        log_message(f"Пользователь {recipient} не найден для remote_pm")
        return False

def notify_tcp_result(client_socket, result, recipient):
    try:
        if result:
            client_socket.send(f"Private message sent to {recipient}.\n".encode('utf-8'))
        else:
            client_socket.send(f"Failed to send private message to {recipient}.\n".encode('utf-8'))
    except Exception:
        pass

def handle_remote_pm_tcp(logged_in_user, remote_user, remote_host, private_message, client_socket, recipient):
    result = _send_remote_private_message_sync(logged_in_user, remote_user, remote_host, private_message)
    notify_tcp_result(client_socket, result, recipient)

def handle_client(client_socket, client_address):
    global last_cmd

    logged_in_user = None
    user_server = None
    last_cmd = ""

    log_message(f"TCP client {client_address} connected")

    try:
        client_socket.settimeout(0.5)
        try:
            peek = client_socket.recv(4096, socket.MSG_PEEK)
            if peek:
                try:
                    msg = json.loads(peek.decode('utf-8').strip())
                    if msg.get('type') == 'remote_pm':
                        data = client_socket.recv(4096)
                        msg = json.loads(data.decode('utf-8').strip())
                        from_host = msg.get('from_host')
                        msg_id = msg.get('msg_id')
                        pending_dialback[msg_id] = (msg.get('sender'), msg.get('recipient'), msg.get('message'), client_socket)
                        dialback_ok = send_dialback_check(from_host, msg_id, msg.get('sender'), msg.get('recipient'), msg.get('message'))
                        if dialback_ok:
                            delivered = deliver_remote_pm(msg.get('sender'), msg.get('recipient'), msg.get('message'), server_host=client_address[0], from_host=from_host)
                            if delivered:
                                client_socket.send((json.dumps({'status': 'ok'}) + '\n').encode('utf-8'))
                            else:
                                client_socket.send((json.dumps({'status': 'error', 'reason': 'User not online'}) + '\n').encode('utf-8'))
                        else:
                            client_socket.send((json.dumps({'status': 'error', 'reason': 'Dialback failed'}) + '\n').encode('utf-8'))
                        pending_dialback.pop(msg_id, None)
                        client_socket.close()
                        log_message(f"[remote_pm] Межсерверное сообщение обработано и соединение закрыто (dialback)")
                        return
                    elif msg.get('type') == 'dialback_check':
                        data = client_socket.recv(4096)
                        msg = json.loads(data.decode('utf-8').strip())
                        resp = {'type': 'dialback_result', 'msg_id': msg.get('msg_id'), 'result': 'ok'}
                        client_socket.send((json.dumps(resp) + '\n').encode('utf-8'))
                        client_socket.close()
                        log_message(f"[dialback] Подтверждение dialback для {msg.get('msg_id')}")
                        return
                except Exception:
                    pass
        except socket.timeout:
            pass

        client_socket.settimeout(None)

        while not logged_in_user:
            if not last_cmd == "/":
                client_socket.send("Enter command (/login /register): ".encode('utf-8'))
            else:
                client_socket.send("*Ping!*".encode('utf-8'))
            command = client_socket.recv(1024).decode('utf-8').strip()

            if not command:
                break

            last_cmd = command

            if not command == "/":
                log_message(f"TCP {client_address} message: {command}")

            if command.startswith("/register"):
                parts = command.split(" ", 2)
                if len(parts) != 3:
                    client_socket.send("Usage: /register <username> <password>\n".encode('utf-8'))
                    continue
                _, username, password = parts
                username = username.replace('@', '_')
                if username in users:
                    client_socket.send("Username already taken. Try another.\n".encode('utf-8'))
                    continue
                users[username] = password
                save_users(users)
                client_socket.send("Registration successful. Please log in.\n".encode('utf-8'))

            elif command.startswith("/login"):
                parts = command.split(" ", 2)
                if len(parts) != 3:
                    client_socket.send("Usage: /login <username> <password>\n".encode('utf-8'))
                    continue
                _, username, password = parts
                if users.get(username) == password:
                    if username in clients_by_user:
                        client_socket.send("This username is already in use.\n".encode('utf-8'))
                    else:
                        logged_in_user = username
                        clients_by_user[username] = client_socket
                        client_socket.send("Login successful.\n".encode('utf-8'))
                        client_socket.send(f"Available servers: {', '.join(servers.keys())}\n".encode('utf-8'))
                        client_socket.send("Select a server using /join_server <server_name>.\n".encode('utf-8'))
                else:
                    client_socket.send("Invalid username or password.\n".encode('utf-8'))

    except (ConnectionResetError, BrokenPipeError):
        log_message(f"TCP client {client_address} disconnected unexpectedly.")
        client_socket.close()
    except Exception as e:
        if not str(e) == "[WinError 10053] Программа на вашем хост-компьютере разорвала установленное подключение":
            log_message(f"TCP client {client_address} error: {e}")

    try:
        while True:
            message = client_socket.recv(1024).decode('utf-8').strip()

            if not message:
                break

            if not message == "/":
                log_message(f"TCP {client_address} message: {message}")

            if message.startswith("/"):
                if message.startswith("/create_server"):
                    parts = message.split(" ", 1)
                    if len(parts) != 2:
                        client_socket.send("Usage: /create_server <server_name>\n".encode('utf-8'))
                        continue
                    _, server_name = parts
                    server_name = get_safe_server_path(server_name)
                    if server_name in servers:
                        client_socket.send("Server already exists.\n".encode('utf-8'))
                    else:
                        servers[server_name] = []
                        save_server(server_name, servers[server_name])
                        client_socket.send(f"Server '{server_name}' created successfully.\n".encode('utf-8'))
                elif message.startswith("/join_server"):
                    parts = message.split(" ", 1)
                    if len(parts) != 2:
                        client_socket.send("Usage: /join_server <server_name>\n".encode('utf-8'))
                        continue
                    _, server_name = parts
                    if server_name not in servers:
                        client_socket.send("Server does not exist.\n".encode('utf-8'))
                    else:
                        if user_server != None:
                            client_socket.send(f"You're already connected to the server '{user_server}'.\n".encode('utf-8'))
                        else:
                            user_server = server_name
                            if logged_in_user and logged_in_user not in servers.get(server_name, []):
                                servers[server_name].append(logged_in_user)
                                save_server(server_name, servers[server_name])
                            if server_name not in clients_by_server:
                                clients_by_server[server_name] = []
                            if client_socket not in clients_by_server[server_name]:
                                clients_by_server[server_name].append(client_socket)
                            asyncio.run(broadcast_message(f"*** {logged_in_user} has joined the server.", user_server))
                            client_socket.send(f"Joined server '{server_name}' successfully.\n".encode('utf-8'))
                elif message.startswith("/delete_server") and logged_in_user == ADMIN_USERNAME:
                    parts = message.split(" ", 1)
                    if len(parts) != 2:
                        client_socket.send("Usage: /delete_server <server_name>\n".encode('utf-8'))
                        continue
                    _, server_name = parts
                    if server_name == default_server:
                        client_socket.send(f"You cannot delete the default server '{default_server}'.\n".encode('utf-8'))
                    elif server_name not in servers:
                        client_socket.send(f"Server '{server_name}' does not exist.\n".encode('utf-8'))
                    else:
                        del servers[server_name]
                        if os.path.exists(os.path.join(servers_dir, f'{server_name}.json')):
                            os.remove(os.path.join(servers_dir, f'{server_name}.json'))
                        
                        asyncio.run(broadcast_message(f"*** Server '{server_name}' has been deleted.", server_name))
                        
                        client_socket.send(f"Server '{server_name}' deleted successfully.\n".encode('utf-8'))

                elif message.startswith("/list_servers"):
                    client_socket.send(f"Servers: {', '.join(servers.keys())}\n".encode('utf-8'))
                elif message.startswith("/members"):
                    if logged_in_user and user_server:
                        members = servers.get(user_server, [])
                        client_socket.send(f"Members in '{user_server}': {', '.join(members)}\n".encode('utf-8'))
                    else:
                        client_socket.send("Please log in and join a server first.\n".encode('utf-8'))
                elif message.startswith("/ban") and logged_in_user == ADMIN_USERNAME:
                    parts = message.split(" ", 1)
                    if len(parts) != 2:
                        client_socket.send("Usage: /ban <username>\n".encode('utf-8'))
                        continue
                    _, banned_user = parts
                    clients_by_user.pop(banned_user)
                    servers[user_server].remove(banned_user)
                    save_server(user_server, servers[user_server])
                    bans[banned_user] = "BANNED"
                    save_bans(bans)
                    asyncio.run(broadcast_message(f"*** {banned_user} has been banned.", user_server))
                elif message.startswith("/act"):
                    parts = message.split(" ", 1)
                    if len(parts) < 2:
                        if not logged_in_user or not username:
                            client_socket.send("Please log in and join a server first.\n")
                            continue

                        client_socket.send("Usage: /act <act>\n".encode('utf-8'))
                        continue

                    _, act_name = parts 
                    asyncio.run(broadcast_message(f"*** {logged_in_user} {act_name}", user_server))
                elif message.startswith("/pm"):
                    parts = message.split(" ", 2)
                    if len(parts) < 3:
                        client_socket.send("Usage: /pm <username> <message>\n".encode('utf-8'))
                        continue
                    _, recipient, private_message = parts
                    m = re.match(r"^([\w\-]+)@([\w\.-]+)$", recipient)
                    if m:
                        remote_user, remote_host = m.group(1), m.group(2)
                        threading.Thread(target=handle_remote_pm_tcp, args=(logged_in_user, remote_user, remote_host, private_message, client_socket, recipient), daemon=True).start()
                        continue
                    if recipient not in users:
                        client_socket.send("User does not exist.\n".encode('utf-8'))
                        continue
                    if recipient == logged_in_user:
                        client_socket.send("You cannot send private messages to yourself.\n".encode('utf-8'))
                        continue
                    if recipient in bans:
                        client_socket.send(f"{recipient} is banned.\n".encode('utf-8'))
                        continue

                    result = asyncio.run(send_private_message(logged_in_user, recipient, private_message))
                    notify_tcp_result(client_socket, result, recipient)
                elif message.startswith("/help"):
                    help_message = "\n".join([f"{cmd} {desc}" for cmd, desc in commands.items()])
                    client_socket.send(f"{help_message}\n".encode('utf-8'))
                elif message == "/":
                    client_socket.send("*Ping!*".encode('utf-8'))
                else:
                    client_socket.send("Unknown command.\n".encode('utf-8'))
                        
            else:
                if not logged_in_user in bans and logged_in_user and user_server:
                    full_message = f"{logged_in_user}: {message}"
                    asyncio.run(broadcast_message(full_message, user_server))
    except Exception as e:
        if not str(e) == "[WinError 10053] Программа на вашем хост-компьютере разорвала установленное подключение":
            log_message(f"TCP client {client_address} error: {e}")
    finally:
        if logged_in_user:
            clients_by_user.pop(logged_in_user, None)
            if user_server:
                if logged_in_user in servers.get(user_server, []):
                    servers[user_server].remove(logged_in_user)
                    save_server(user_server, servers[user_server])
                client_list = clients_by_server.get(user_server, [])
                if client_socket in client_list:
                    client_list.remove(client_socket)
                asyncio.run(broadcast_message(f"*** {logged_in_user} has left the server.", user_server))
        client_socket.close()
        log_message(f"TCP client {client_address} disconnected")

def start_tcp_server(host='127.0.0.1', port=TCP_PORT):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"TCP server started on {host}:{port}")

    while True:
        client_socket, client_address = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address))
        client_thread.start()

if __name__ == "__main__":
    threading.Thread(target=start_tcp_server).start()

pending_dialback = {}

def send_dialback_check(from_host, msg_id, sender, recipient, message):
    try:
        with socket.create_connection((from_host, 42439), timeout=5) as s:
            data = {
                'type': 'dialback_check',
                'msg_id': msg_id,
                'sender': sender,
                'recipient': recipient,
                'message': message
            }
            s.sendall((json.dumps(data) + '\n').encode('utf-8'))
            s.settimeout(5)
            response = b''
            while not response.endswith(b'\n'):
                chunk = s.recv(4096)
                if not chunk:
                    break
                response += chunk
            try:
                resp_json = json.loads(response.decode('utf-8').strip())
                return resp_json.get('result') == 'ok'
            except Exception as e:
                log_message(f"dialback_check: ошибка разбора ответа: {e}")
                return False
    except Exception as e:
        log_message(f"dialback_check: ошибка соединения: {e}")
        return False
