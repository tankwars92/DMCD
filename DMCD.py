import asyncio
import socket
import threading
import json
import time
import os
import re
import hashlib

# Change this!
MY_SERVER_HOST = "example.com"
TCP_PORT = 42439
ADMIN_USERNAME = "ADMIN"

users_file = 'users.json'
bans_file = 'bans.json'
servers_dir = 'servers'

clients_by_user = {}
clients_by_server = {}
socket_to_session = {}

session_lock = threading.RLock()

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

members_by_room = {}
remote_subscribers_by_room = {}
user_remote_counters = {}

def _room_members(room: str):
    return members_by_room.setdefault(room, {})

def _remote_subscribers(room: str):
    return remote_subscribers_by_room.setdefault(room, set())

def parse_room_and_host(name: str):
    if '@' in name:
        idx = name.rfind('@')
        return name[:idx], name[idx+1:]
    return name, None

def display_name_for(viewer_host: str, username: str, origin_host: str):
    return username if origin_host == viewer_host else f"{username}@{origin_host}"

def get_advertised_host() -> str:
    return MY_SERVER_HOST

def format_room_event_text(viewer_host: str, event: str, username: str, origin_host: str, payload: dict | None = None):
    disp = display_name_for(viewer_host, username, origin_host)
    if event == 'joined':
        return f"*** {disp} has joined the server."
    if event == 'left':
        return f"*** {disp} has left the server."
    if event == 'message':
        text = payload.get('text') if payload else ''
        return f"{disp}: {text}"
    if event == 'act':
        act = payload.get('act') if payload else ''
        return f"*** {disp} {act}"
    return ''

def _authoritative_room_add_member(room: str, username: str, origin_host: str) -> bool:
    room_map = _room_members(room)
    key = (username, origin_host)
    prev = room_map.get(key, 0)
    room_map[key] = prev + 1

    return prev == 0

def _authoritative_room_remove_member(room: str, username: str, origin_host: str) -> bool:
    room_map = _room_members(room)
    key = (username, origin_host)
    prev = room_map.get(key, 0)
    if prev <= 1:
        room_map.pop(key, None)
        return prev == 1
    else:
        room_map[key] = prev - 1
        return False

def _update_remote_subscriber_count(room: str, origin_host: str):
    room_map = _room_members(room)
    hosts_present = any(h == origin_host for (_, h) in room_map.keys())
    subs = _remote_subscribers(room)
    if hosts_present:
        subs.add(origin_host)
    else:
        subs.discard(origin_host)

dialback_cache = {}
DIALBACK_CACHE_TTL = 300

connection_pool = {}
CONNECTION_POOL_MAX_AGE = 60
pending_dialback = {}

def get_cached_dialback_result(host, msg_type):
    cache_key = f"{host}:{msg_type}"
    if cache_key in dialback_cache:
        timestamp, result = dialback_cache[cache_key]
        if time.time() - timestamp < DIALBACK_CACHE_TTL:
            return result
        else:
            del dialback_cache[cache_key]
    return None

def cache_dialback_result(host, msg_type, result):
    cache_key = f"{host}:{msg_type}"
    dialback_cache[cache_key] = (time.time(), result)

def cleanup_connection_pool():
    current_time = time.time()
    to_remove = []
    for host, (socket_obj, timestamp) in connection_pool.items():
        if current_time - timestamp > CONNECTION_POOL_MAX_AGE:
            try:
                socket_obj.close()
            except:
                pass
            to_remove.append(host)
    for host in to_remove:
        del connection_pool[host]

def get_pooled_connection(host, port):
    cleanup_connection_pool()
    pool_key = f"{host}:{port}"
    
    if pool_key in connection_pool:
        socket_obj, timestamp = connection_pool[pool_key]
        try:
            socket_obj.settimeout(0.1)
            socket_obj.recv(1, socket.MSG_PEEK)
            return socket_obj
        except:
            del connection_pool[pool_key]
    
    try:
        new_socket = socket.create_connection((host, port), timeout=3)
        connection_pool[pool_key] = (new_socket, time.time())
        return new_socket
    except Exception as e:
        return None

def send_with_pooled_connection(host, port, data, timeout=3):
    sock = get_pooled_connection(host, port)
    if not sock:
        return None
    
    try:
        sock.settimeout(timeout)
        sock.sendall((json.dumps(data) + '\n').encode('utf-8'))
        
        response = b''
        while not response.endswith(b'\n'):
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
        
        if response:
            return json.loads(response.decode('utf-8').strip())
        return None
    except Exception as e:
        log_message(f"Error sending through pooled connection: {e}")
        pool_key = f"{host}:{port}"
        if pool_key in connection_pool:
            del connection_pool[pool_key]
        return None

MAX_RETRIES = 3
RETRY_DELAY = 1

def send_with_retry(func, *args, **kwargs):
    try:
        result = func(*args, **kwargs)
        if result is not None:
            return result
    except Exception as e:
        if "timed out" in str(e) or "timeout" in str(e):
            log_message(f"[retry] Timeout: {e}")
        else:
            log_message(f"[retry] Error: {e}")
            return None
    
    def _retry_worker():
        for attempt in range(1, MAX_RETRIES):
            try:
                time.sleep(RETRY_DELAY)
                result = func(*args, **kwargs)
                if result is not None:
                    log_message(f"[retry] Sucessfully after {attempt + 1} attempts")
                    return
            except Exception as e:
                if "timed out" in str(e) or "timeout" in str(e):
                    log_message(f"[retry] Attempt {attempt + 1}/{MAX_RETRIES} timeout: {e}")
                else:
                    log_message(f"[retry] Error: {e}")
                    break
    
    threading.Thread(target=_retry_worker, daemon=True).start()
    return None

def send_dialback_check(from_host, msg_id, msg_type, **kwargs):
    cached_result = get_cached_dialback_result(from_host, msg_type)

    if cached_result is True:
        return True
    
    def _send_dialback():
        try:
            with socket.create_connection((from_host, 42439), timeout=5) as s:
                data = {
                    'type': 'dialback_check',
                    'msg_id': msg_id,
                    'msg_type': msg_type,
                    'from_host': MY_SERVER_HOST,
                    **kwargs
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
                    result = resp_json.get('result') == 'ok'

                    if result:
                        cache_dialback_result(from_host, msg_type, True)
                    return result
                except Exception as e:
                    log_message(f"[dialback_check] Error parsing response: {e}")
                    return False
        except Exception as e:
            log_message(f"[dialback_check] Connection error: {e}")
            raise
    
    return send_with_retry(_send_dialback)

def send_remote_room_message(host: str, room: str, payload: dict) -> dict | None:
    if host is None:
        return None
    data = {
        'msg_id': generate_msg_id(payload.get('sender', ''), room, payload.get('event', payload.get('type',''))),
        'from_host': get_advertised_host(),
        **payload
    }
    return _send_remote_room_sync(host, 42439, data)

def _broadcast_room_event_locally(room: str, event: str, username: str, origin_host: str, payload: dict | None = None, room_key: str | None = None):
    try:
        text = format_room_event_text(MY_SERVER_HOST, event, username, origin_host, payload)
        target_key = room_key or room
        broadcast_message(text, target_key)
    except Exception as e:
        pass

def _send_room_event_to_remotes(room: str, event: str, username: str, origin_host: str, payload: dict | None = None):
    subs = list(_remote_subscribers(room))
    for host in subs:
        if host == MY_SERVER_HOST:
            continue
        data = {
            'type': 'room_event',
            'room': room,
            'event': event,
            'sender': username,
            'sender_origin': origin_host,
            'msg_id': generate_msg_id(username, room, event),
            'from_host': MY_SERVER_HOST,
            'payload': payload or {}
        }
        try:
            send_remote_room_message(host, room, data)
        except Exception:
            pass


class Session:
    def __init__(self, client_socket: socket.socket):
        self.client_socket = client_socket
        self.username = None
        self.server_name = None
        self.protocol = 'tcp'
        self.created_at = time.time()

    def send_text(self, message: str) -> bool:
        try:
            if not message.endswith('\n'):
                payload = (message + '\n').encode('utf-8')
            else:
                payload = message.encode('utf-8')
            self.client_socket.send(payload)
            return True
        except Exception as e:
            log_message(f"Error sending to client ({self.username}): {e}")
            return False
    
def broadcast_message(message, server_name, sender_session: 'Session' = None):
    try:
        with session_lock:
            sessions = list(clients_by_server.get(server_name, set()))
        for sess in sessions:
            if sender_session is not None and sess is sender_session:
                continue
            sess.send_text(message)
    except Exception as e:
        log_message(f"broadcast_message error: {e}")


def send_private_message(sender_username: str, recipient_username: str, message: str) -> bool:
    delivered_any = False
    with session_lock:
        recipient_sessions = list(clients_by_user.get(recipient_username, set()))
    pm_text = f"(Private) {sender_username}: {message}"
    for sess in recipient_sessions:
        ok = sess.send_text(pm_text)
        delivered_any = delivered_any or ok
    return delivered_any

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
                    log_message(f"[remote_pm] Server response: {resp_json}")
                    return False
            except Exception as e:
                log_message(f"[remote_pm] Error parsing response: {e}")
                return False
    except Exception as e:
        log_message(f"[remove_pm] Sending error: {e}")
        return False

async def send_remote_private_message(sender, recipient, host, message):
    return await asyncio.to_thread(_send_remote_private_message_sync, sender, recipient, host, message)

def deliver_remote_pm(sender, recipient, message, server_host=None, from_host=None):
    sender_display = sender
    if from_host:
        sender_display = f"{sender}@{from_host}"
    elif server_host:
        sender_display = f"{sender}@{server_host}"
    pm_text = f"(Private) {sender_display}: {message}"
    with session_lock:
        recipient_sessions = list(clients_by_user.get(recipient, set()))
    if not recipient_sessions:
        log_message(f"[remote_pm] User {recipient} not found")
        return False
    delivered_any = False
    for sess in recipient_sessions:
        ok = sess.send_text(pm_text)
        delivered_any = delivered_any or ok
    if delivered_any:
        log_message(f"[remote_pm] Sucessfully sent to {recipient} ({len(recipient_sessions)} sessions)")
    else:
        log_message(f"[remote_pm] Failed to send remote_pm to {recipient}")
    return delivered_any

def notify_tcp_result(client_socket, result, recipient):
    try:
        if result:
            client_socket.send(f"Private message sent to {recipient}.\n".encode('utf-8'))
        else:
            client_socket.send(f"Failed to send private message to {recipient}.\n".encode('utf-8'))
    except Exception:
        pass

def handle_remote_pm_tcp(sender, recipient, host, private_message, client_socket, recipient_display):
    result = _send_remote_private_message_sync(sender, recipient, host, private_message)
    notify_tcp_result(client_socket, result, recipient_display)

def handle_client(client_socket, client_address):
    global last_cmd

    logged_in_user = None
    user_server = None
    session = Session(client_socket)
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
                        client_addr = client_address[0]
                        pending_dialback[msg_id] = (msg.get('sender'), msg.get('recipient'), msg.get('message'), client_socket)
                        
                        def async_dialback_check():
                            try:
                                dialback_ok = send_dialback_check(
                                    from_host, msg_id, 'remote_pm',
                                    sender=msg.get('sender'),
                                    recipient=msg.get('recipient'),
                                    message=msg.get('message')
                                )
                                
                                if msg_id in pending_dialback:
                                    sender, recipient, message, sock = pending_dialback.pop(msg_id)
                                    
                                    if dialback_ok:
                                        def _deliver_pm():
                                            return deliver_remote_pm(sender, recipient, message, server_host=client_addr, from_host=from_host)
                                        
                                        delivered = send_with_retry(_deliver_pm)
                                        if delivered:
                                            sock.send((json.dumps({'status': 'ok'}) + '\n').encode('utf-8'))
                                        else:
                                            sock.send((json.dumps({'status': 'error', 'reason': 'User not online'}) + '\n').encode('utf-8'))
                                    else:
                                        sock.send((json.dumps({'status': 'error', 'reason': 'Dialback failed'}) + '\n').encode('utf-8'))
                                    
                                    sock.close()
                                    log_message(f"[remote_pm] Message processed")
                            except Exception as e:
                                log_message(f"[remote_pm] Error: {e}")
                                if msg_id in pending_dialback:
                                    _, _, _, sock = pending_dialback.pop(msg_id)
                                    try:
                                        sock.send((json.dumps({'status': 'error', 'reason': 'Internal error'}) + '\n').encode('utf-8'))
                                        sock.close()
                                    except:
                                        pass
                        
                        threading.Thread(target=async_dialback_check, daemon=True).start()
                        return
                    elif msg.get('type') == 'dialback_check':
                        data = client_socket.recv(4096)
                        msg = json.loads(data.decode('utf-8').strip())
                        resp = handle_dialback_check(msg)
                        client_socket.send((json.dumps(resp) + '\n').encode('utf-8'))
                        client_socket.close()
                        return
                    elif msg.get('type') in ('room_join','room_leave','room_message','room_act','room_members_request','room_event'):
                        data = client_socket.recv(4096)
                        msg = json.loads(data.decode('utf-8').strip())
                        msg_type = msg.get('type')
                        from_host = msg.get('from_host')
                        msg_id = msg.get('msg_id')
                        room = msg.get('room')
                        
                        def async_room_dialback_check():
                            try:
                                
                                dialback_ok = False
                                if msg_type in ('room_join', 'room_leave', 'room_message', 'room_act'):
                                    dialback_ok = send_dialback_check(
                                        from_host, msg_id, msg_type,
                                        sender=msg.get('sender', ''),
                                        room=room
                                    )
                                elif msg_type == 'room_event':
                                    dialback_ok = send_dialback_check(
                                        from_host, msg_id, msg_type,
                                        sender=msg.get('sender', ''),
                                        room=room,
                                        event=msg.get('event', '')
                                    )
                                elif msg_type == 'room_members_request':
                                    dialback_ok = send_dialback_check(
                                        from_host, msg_id, msg_type,
                                        room=room
                                    )
                                
                                
                                if not dialback_ok:
                                    client_socket.send((json.dumps({'status':'error','reason':'Dialback failed'}) + '\n').encode('utf-8'))
                                    client_socket.close()
                                    return
                                
                                
                                if msg_type == 'room_event':
                                    event = msg.get('event')
                                    sender = msg.get('sender')
                                    sender_origin = msg.get('sender_origin') or from_host
                                    payload = msg.get('payload') or {}
                                    room_key = f"{room}@{from_host}"
                                    _broadcast_room_event_locally(room, event, sender, sender_origin, payload, room_key=room_key)
                                    client_socket.send((json.dumps({'status':'ok'}) + '\n').encode('utf-8'))
                                    client_socket.close()
                                    return
                                    
                                elif msg_type == 'room_members_request':
                                    room_map = _room_members(room)
                                    members = [{'username': u, 'origin': h} for (u,h) in room_map.keys()]
                                    resp = {'status':'ok','type':'room_members_response','room':room,'members':members}
                                    client_socket.send((json.dumps(resp) + '\n').encode('utf-8'))
                                    client_socket.close()
                                    return
                                    
                                elif msg_type == 'room_join':
                                    sender = msg.get('sender')
                                    origin_host = from_host
                                    first = _authoritative_room_add_member(room, sender, origin_host)
                                    _update_remote_subscriber_count(room, origin_host)
                                    if first:
                                        _broadcast_room_event_locally(room, 'joined', sender, origin_host)
                                        _send_room_event_to_remotes(room, 'joined', sender, origin_host)
                                    client_socket.send((json.dumps({'status':'ok'}) + '\n').encode('utf-8'))
                                    client_socket.close()
                                    return
                                    
                                elif msg_type == 'room_leave':
                                    sender = msg.get('sender')
                                    origin_host = from_host
                                    last = _authoritative_room_remove_member(room, sender, origin_host)
                                    _update_remote_subscriber_count(room, origin_host)
                                    if last:
                                        _broadcast_room_event_locally(room, 'left', sender, origin_host)
                                        _send_room_event_to_remotes(room, 'left', sender, origin_host)
                                    client_socket.send((json.dumps({'status':'ok'}) + '\n').encode('utf-8'))
                                    client_socket.close()
                                    return
                                    
                                elif msg_type == 'room_message':
                                    sender = msg.get('sender')
                                    origin_host = from_host
                                    payload = msg.get('payload') or {}
                                    _broadcast_room_event_locally(room, 'message', sender, origin_host, payload)
                                    
                                    def _send_to_federation():
                                        _send_room_event_to_remotes(room, 'message', sender, origin_host, payload)
                                        return True
                                    
                                    send_with_retry(_send_to_federation)
                                    client_socket.send((json.dumps({'status':'ok'}) + '\n').encode('utf-8'))
                                    client_socket.close()
                                    return
                                    
                                elif msg_type == 'room_act':
                                    sender = msg.get('sender')
                                    origin_host = from_host
                                    payload = msg.get('payload') or {}
                                    _broadcast_room_event_locally(room, 'act', sender, origin_host, payload)
                                    _send_room_event_to_remotes(room, 'act', sender, origin_host, payload)
                                    client_socket.send((json.dumps({'status':'ok'}) + '\n').encode('utf-8'))
                                    client_socket.close()
                                    return
                                    
                            except Exception as e:
                                try:
                                    client_socket.send((json.dumps({'status':'error','reason':'Internal error'}) + '\n').encode('utf-8'))
                                    client_socket.close()
                                except:
                                    pass
                        
                        threading.Thread(target=async_room_dialback_check, daemon=True).start()
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
                    logged_in_user = username
                    session.username = username
                    with session_lock:
                        sessions = clients_by_user.get(username)
                        if sessions is None:
                            clients_by_user[username] = set([session])
                        else:
                            sessions.add(session)
                        socket_to_session[client_socket] = session
                        client_socket.send("Login successful.\n".encode('utf-8'))
                        client_socket.send(f"Available servers: {', '.join(servers.keys())}\n".encode('utf-8'))
                        client_socket.send("Select a server using /join_server <server_name>.\n".encode('utf-8'))
                else:
                    client_socket.send("Invalid username or password.\n".encode('utf-8'))

    except (ConnectionResetError, BrokenPipeError):
        log_message(f"TCP client {client_address} disconnected unexpectedly.")
        client_socket.close()
    except Exception as e:
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
                    _, server_spec = parts
                    room_name, host_part = parse_room_and_host(server_spec)
                    if host_part and host_part != MY_SERVER_HOST:
                        if user_server is not None:
                            client_socket.send(f"You're already connected to the server '{user_server}'.\n".encode('utf-8'))
                            continue
                        user_server = f"{room_name}@{host_part}"
                        session.server_name = user_server
                        should_remote_join = False
                        with session_lock:
                            server_sessions = clients_by_server.get(user_server)
                            if server_sessions is None:
                                clients_by_server[user_server] = set([session])
                            else:
                                server_sessions.add(session)

                            key = (logged_in_user, room_name, host_part)
                            prev = user_remote_counters.get(key, 0)
                            user_remote_counters[key] = prev + 1
                            if prev == 0:
                                should_remote_join = True
                        if should_remote_join:
                            payload = {
                                'type':'room_join',
                                'room': room_name,
                                'sender': logged_in_user
                            }
                            send_remote_room_message(host_part, room_name, payload)
                        client_socket.send(f"Joined server '{user_server}' successfully.\n".encode('utf-8'))
                    else:
                        local_room = room_name
                        if local_room not in servers:
                            client_socket.send("Server does not exist.\n".encode('utf-8'))
                        else:
                            if user_server != None:
                                client_socket.send(f"You're already connected to the server '{user_server}'.\n".encode('utf-8'))
                            else:
                                user_server = local_room
                                session.server_name = local_room
                                should_broadcast_join = False
                                with session_lock:
                                    server_sessions = clients_by_server.get(local_room)
                                    if server_sessions is None:
                                        clients_by_server[local_room] = set([session])
                                        should_broadcast_join = True
                                    else:
                                        had_sessions_for_user = any(s.username == logged_in_user for s in server_sessions)
                                        server_sessions.add(session)
                                        if not had_sessions_for_user:
                                            should_broadcast_join = True

                                    first = _authoritative_room_add_member(local_room, logged_in_user, MY_SERVER_HOST)
                                    if first:
                                        should_broadcast_join = True

                                    members = servers.get(local_room, [])
                                    if logged_in_user not in members:
                                        members.append(logged_in_user)
                                        servers[local_room] = members
                                        save_server(local_room, members)
                                if should_broadcast_join:
                                    broadcast_message(f"*** {logged_in_user} has joined the server.", user_server)
                                    _send_room_event_to_remotes(local_room, 'joined', logged_in_user, MY_SERVER_HOST)
                                client_socket.send(f"Joined server '{local_room}' successfully.\n".encode('utf-8'))
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
                        
                        broadcast_message(f"*** Server '{server_name}' has been deleted.", server_name)
                        
                        client_socket.send(f"Server '{server_name}' deleted successfully.\n".encode('utf-8'))

                elif message.startswith("/list_servers"):
                    client_socket.send(f"Servers: {', '.join(servers.keys())}\n".encode('utf-8'))
                elif message.startswith("/members"):
                    if not (logged_in_user and user_server):
                        client_socket.send("Please log in and join a server first.\n".encode('utf-8'))
                        continue

                    room_name, host_part = parse_room_and_host(user_server)
                    if host_part and host_part != MY_SERVER_HOST:
                        req = {
                            'type': 'room_members_request',
                            'room': room_name
                        }
                        resp = send_remote_room_message(host_part, room_name, req)
                        if not resp or resp.get('status') != 'ok':
                            client_socket.send("Failed to fetch members.\n".encode('utf-8'))
                            continue
                        members_raw = resp.get('members', [])
                        disp = [display_name_for(MY_SERVER_HOST, m['username'], m['origin']) for m in members_raw]
                        client_socket.send(f"Members in '{user_server}': {', '.join(disp)}\n".encode('utf-8'))
                    else:
                        room = room_name
                        room_map = _room_members(room)
                        disp = [display_name_for(MY_SERVER_HOST, u, h) for (u,h) in room_map.keys()]
                        client_socket.send(f"Members in '{user_server}': {', '.join(disp)}\n".encode('utf-8'))
                elif message.startswith("/ban") and logged_in_user == ADMIN_USERNAME:
                    parts = message.split(" ", 1)
                    if len(parts) != 2:
                        client_socket.send("Usage: /ban <username>\n".encode('utf-8'))
                        continue
                    _, banned_user = parts
                    servers_to_notify = set()
                    with session_lock:
                        user_sessions = clients_by_user.pop(banned_user, set())
                        for sess in list(user_sessions):
                            srv = sess.server_name
                            try:
                                if srv:
                                    server_sessions = clients_by_server.get(srv, set())
                                    if sess in server_sessions:
                                        server_sessions.remove(sess)

                                    still_has = any(s.username == banned_user for s in server_sessions)
                                    if not still_has:
                                        members = servers.get(srv, [])
                                        if banned_user in members:
                                            members.remove(banned_user)
                                            servers[srv] = members
                                            save_server(srv, members)
                                        servers_to_notify.add(srv)
                            finally:
                                try:
                                    sess.client_socket.close()
                                except Exception:
                                    pass

                        for sock, s in list(socket_to_session.items()):
                            if s.username == banned_user:
                                socket_to_session.pop(sock, None)
                    bans[banned_user] = "BANNED"
                    save_bans(bans)
                    for srv in servers_to_notify:
                        broadcast_message(f"*** {banned_user} has been banned.", srv)
                elif message.startswith("/act"):
                    parts = message.split(" ", 1)
                    if len(parts) < 2:
                        if not logged_in_user or not username:
                            client_socket.send("Please log in and join a server first.\n")
                            continue

                        client_socket.send("Usage: /act <act>\n".encode('utf-8'))
                        continue

                    _, act_name = parts 

                    room_name, host_part = parse_room_and_host(user_server)
                    if host_part and host_part != MY_SERVER_HOST:
                        payload = {
                            'type': 'room_act',
                            'room': room_name,
                            'sender': logged_in_user,
                            'payload': {'act': act_name}
                        }
                        send_remote_room_message(host_part, room_name, payload)
                    else:
                        broadcast_message(f"*** {logged_in_user} {act_name}", user_server)
                        _send_room_event_to_remotes(room_name, 'act', logged_in_user, MY_SERVER_HOST, {'act': act_name})
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

                    result = send_private_message(logged_in_user, recipient, private_message)
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
                    room_name, host_part = parse_room_and_host(user_server)
                    if host_part and host_part != MY_SERVER_HOST:
                        payload = {
                            'type': 'room_message',
                            'room': room_name,
                            'sender': logged_in_user,
                            'payload': {'text': message}
                        }
                        send_remote_room_message(host_part, room_name, payload)
                    else:
                        full_message = f"{logged_in_user}: {message}"
                        broadcast_message(full_message, user_server)
                        _send_room_event_to_remotes(room_name, 'message', logged_in_user, MY_SERVER_HOST, {'text': message})
    except Exception as e:
        log_message(f"TCP client {client_address} error: {e}")

    finally:
        try:
            left_broadcast_needed = None
            user_to_log = logged_in_user
            with session_lock:
                s = socket_to_session.pop(client_socket, None)
                if s is None:
                    s = session
                if s and s.username:
                    user_sessions = clients_by_user.get(s.username, set())
                    if s in user_sessions:
                        user_sessions.remove(s)
                    if not user_sessions:
                        clients_by_user.pop(s.username, None)
                if s and s.server_name:
                    server_sessions = clients_by_server.get(s.server_name, set())
                    if s in server_sessions:
                        server_sessions.remove(s)

                    still_has = any(sess.username == s.username for sess in server_sessions)
                    if not still_has:
                        if '@' not in s.server_name:

                            members = servers.get(s.server_name, [])
                            if s.username in members:
                                members.remove(s.username)
                                servers[s.server_name] = members
                                save_server(s.server_name, members)

                            left_broadcast_needed = (s.username, s.server_name)
                            last = _authoritative_room_remove_member(s.server_name, s.username, MY_SERVER_HOST)
                            if last:
                                _send_room_event_to_remotes(s.server_name, 'left', s.username, MY_SERVER_HOST)
                        else:
                            try:
                                room_name, host_part = s.server_name.split('@', 1)
                                key = (s.username, room_name, host_part)
                                prev = user_remote_counters.get(key, 0)
                                if prev > 1:
                                    user_remote_counters[key] = prev - 1
                                else:
                                    user_remote_counters.pop(key, None)
                                    payload = {
                                        'type': 'room_leave',
                                        'room': room_name,
                                        'sender': s.username
                                    }
                                    send_remote_room_message(host_part, room_name, payload)
                            except Exception:
                                pass
        finally:
            try:
                client_socket.close()
            except Exception:
                pass
            if left_broadcast_needed:
                u, srv = left_broadcast_needed
                broadcast_message(f"*** {u} has left the server.", srv)
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

def handle_dialback_check(msg_data):
    msg_id = msg_data.get('msg_id')
    msg_type = msg_data.get('msg_type')
    from_host = msg_data.get('from_host')
    
    return {
        'type': 'dialback_result', 
        'msg_id': msg_id, 
        'result': 'ok'
    }

def _send_remote_room_sync(target_host: str, target_port: int, data: dict):
    def _send_room_msg():
        try:
            with socket.create_connection((target_host, target_port), timeout=5) as s:
                s.sendall((json.dumps(data) + '\n').encode('utf-8'))
                s.settimeout(5)
                response = b''
                while not response.endswith(b'\n'):
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    response += chunk
                if response:
                    try:
                        return json.loads(response.decode('utf-8').strip())
                    except Exception:
                        return None
                return None
        except Exception as e:
            raise
    
    return send_with_retry(_send_room_msg)

def cleanup_tasks():
    while True:
        time.sleep(60)
        cleanup_connection_pool()

        current_time = time.time()
        to_remove = []
        for key, (timestamp, result) in dialback_cache.items():
            if current_time - timestamp > DIALBACK_CACHE_TTL:
                to_remove.append(key)
        for key in to_remove:
            del dialback_cache[key]

if __name__ == "__main__":
    threading.Thread(target=cleanup_tasks, daemon=True).start()
    threading.Thread(target=start_tcp_server).start()
