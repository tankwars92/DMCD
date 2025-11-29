import json
import mimetypes
import os
import shutil
import threading
import time
import uuid

from caps.caps import Capability

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UPLOAD_DIR = os.path.join(BASE_DIR, 'uploads')
UPLOAD_TTL_SECONDS = 24 * 60 * 60
MAX_UPLOAD_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB

_cleanup_started = False
_cleanup_lock = threading.Lock()


def _ensure_uploads():
    os.makedirs(UPLOAD_DIR, exist_ok=True)


def _start_cleanup_once():
    global _cleanup_started
    with _cleanup_lock:
        if _cleanup_started:
            return
        _ensure_uploads()

        def _worker():
            while True:
                cutoff = time.time() - UPLOAD_TTL_SECONDS
                try:
                    for entry in os.listdir(UPLOAD_DIR):
                        full = os.path.join(UPLOAD_DIR, entry)
                        try:
                            if os.path.getmtime(full) < cutoff:
                                if os.path.isdir(full):
                                    shutil.rmtree(full, ignore_errors=True)
                                else:
                                    os.remove(full)
                        except Exception:
                            pass
                except Exception:
                    pass
                time.sleep(60)

        threading.Thread(target=_worker, daemon=True).start()
        _cleanup_started = True


def _safe_filename(name):
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
    return ''.join(ch for ch in name if ch in allowed) or 'file'


def _http_response(sock, code, headers, body):
    reason = {200: 'OK', 400: 'Bad Request', 404: 'Not Found', 500: 'Internal Server Error'}.get(code, 'OK')
    head = f"HTTP/1.1 {code} {reason}\r\n".encode('utf-8')
    for k, v in headers.items():
        head += f"{k}: {v}\r\n".encode('utf-8')
    head += b"\r\n"
    try:
        sock.sendall(head)
        if body:
            sock.sendall(body)
    except Exception:
        pass


class HttpFileUploadCapability(Capability):
    def __init__(self):
        super().__init__(
            name="http_file_upload",
            commands={
                "info": "- Show upload endpoint, TTL and max file size"
            }
        )
        _start_cleanup_once()

    def sniff_raw_connection(self, peek_bytes):
        if not peek_bytes:
            return False
        head = peek_bytes[:8].upper()
        return head.startswith(b'GET ') or head.startswith(b'POST ')

    def handle_raw_connection(self, client_socket, client_address):
        try:
            client_socket.settimeout(10)
            req = b''
            while b"\r\n\r\n" not in req and len(req) < 65536:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                req += chunk
            if not req:
                client_socket.close()
                return

            try:
                head, body = req.split(b"\r\n\r\n", 1)
            except ValueError:
                head, body = req, b''
            lines = head.split(b"\r\n")
            if not lines:
                client_socket.close()
                return
            request_line = lines[0].decode('utf-8', errors='ignore')
            parts = request_line.split(' ')
            method = parts[0] if parts else ''
            path = parts[1] if len(parts) > 1 else '/'
            headers = {}
            for ln in lines[1:]:
                if b":" in ln:
                    k, v = ln.split(b":", 1)
                    headers[k.strip().decode('utf-8', errors='ignore').lower()] = v.strip().decode('utf-8', errors='ignore')

            try:
                cl = int(headers.get('content-length', '0') or '0')
            except Exception:
                cl = 0
            missing = max(0, cl - len(body))
            while missing > 0:
                chunk = client_socket.recv(min(65536, missing))
                if not chunk:
                    break
                body += chunk
                missing -= len(chunk)

            if method == 'POST' and path == '/upload':
                self._handle_upload(client_socket, headers, body)
                return
            if method == 'GET' and path.startswith('/file/'):
                self._handle_download(client_socket, path)
                return

            _http_response(client_socket, 404, {'Content-Length': '0', 'Connection': 'close'}, b'')
        except Exception:
            _http_response(client_socket, 500, {'Content-Length': '0', 'Connection': 'close'}, b'')
        finally:
            try:
                client_socket.close()
            except Exception:
                pass

    def _handle_upload(self, sock, headers, body):
        ctype = headers.get('content-type', '')
        if 'multipart/form-data' not in ctype:
            _http_response(sock, 400, {'Content-Length': '0', 'Connection': 'close'}, b'')
            return
        try:
            cl = int(headers.get('content-length', '0') or '0')
        except Exception:
            cl = 0
        if 0 < MAX_UPLOAD_SIZE_BYTES < cl:
            _http_response(sock, 400, {'Content-Length': '0', 'Connection': 'close'}, b'')
            return
        boundary = None
        for part in ctype.split(';'):
            part = part.strip()
            if part.startswith('boundary='):
                boundary = part.split('=', 1)[1]
                if boundary.startswith('"') and boundary.endswith('"'):
                    boundary = boundary[1:-1]
        if not boundary:
            _http_response(sock, 400, {'Content-Length': '0', 'Connection': 'close'}, b'')
            return

        bmark = ('--' + boundary).encode('utf-8')
        sections = body.split(bmark)
        saved = None
        for sec in sections:
            if b'Content-Disposition' in sec and b'filename=' in sec:
                try:
                    header_blob, file_payload = sec.split(b'\r\n\r\n', 1)
                except ValueError:
                    continue
                if file_payload.endswith(b'\r\n'):
                    file_payload = file_payload[:-2]
                if file_payload.endswith(b'--'):
                    file_payload = file_payload[:-2]
                filename = 'file'
                try:
                    disp = next((ln for ln in header_blob.split(b'\r\n') if b'Content-Disposition' in ln), b'')
                    if b'filename=' in disp:
                        fn = disp.split(b'filename=')[1].strip()
                        if fn.startswith(b'"') and fn.endswith(b'"'):
                            fn = fn[1:-1]
                        filename = _safe_filename(fn.decode('utf-8', errors='ignore'))
                except Exception:
                    pass
                if 0 < MAX_UPLOAD_SIZE_BYTES < len(file_payload):
                    _http_response(sock, 400, {'Content-Length': '0', 'Connection': 'close'}, b'')
                    break
                file_id = uuid.uuid4().hex
                target_dir = os.path.join(UPLOAD_DIR, file_id)
                try:
                    os.makedirs(target_dir, exist_ok=True)
                    with open(os.path.join(target_dir, filename), 'wb') as f:
                        f.write(file_payload)
                    try:
                        import DMCD
                        host = DMCD.MY_SERVER_HOST
                        port = DMCD.TCP_PORT
                    except Exception:
                        host = 'localhost'
                        port = 42439
                    payload = {
                        'status': 'ok',
                        'id': file_id,
                        'filename': filename,
                        'size': len(file_payload),
                        'url': f'http://{host}:{port}/file/{file_id}/{filename}'
                    }
                    body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
                    _http_response(sock, 200, {'Content-Type': 'application/json; charset=utf-8', 'Content-Length': str(len(body)), 'Connection': 'close'}, body)
                    saved = True
                except Exception:
                    pass
                break

        if not saved:
            _http_response(sock, 400, {'Content-Length': '0', 'Connection': 'close'}, b'')

    def _handle_download(self, sock, path):
        parts = path.split('/')
        if len(parts) < 4:
            _http_response(sock, 404, {'Content-Length': '0', 'Connection': 'close'}, b'')
            return
        file_id = parts[2]
        filename = _safe_filename(parts[3])
        full_dir = os.path.join(UPLOAD_DIR, file_id)
        full_path = os.path.join(full_dir, filename)
        if not os.path.isfile(full_path):
            _http_response(sock, 404, {'Content-Length': '0', 'Connection': 'close'}, b'')
            return
        mime, _ = mimetypes.guess_type(filename)
        if not mime:
            mime = 'application/octet-stream'
        try:
            size = os.path.getsize(full_path)
            headers = {
                'Content-Type': mime,
                'Content-Length': str(size),
                'Content-Disposition': f'attachment; filename="{filename}"',
                'Connection': 'close'
            }
            _http_response(sock, 200, headers, b'')
            with open(full_path, 'rb') as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    try:
                        sock.sendall(chunk)
                    except Exception:
                        break
        except Exception:
            _http_response(sock, 500, {'Content-Length': '0', 'Connection': 'close'}, b'')

    def handle_info(self, args, session, client_socket, client_key, server_context=None):
        try:
            import DMCD
            host = DMCD.MY_SERVER_HOST
            port = DMCD.TCP_PORT
        except Exception:
            host = 'localhost'
            port = 42442
        ttl = UPLOAD_TTL_SECONDS
        max_sz = MAX_UPLOAD_SIZE_BYTES
        msg = (
            f"HTTP upload endpoint: http://{host}:{port}/upload\n"
            f"Download URL: http://{host}:{port}/file/<id>/<filename>\n"
            f"Retention (TTL): {ttl} seconds.\n"
            f"Max file size: {max_sz} bytes."
        )
        return self.send_message(msg, client_socket, client_key)


