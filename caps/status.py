from caps.caps import Capability

class StatusCapability(Capability):
    def __init__(self):
        super().__init__(
            name="status",
            commands={
                "check": "<username> - Check if a user is online or offline"
            }
        )
    
    def handle_check(self, args, session, client_socket, client_key, server_context=None):
        if len(args) < 1:
            self.send_message("Usage: /status-check <username>", client_socket, client_key)
            return True
        
        username = args[0]
        
        if '@' in username:
            user_part, host_part = username.rsplit('@', 1)
            import DMCD
            if host_part == DMCD.MY_SERVER_HOST:
                username = user_part
        
        if server_context:
            clients_by_user = server_context.get('clients_by_user', {})
            session_lock = server_context.get('session_lock')
            
            with session_lock:
                user_sessions = list(clients_by_user.get(username, set()))
        else:
            import DMCD
            with DMCD.session_lock:
                user_sessions = list(server3.clients_by_user.get(username, set()))
        
        if user_sessions:
            self.send_message(f"{username} is online.", client_socket, client_key)
        else:
            self.send_message(f"{username} is offline.", client_socket, client_key)
        
        return True
