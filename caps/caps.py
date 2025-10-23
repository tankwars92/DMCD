class Capability:
    def __init__(self, name, commands):
        self.name = name
        self.commands = commands
    
    def get_help(self):
        help_text = ""
        for cmd, desc in self.commands.items():
            help_text += f"/{self.name}-{cmd} {desc}\n"
        return help_text
    
    def handle_command(self, command, args, session, client_socket, client_key, server_context=None):
        if command not in self.commands:
            return False
        
        handler_name = f"handle_{command}"
        if hasattr(self, handler_name):
            handler = getattr(self, handler_name)
            return handler(args, session, client_socket, client_key, server_context)
        
        return False
    
    def send_message(self, message, client_socket, client_key, message_type='system'):
        from DMCD import send_to_client
        return send_to_client(client_socket, message, client_key, message_type)

class CapabilitiesManager:
    def __init__(self):
        self.capabilities = {}
        self._load_capabilities()
    
    def _load_capabilities(self):
        import os
        import importlib
        
        caps_dir = os.path.dirname(os.path.abspath(__file__))
        
        for filename in os.listdir(caps_dir):
            if filename.endswith('.py') and filename != '__init__.py' and filename != 'caps.py':
                module_name = filename[:-3]
                try:
                    module = importlib.import_module(f'caps.{module_name}')
                    
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        if (isinstance(attr, type) and 
                            issubclass(attr, Capability) and 
                            attr != Capability):
                            capability = attr()
                            self._register_capability(capability)
                except Exception as e:
                    print(f"Error loading capability {module_name}: {e}")
    
    def _register_capability(self, capability):
        self.capabilities[capability.name] = capability
    
    def get_capability(self, name):
        return self.capabilities.get(name)
    
    def get_all_capabilities(self):
        return self.capabilities.copy()
    
    def get_capabilities_list(self):
        return list(self.capabilities.keys())
    
    def handle_capability_command(self, command, args, session, client_socket, client_key, server_context=None):
        if '-' in command:
            cap_name, cmd_name = command.split('-', 1)
            capability = self.get_capability(cap_name)
            if capability:
                return capability.handle_command(cmd_name, args, session, client_socket, client_key, server_context)
        else:
            capability = self.get_capability(command)
            if capability:
                from DMCD import send_to_client
                send_to_client(client_socket, capability.get_help(), client_key)
                return True
        
        return False
    
