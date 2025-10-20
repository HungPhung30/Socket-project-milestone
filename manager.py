#!/usr/bin/env python3

import socket
import threading
import sys
import time
from utils import Message, SUCCESS, FAILURE

class DSS_Manager:
    def __init__(self, port):
        self.port = port
        self.users = {}  # user_name -> {address, m_port, c_port}
        self.disks = {}  # disk_name -> {address, m_port, c_port, state}
        self.dss_configs = {}  # dss_name -> {n, striping_unit, disk_order}
        self.files = {}  # dss_name -> {file_name -> {size, owner}}
        self.read_operations = {}  # Track ongoing read operations
        self.lock = threading.Lock()
        
    def start_server(self):
        """Start the manager server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind(('0.0.0.0', self.port))
            self.server_socket.listen(10)
            print(f"Manager listening on port {self.port}")
            
            while True:
                client_socket, address = self.server_socket.accept()
                print(f"Connection from {address}")
                
                # Handle each client in a separate thread
                client_thread = threading.Thread(
                    target=self.handle_client, 
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()
                
        except KeyboardInterrupt:
            print("\nShutting down manager...")
        finally:
            self.server_socket.close()
    
    def handle_client(self, client_socket, address):
        """Handle messages from a client (user or disk)"""
        try:
            while True:
                message = Message.receive_message(client_socket)
                if not message:
                    break
                
                command, args = Message.decode_message(message.encode())
                print(f"Received: {command} {args}")
                
                response = self.process_command(command, args, address)
                Message.send_message(client_socket, response)
                
        except Exception as e:
            print(f"Error handling client {address}: {e}")
        finally:
            client_socket.close()
    
    def process_command(self, command, args, address):
        """Process incoming commands"""
        with self.lock:
            if command == "register-user":
                return self.register_user(args, address)
            elif command == "register-disk":
                return self.register_disk(args, address)
            elif command == "configure-dss":
                return self.configure_dss(args)
            elif command == "ls":
                return self.list_files(args)
            elif command == "copy":
                return self.copy_file(args)
            elif command == "read":
                return self.read_file(args)
            elif command == "disk-failure":
                return self.disk_failure(args)
            elif command == "decommission-dss":
                return self.decommission_dss(args)
            elif command == "decommission-complete":
                return self.decommission_complete(args)
            elif command == "deregister-user":
                return self.deregister_user(args)
            elif command == "deregister-disk":
                return self.deregister_disk(args)
            elif command == "copy-complete":
                return self.copy_complete(args)
            elif command == "read-complete":
                return self.read_complete(args)
            elif command == "recovery-complete":
                return self.recovery_complete(args)
            else:
                return FAILURE
    
    def register_user(self, args, address):
        """Register a user process"""
        if len(args) != 4:
            return FAILURE
        
        user_name, ipv4_addr, m_port, c_port = args
        
        # Check if user already registered
        if user_name in self.users:
            return FAILURE
        
        # Validate parameters
        if len(user_name) > 15:
            return FAILURE
        
        # Store user info
        self.users[user_name] = {
            'address': ipv4_addr,
            'm_port': int(m_port),
            'c_port': int(c_port)
        }
        
        print(f"Registered user: {user_name}")
        return SUCCESS
    
    def register_disk(self, args, address):
        """Register a disk process"""
        if len(args) != 4:
            return FAILURE
        
        disk_name, ipv4_addr, m_port, c_port = args
        
        # Check if disk already registered
        if disk_name in self.disks:
            return FAILURE
        
        # Store disk info
        self.disks[disk_name] = {
            'address': ipv4_addr,
            'm_port': int(m_port),
            'c_port': int(c_port),
            'state': 'Free'
        }
        
        print(f"Registered disk: {disk_name}")
        return SUCCESS
    
    def configure_dss(self, args):
        """Configure a new DSS"""
        if len(args) != 3:
            return FAILURE
        
        dss_name, n, striping_unit = args
        n = int(n)
        striping_unit = int(striping_unit)
        
        # Validate parameters
        if n < 3:
            return FAILURE
        if striping_unit < 128 or striping_unit > 1048576:
            return FAILURE
        if not (striping_unit & (striping_unit - 1) == 0):  # Check if power of 2
            return FAILURE
        if dss_name in self.dss_configs:
            return FAILURE
        
        # Check if enough free disks
        free_disks = [name for name, info in self.disks.items() 
                     if info['state'] == 'Free']
        if len(free_disks) < n:
            return FAILURE
        
        # Select n random disks and update their state
        selected_disks = free_disks[:n]
        disk_order = []
        
        for disk_name in selected_disks:
            self.disks[disk_name]['state'] = 'InDSS'
            disk_order.append(disk_name)
        
        # Store DSS configuration
        self.dss_configs[dss_name] = {
            'n': n,
            'striping_unit': striping_unit,
            'disk_order': disk_order
        }
        
        self.files[dss_name] = {}
        
        print(f"Configured DSS: {dss_name} with disks {disk_order}")
        return SUCCESS
    
    def list_files(self, args):
        """List files in all DSSs"""
        if len(args) != 0:
            return FAILURE
        
        if not self.dss_configs:
            return FAILURE
        
        response_lines = []
        for dss_name, config in self.dss_configs.items():
            # DSS info line
            disk_names = ", ".join(config['disk_order'])
            dss_line = f"{dss_name}: Disk array with n={config['n']} ({disk_names}) with striping-unit {config['striping_unit']} B."
            response_lines.append(dss_line)
            
            # File info lines
            if dss_name in self.files:
                for file_name, file_info in self.files[dss_name].items():
                    file_line = f"  {file_name} {file_info['size']} B {file_info['owner']}"
                    response_lines.append(file_line)
        
        return SUCCESS + "\\n" + "\\n".join(response_lines)
    
    def configure_dss(self, args):
        """Configure a new DSS - Updated for milestone demo"""
        if len(args) != 3:
            return FAILURE
        
        dss_name, n, striping_unit = args
        n = int(n)
        striping_unit = int(striping_unit)
        
        # Validate parameters
        if n < 3:
            print(f"DSS configuration failed: n={n} < 3")
            return FAILURE
        if striping_unit < 128 or striping_unit > 1048576:
            print(f"DSS configuration failed: invalid striping unit {striping_unit}")
            return FAILURE
        if not (striping_unit & (striping_unit - 1) == 0):  # Check if power of 2
            print(f"DSS configuration failed: striping unit {striping_unit} not power of 2")
            return FAILURE
        if dss_name in self.dss_configs:
            print(f"DSS configuration failed: {dss_name} already exists")
            return FAILURE
        
        # Check if enough free disks
        free_disks = [name for name, info in self.disks.items() 
                    if info['state'] == 'Free']
        if len(free_disks) < n:
            print(f"DSS configuration failed: need {n} disks, only {len(free_disks)} free")
            print(f"Free disks: {free_disks}")
            return FAILURE
        
        # Select n disks and update their state
        selected_disks = free_disks[:n]
        disk_order = []
        
        for disk_name in selected_disks:
            self.disks[disk_name]['state'] = 'InDSS'
            disk_order.append(disk_name)
        
        # Store DSS configuration
        self.dss_configs[dss_name] = {
            'n': n,
            'striping_unit': striping_unit,
            'disk_order': disk_order
        }
        
        self.files[dss_name] = {}
        
        print(f"Successfully configured {dss_name} with {n} disks: {disk_order}")
        return SUCCESS
    
    def copy_file(self, args):
        """Initiate file copy to DSS"""
        if len(args) != 3:
            return FAILURE
        
        file_name, file_size, owner = args
        file_size = int(file_size)
        
        # Check if any DSS exists
        if not self.dss_configs:
            print("No DSS configured. Copy failed.")
            return FAILURE
        
        # Select a DSS (use first available)
        dss_name = list(self.dss_configs.keys())[0]
        config = self.dss_configs[dss_name]
        
        # DON'T store file metadata yet - wait for copy-complete
        
        # Return DSS parameters
        response_parts = [SUCCESS, dss_name, str(file_size), str(config['n']), 
                        str(config['striping_unit']), str(len(config['disk_order']))]
        
        # Add disk information
        for disk_name in config['disk_order']:
            disk_info = self.disks[disk_name]
            response_parts.extend([
                disk_name, disk_info['address'], str(disk_info['c_port'])
            ])
        
        print(f"Copy operation initiated for {file_name} to {dss_name}")
        return " ".join(response_parts)
        
        
    def copy_complete(self, args):
        """Handle copy completion - NOW add file to directory"""
        if len(args) != 4:  # dss_name, file_name, owner, file_size
            print(f"Copy-complete requires 4 args, got {len(args)}: {args}")
            return FAILURE
        
        dss_name, file_name, owner, file_size = args
        
        if dss_name not in self.dss_configs:
            print(f"DSS {dss_name} not found")
            return FAILURE
        
        # Add file to DSS directory NOW
        if dss_name not in self.files:
            self.files[dss_name] = {}
        
        self.files[dss_name][file_name] = {
            'size': int(file_size),
            'owner': owner
        }
        
        print(f"Copy completed for {file_name} ({file_size} bytes) on {dss_name} by {owner}")
        return SUCCESS
    
    def read_file(self, args):
        """Initiate file read from DSS"""
        if len(args) != 3:
            print(f"Read command requires 3 args, got {len(args)}: {args}")
            return FAILURE
        
        dss_name, file_name, user_name = args
        
        # Check if DSS exists
        if dss_name not in self.dss_configs:
            print(f"DSS {dss_name} not found")
            return FAILURE
        
        # Check if file exists in DSS
        if dss_name not in self.files or file_name not in self.files[dss_name]:
            print(f"File {file_name} not found in DSS {dss_name}")
            print(f"Available files in {dss_name}: {list(self.files.get(dss_name, {}).keys())}")
            return FAILURE
        
        file_info = self.files[dss_name][file_name]
        
        # Check ownership
        if file_info['owner'] != user_name:
            print(f"Access denied: {user_name} does not own {file_name} (owned by {file_info['owner']})")
            return FAILURE
        
        config = self.dss_configs[dss_name]
        
        # Return DSS and file parameters
        response_parts = [SUCCESS, str(file_info['size']), str(config['n']), 
                        str(config['striping_unit']), str(len(config['disk_order']))]
        
        # Add disk information
        for disk_name in config['disk_order']:
            disk_info = self.disks[disk_name]
            response_parts.extend([
                disk_name, disk_info['address'], str(disk_info['c_port'])
            ])
        
        print(f"Read operation initiated for {file_name} from {dss_name} by {user_name}")
        return " ".join(response_parts)
    
    def read_complete(self, args):
        """Handle read completion"""
        return SUCCESS
    
    def disk_failure(self, args):
        """Handle disk failure simulation - Phase 1: Return DSS info"""
        if len(args) != 1:
            return FAILURE
        
        dss_name = args[0]
        
        if dss_name not in self.dss_configs:
            print(f"DSS {dss_name} not found")
            return FAILURE
        
        # if dss_name in self.read_operations and self.read_operations[dss_name]:
        #     print(f"DSS {dss_name} has ongoing read operations")
        #     return FAILURE
        
        config = self.dss_configs[dss_name]
        
        # Return DSS parameters for failure and recovery
        response_parts = [SUCCESS, str(config['n']), str(config['striping_unit']), 
                        str(len(config['disk_order']))]
        
        # Add disk information
        for disk_name in config['disk_order']:
            disk_info = self.disks[disk_name]
            response_parts.extend([
                disk_name, disk_info['address'], str(disk_info['c_port'])
            ])
        
        print(f"Disk failure initiated for DSS {dss_name}")
        return " ".join(response_parts)

    def recovery_complete(self, args):
        """Handle recovery completion - Phase 2"""
        if len(args) < 2:
            return FAILURE
        
        dss_name = args[0]
        failed_disk_name = args[1]
        
        print(f"Recovery completed for disk {failed_disk_name} in DSS {dss_name}")
        return SUCCESS
    
    def decommission_dss(self, args):
        """Decommission a DSS - Phase 1: Return DSS info"""
        if len(args) != 1:
            return FAILURE
        
        dss_name = args[0]
        
        if dss_name not in self.dss_configs:
            print(f"DSS {dss_name} not found")
            return FAILURE
        
        config = self.dss_configs[dss_name]
        
        # Return DSS parameters so user can instruct disks to delete content
        response_parts = [SUCCESS, str(config['n']), str(config['striping_unit']), 
                        str(len(config['disk_order']))]
        
        # Add disk information
        for disk_name in config['disk_order']:
            disk_info = self.disks[disk_name]
            response_parts.extend([
                disk_name, disk_info['address'], str(disk_info['c_port'])
            ])
        
        print(f"Decommission initiated for DSS {dss_name}")
        return " ".join(response_parts)

    def decommission_complete(self, args):
        """Decommission a DSS - Phase 2: Clean up after user confirms deletion"""
        if len(args) != 1:
            return FAILURE
        
        dss_name = args[0]
        
        if dss_name not in self.dss_configs:
            return FAILURE
        
        config = self.dss_configs[dss_name]
        
        # Set all disks in this DSS back to Free
        for disk_name in config['disk_order']:
            if disk_name in self.disks:
                self.disks[disk_name]['state'] = 'Free'
                print(f"Disk {disk_name} set to Free")
        
        # Remove DSS configuration
        del self.dss_configs[dss_name]
        if dss_name in self.files:
            del self.files[dss_name]
        
        print(f"✓ DSS {dss_name} decommissioned")
        return SUCCESS
    
    def deregister_user(self, args):
        """Deregister a user"""
        if len(args) != 1:
            return FAILURE
        
        user_name = args[0]
        
        if user_name not in self.users:
            return FAILURE
        
        del self.users[user_name]
        return SUCCESS
    
    def deregister_disk(self, args):
        """Deregister a disk"""
        if len(args) != 1:
            return FAILURE
        
        disk_name = args[0]
        
        if disk_name not in self.disks:
            return FAILURE
        
        del self.disks[disk_name]
        return SUCCESS
    
    

def main():
    if len(sys.argv) != 2:
        print("Usage: python manager.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    manager = DSS_Manager(port)
    manager.start_server()

if __name__ == "__main__":
    main()