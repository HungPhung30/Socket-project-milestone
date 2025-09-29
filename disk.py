#!/usr/bin/env python3

import socket
import threading
import sys
import os
import json
from utils import Message, SUCCESS, FAILURE, calculate_parity, pad_block

class DSS_Disk:
    def __init__(self, disk_name, manager_ip, manager_port, m_port, c_port):
        self.disk_name = disk_name
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.m_port = m_port  # Management port
        self.c_port = c_port  # Command port
        self.storage = {}  # file_name -> {stripes: {stripe_num: data}}
        self.dss_info = {}  # Store DSS configuration info
        self.failed = False
        self.lock = threading.Lock()
        
        # Create storage directory
        self.storage_dir = f"disk_{disk_name}_storage"
        os.makedirs(self.storage_dir, exist_ok=True)
    
    def register_with_manager(self):
        """Register this disk with the manager"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.manager_ip, self.manager_port))
            
            # Send registration message
            message = Message.encode_message("register-disk", self.disk_name, 
                                           "127.0.0.1", self.m_port, self.c_port)
            Message.send_message(sock, message)
            
            # Wait for response
            response = Message.receive_message(sock)
            sock.close()
            
            if response == SUCCESS:
                print(f"Disk {self.disk_name} registered successfully")
                return True
            else:
                print(f"Failed to register disk {self.disk_name}")
                return False
                
        except Exception as e:
            print(f"Error registering with manager: {e}")
            return False
    
    def start_command_server(self):
        """Start the command server for peer-to-peer communication"""
        self.command_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.command_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.command_socket.bind(('localhost', self.c_port))
            self.command_socket.listen(10)
            print(f"Disk {self.disk_name} command server listening on port {self.c_port}")
            
            while True:
                client_socket, address = self.command_socket.accept()
                
                # Handle each client in a separate thread
                client_thread = threading.Thread(
                    target=self.handle_command_client, 
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()
                
        except Exception as e:
            print(f"Error in command server: {e}")
        finally:
            self.command_socket.close()
    
    def handle_command_client(self, client_socket, address):
        """Handle P2P commands from users or other disks"""
        try:
            while True:
                message = Message.receive_message(client_socket)
                if not message:
                    break
                
                command, args = Message.decode_message(message.encode())
                print(f"Disk {self.disk_name} received: {command} {args}")
                
                response = self.process_command(command, args)
                Message.send_message(client_socket, response)
                
        except Exception as e:
            print(f"Error handling command client {address}: {e}")
        finally:
            client_socket.close()
    
    def process_command(self, command, args):
        """Process P2P commands"""
        if self.failed:
            return "FAIL"
        
        with self.lock:
            if command == "store-block":
                return self.store_block(args)
            elif command == "read-block":
                return self.read_block(args)
            elif command == "fail":
                return self.simulate_failure()
            elif command == "get-stripe":
                return self.get_stripe(args)
            else:
                return FAILURE
    
    def store_block(self, args):
        """Store a data or parity block"""
        if len(args) < 5:
            return FAILURE
        
        dss_name, file_name, stripe_num, block_type, data_size = args[:5]
        stripe_num = int(stripe_num)
        data_size = int(data_size)
        
        if block_type == "data":
            block_data = b'D' * data_size  # Dummy data block
        else:  # parity
            block_data = b'P' * data_size  # Dummy parity block
        
        # Store the block
        file_path = os.path.join(self.storage_dir, f"{dss_name}_{file_name}")
        
        # Load existing file data or create new
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                file_data = json.loads(f.read().decode())
        else:
            file_data = {'stripes': {}}
        
        # Store stripe data
        file_data['stripes'][str(stripe_num)] = {
            'type': block_type,
            'data': block_data.hex(),
            'size': data_size
        }
        
        # Save to file
        with open(file_path, 'wb') as f:
            f.write(json.dumps(file_data).encode())
        
        print(f"Stored {block_type} block for stripe {stripe_num} of {file_name}")
        return SUCCESS
    
    def read_block(self, args):
        """Read a data or parity block"""
        if len(args) < 3:
            return FAILURE
        
        dss_name, file_name, stripe_num = args[:3]
        stripe_num = int(stripe_num)
        
        file_path = os.path.join(self.storage_dir, f"{dss_name}_{file_name}")
        
        if not os.path.exists(file_path):
            return FAILURE
        
        try:
            with open(file_path, 'rb') as f:
                file_data = json.loads(f.read().decode())
            
            stripe_key = str(stripe_num)
            if stripe_key not in file_data['stripes']:
                return FAILURE
            
            stripe_data = file_data['stripes'][stripe_key]
            block_data = bytes.fromhex(stripe_data['data'])
            
            # In a real implementation, you'd send the actual data
            # For now, return success with data size
            return f"{SUCCESS} {stripe_data['size']} {stripe_data['type']}"
            
        except Exception as e:
            print(f"Error reading block: {e}")
            return FAILURE
    
    def get_stripe(self, args):
        """Get all blocks for a specific stripe (for recovery)"""
        if len(args) < 3:
            return FAILURE
        
        dss_name, file_name, stripe_num = args[:3]
        stripe_num = int(stripe_num)
        
        file_path = os.path.join(self.storage_dir, f"{dss_name}_{file_name}")
        
        if not os.path.exists(file_path):
            return FAILURE
        
        try:
            with open(file_path, 'rb') as f:
                file_data = json.loads(f.read().decode())
            
            stripe_key = str(stripe_num)
            if stripe_key not in file_data['stripes']:
                return FAILURE
            
            stripe_data = file_data['stripes'][stripe_key]
            return f"{SUCCESS} {stripe_data['type']} {stripe_data['size']}"
            
        except Exception as e:
            print(f"Error getting stripe: {e}")
            return FAILURE
    
    def simulate_failure(self):
        """Simulate disk failure"""
        self.failed = True
        print(f"Disk {self.disk_name} has failed!")
        return "FAIL-COMPLETE"
    
    def recover_stripe(self, dss_name, file_name, stripe_num, other_disk_info):
        """Recover a stripe from other disks using XOR parity"""
        print(f"Recovering stripe {stripe_num} for {file_name}")

        recovered_data = b'R' * 1024  # Dummy recovered data
        
        file_path = os.path.join(self.storage_dir, f"{dss_name}_{file_name}")
        
        # Load existing file data or create new
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                file_data = json.loads(f.read().decode())
        else:
            file_data = {'stripes': {}}
        
        # Store recovered stripe data
        file_data['stripes'][str(stripe_num)] = {
            'type': 'data',  # Assume we're recovering data
            'data': recovered_data.hex(),
            'size': len(recovered_data)
        }
        
        # Save to file
        with open(file_path, 'wb') as f:
            f.write(json.dumps(file_data).encode())
        
        print(f"Recovered stripe {stripe_num}")
        return SUCCESS
    def deregister_with_manager(self):
        """Deregister this disk with the manager"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.manager_ip, self.manager_port))
            
            # Send deregistration message
            message = Message.encode_message("deregister-disk", self.disk_name)
            Message.send_message(sock, message)
            
            # Wait for response
            response = Message.receive_message(sock)
            sock.close()
            
            if response == SUCCESS:
                print(f"Disk {self.disk_name} deregistered successfully")
                return True
            else:
                print(f"Failed to deregister disk {self.disk_name}")
                return False
                
        except Exception as e:
            print(f"Error deregistering with manager: {e}")
            return False

def main():
    if len(sys.argv) != 6:
        print("Usage: python disk.py <disk_name> <manager_ip> <manager_port> <m_port> <c_port>")
        sys.exit(1)
    
    disk_name = sys.argv[1]
    manager_ip = sys.argv[2]
    manager_port = int(sys.argv[3])
    m_port = int(sys.argv[4])
    c_port = int(sys.argv[5])
    
    disk = DSS_Disk(disk_name, manager_ip, manager_port, m_port, c_port)
    
    # Register with manager
    if not disk.register_with_manager():
        print("Failed to register with manager")
        sys.exit(1)
    
    # Start command server
    try:
        disk.start_command_server()
    except KeyboardInterrupt:
        print(f"\nShutting down disk {disk_name}...")
        # Deregister before shutting down
        disk.deregister_with_manager()
        print(f"Disk {disk_name} shut down")

if __name__ == "__main__":
    main()