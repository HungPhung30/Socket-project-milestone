#!/usr/bin/env python3

import socket
import threading
import sys
import os
import math
from utils import Message, SUCCESS, FAILURE, calculate_parity, pad_block, get_disk_for_stripe

class DSS_User:
    def __init__(self, user_name, manager_ip, manager_port, m_port, c_port):
        self.user_name = user_name
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.m_port = m_port  # Management port
        self.c_port = c_port  # Command port
        self.lock = threading.Lock()
    
    def register_with_manager(self):
        """Register this user with the manager"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.manager_ip, self.manager_port))
            
            # Send registration message
            message = Message.encode_message("register-user", self.user_name, 
                                           "127.0.0.1", self.m_port, self.c_port)
            Message.send_message(sock, message)
            
            # Wait for response
            response = Message.receive_message(sock)
            sock.close()
            
            if response == SUCCESS:
                print(f"User {self.user_name} registered successfully")
                return True
            else:
                print(f"Failed to register user {self.user_name}")
                return False
                
        except Exception as e:
            print(f"Error registering with manager: {e}")
            return False
    
    def send_command_to_manager(self, command, *args):
        """Send a command to the manager and get response"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.manager_ip, self.manager_port))
            
            # Send command message
            message = Message.encode_message(command, *args)
            Message.send_message(sock, message)
            
            # Wait for response
            response = Message.receive_message(sock)
            sock.close()
            
            return response
                
        except Exception as e:
            print(f"Error sending command to manager: {e}")
            return FAILURE
    
    def list_files(self):
        """List all files in the DSS"""
        response = self.send_command_to_manager("ls")
        
        if response.startswith(SUCCESS):
            # Parse and display the file listing
            lines = response.split("\\n")
            print("DSS File Listing:")
            for line in lines[1:]:  # Skip the SUCCESS part
                if line.strip():
                    print(line)
        else:
            print("No DSSs configured or error occurred")
    
    def copy_file_to_dss(self, file_path):
        """Copy a file to the DSS"""
        if not os.path.exists(file_path):
            print(f"File {file_path} does not exist")
            return False
        
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        # Request copy operation from manager
        response = self.send_command_to_manager("copy", file_name, str(file_size), self.user_name)
        
        if not response.startswith(SUCCESS):
            print("Failed to initiate copy operation")
            return False
        
        # Parse manager response
        parts = response.split()
        if len(parts) < 6:
            print("Invalid response from manager")
            return False
        
        dss_name = parts[1]
        file_size_confirm = int(parts[2])
        n = int(parts[3])
        striping_unit = int(parts[4])
        num_disks = int(parts[5])
        
        # Parse disk information
        disk_info = []
        for i in range(num_disks):
            base_idx = 6 + i * 3
            if base_idx + 2 < len(parts):
                disk_info.append({
                    'name': parts[base_idx],
                    'address': parts[base_idx + 1],
                    'port': int(parts[base_idx + 2])
                })
        
        print(f"Copying {file_name} to DSS {dss_name}")
        print(f"DSS config: n={n}, striping_unit={striping_unit}")
        
        # Perform the actual copy operation
        success = self.perform_copy_operation(file_path, dss_name, disk_info, n, striping_unit)
        
        if success:
            # Notify manager of completion
            self.send_command_to_manager("copy-complete", dss_name, file_name, self.user_name)
            print(f"Successfully copied {file_name} to DSS")
        else:
            print(f"Failed to copy {file_name} to DSS")
        
        return success
    
    def perform_copy_operation(self, file_path, dss_name, disk_info, n, striping_unit):
        """Perform the actual copy operation using BIDP"""
        try:
            with open(file_path, 'rb') as f:
                file_data = f.read()
            
            file_name = os.path.basename(file_path)
            total_size = len(file_data)
            
            # Calculate number of stripes needed
            data_per_stripe = (n - 1) * striping_unit
            num_stripes = math.ceil(total_size / data_per_stripe)
            
            print(f"File size: {total_size} bytes, {num_stripes} stripes needed")
            
            # Process each stripe
            for stripe_num in range(num_stripes):
                start_pos = stripe_num * data_per_stripe
                end_pos = min(start_pos + data_per_stripe, total_size)
                stripe_data = file_data[start_pos:end_pos]
                
                # Split stripe into n-1 data blocks
                data_blocks = []
                for i in range(n - 1):
                    block_start = i * striping_unit
                    block_end = min(block_start + striping_unit, len(stripe_data))
                    
                    if block_start < len(stripe_data):
                        block_data = stripe_data[block_start:block_end]
                        # Pad block to striping_unit size
                        block_data = pad_block(block_data, striping_unit)
                    else:
                        # Empty block (all zeros)
                        block_data = b'\x00' * striping_unit
                    
                    data_blocks.append(block_data)
                
                # Calculate parity block
                parity_block = calculate_parity(data_blocks)
                
                # Determine which disk gets the parity
                parity_disk_idx = get_disk_for_stripe(stripe_num, n)
                
                # Store data blocks
                for i, block_data in enumerate(data_blocks):
                    disk_idx = i if i < parity_disk_idx else i + 1
                    success = self.store_block_on_disk(
                        disk_info[disk_idx], dss_name, file_name, 
                        stripe_num, "data", block_data
                    )
                    if not success:
                        print(f"Failed to store data block {i} of stripe {stripe_num}")
                        return False
                
                # Store parity block
                success = self.store_block_on_disk(
                    disk_info[parity_disk_idx], dss_name, file_name,
                    stripe_num, "parity", parity_block
                )
                if not success:
                    print(f"Failed to store parity block of stripe {stripe_num}")
                    return False
                
                print(f"Stored stripe {stripe_num}/{num_stripes}")
            
            return True
            
        except Exception as e:
            print(f"Error during copy operation: {e}")
            return False
    
    def store_block_on_disk(self, disk_info, dss_name, file_name, stripe_num, block_type, block_data):
        """Store a block on a specific disk"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((disk_info['address'], disk_info['port']))
            
            # Send store command
            message = Message.encode_message("store-block", dss_name, file_name, 
                                           str(stripe_num), block_type, str(len(block_data)))
            Message.send_message(sock, message)
            
            # In a real implementation, you'd send the actual block data here
            # For simulation, the disk creates dummy data
            
            # Wait for response
            response = Message.receive_message(sock)
            sock.close()
            
            return response == SUCCESS
            
        except Exception as e:
            print(f"Error storing block on disk {disk_info['name']}: {e}")
            return False
    
    def read_file_from_dss(self, dss_name, file_name):
        """Read a file from the DSS"""
        # Request read operation from manager
        response = self.send_command_to_manager("read", dss_name, file_name, self.user_name)
        
        if not response.startswith(SUCCESS):
            if "ownership" in response.lower() or "owner" in response.lower():
                print(f"Access denied: You don't own the file {file_name}")
            else:
                print("Failed to initiate read operation")
            return False
        
        # Parse manager response
        parts = response.split()
        if len(parts) < 5:
            print("Invalid response from manager")
            return False
        
        file_size = int(parts[1])
        n = int(parts[2])
        striping_unit = int(parts[3])
        num_disks = int(parts[4])
        
        # Parse disk information
        disk_info = []
        for i in range(num_disks):
            base_idx = 5 + i * 3
            if base_idx + 2 < len(parts):
                disk_info.append({
                    'name': parts[base_idx],
                    'address': parts[base_idx + 1],
                    'port': int(parts[base_idx + 2])
                })
        
        print(f"Reading {file_name} from DSS {dss_name}")
        print(f"File size: {file_size} bytes")
        
        # Perform the actual read operation
        file_data = self.perform_read_operation(dss_name, file_name, file_size, disk_info, n, striping_unit)
        
        if file_data:
            # Save the read file
            output_path = f"read_{file_name}"
            with open(output_path, 'wb') as f:
                f.write(file_data)
            
            # Notify manager of completion
            self.send_command_to_manager("read-complete", dss_name, file_name, self.user_name)
            print(f"Successfully read {file_name} and saved as {output_path}")
            return True
        else:
            print(f"Failed to read {file_name} from DSS")
            return False
    
    def perform_read_operation(self, dss_name, file_name, file_size, disk_info, n, striping_unit):
        """Perform the actual read operation using BIDP"""
        try:
            # Calculate number of stripes
            data_per_stripe = (n - 1) * striping_unit
            num_stripes = math.ceil(file_size / data_per_stripe)
            
            file_data = bytearray()
            
            # Read each stripe
            for stripe_num in range(num_stripes):
                stripe_data = self.read_stripe(dss_name, file_name, stripe_num, disk_info, n, striping_unit)
                if stripe_data is None:
                    print(f"Failed to read stripe {stripe_num}")
                    return None
                
                file_data.extend(stripe_data)
                print(f"Read stripe {stripe_num}/{num_stripes}")
            
            # Trim to actual file size
            return bytes(file_data[:file_size])
            
        except Exception as e:
            print(f"Error during read operation: {e}")
            return None
    
    def read_stripe(self, dss_name, file_name, stripe_num, disk_info, n, striping_unit):
        """Read a single stripe from the DSS"""
        # Determine parity disk for this stripe
        parity_disk_idx = get_disk_for_stripe(stripe_num, n)
        
        # Read data blocks (skip parity disk)
        data_blocks = []
        for i in range(n - 1):
            disk_idx = i if i < parity_disk_idx else i + 1
            
            block_data = self.read_block_from_disk(
                disk_info[disk_idx], dss_name, file_name, stripe_num
            )
            
            if block_data is None:
                print(f"Failed to read data block {i} from disk {disk_info[disk_idx]['name']}")
                # In a real implementation, you would attempt recovery here
                return None
            
            data_blocks.append(block_data)
        
        # Combine data blocks to form stripe
        stripe_data = bytearray()
        for block in data_blocks:
            stripe_data.extend(block)
        
        return bytes(stripe_data)
    
    def read_block_from_disk(self, disk_info, dss_name, file_name, stripe_num):
        """Read a block from a specific disk"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((disk_info['address'], disk_info['port']))
            
            # Send read command
            message = Message.encode_message("read-block", dss_name, file_name, str(stripe_num))
            Message.send_message(sock, message)
            
            # Wait for response
            response = Message.receive_message(sock)
            sock.close()
            
            if response.startswith(SUCCESS):
                # In a real implementation, you'd receive the actual block data
                # For simulation, return dummy data
                return b'D' * 1024  # Dummy data
            else:
                return None
            
        except Exception as e:
            print(f"Error reading block from disk {disk_info['name']}: {e}")
            return None
    
    def simulate_disk_failure(self, dss_name):
        """Simulate a disk failure in the DSS"""
        response = self.send_command_to_manager("disk-failure", dss_name)
        
        if response == SUCCESS:
            print(f"Simulated disk failure in DSS {dss_name}")
        else:
            print(f"Failed to simulate disk failure in DSS {dss_name}")
    
    def interactive_mode(self):
        """Run in interactive mode"""
        print(f"DSS User {self.user_name} - Interactive Mode")
        print("Commands:")
        print("  ls - List files")
        print("  copy <file_path> - Copy file to DSS")
        print("  read <dss_name> <file_name> - Read file from DSS")
        print("  fail <dss_name> - Simulate disk failure")
        print("  configure <dss_name> <n> <striping_unit> - Configure DSS")
        print("  decommission <dss_name> - Decommission DSS")
        print("  deregister - Deregister user")
        print("  quit - Exit")
        
        while True:
            try:
                command = input(f"{self.user_name}> ").strip().split()
                
                if not command:
                    continue
                
                if command[0] == "quit":
                    break
                elif command[0] == "ls":
                    self.list_files()
                elif command[0] == "copy" and len(command) == 2:
                    self.copy_file_to_dss(command[1])
                elif command[0] == "read" and len(command) == 3:
                    self.read_file_from_dss(command[1], command[2])
                elif command[0] == "fail" and len(command) == 2:
                    self.simulate_disk_failure(command[1])
                else:
                    print("Invalid command or arguments")
                    
            except KeyboardInterrupt:
                break
        
        # Deregister when exiting
        self.send_command_to_manager("deregister-user", self.user_name)
        print(f"User {self.user_name} deregistered")
        
    def configure_dss(self, dss_name, n, striping_unit):
        """Configure a new DSS"""
        response = self.send_command_to_manager("configure-dss", dss_name, str(n), str(striping_unit))
        
        if response == SUCCESS:
            print(f"Successfully configured DSS {dss_name} with n={n}, striping_unit={striping_unit}")
            return True
        else:
            print(f"Failed to configure DSS {dss_name}")
            return False

    def deregister(self):
        """Deregister this user from the manager"""
        response = self.send_command_to_manager("deregister-user", self.user_name)
        if response == SUCCESS:
            print(f"User {self.user_name} deregistered successfully")
            return True
        else:
            print(f"Failed to deregister user {self.user_name}")
            return False

# Update the interactive_mode method to include new commands
    def interactive_mode(self):
        """Run in interactive mode"""
        print(f"DSS User {self.user_name} - Interactive Mode")
        print("Commands:")
        print("  ls - List files")
        print("  copy <file_path> - Copy file to DSS")
        print("  read <dss_name> <file_name> - Read file from DSS")
        print("  fail <dss_name> - Simulate disk failure")
        print("  configure <dss_name> <n> <striping_unit> - Configure DSS")
        print("  deregister - Deregister user")
        print("  quit - Exit")
        
        while True:
            try:
                command = input(f"{self.user_name}> ").strip().split()
                
                if not command:
                    continue
                
                if command[0] == "quit":
                    break
                elif command[0] == "ls":
                    self.list_files()
                elif command[0] == "copy" and len(command) == 2:
                    self.copy_file_to_dss(command[1])
                elif command[0] == "read" and len(command) == 3:
                    self.read_file_from_dss(command[1], command[2])
                elif command[0] == "fail" and len(command) == 2:
                    self.simulate_disk_failure(command[1])
                elif command[0] == "configure" and len(command) == 4:
                    self.configure_dss(command[1], int(command[2]), int(command[3]))
                elif command[0] == "deregister":
                    if self.deregister():
                        break
                elif command[0] == "decommission" and len(command) == 2:
                    self.decommission_dss(command[1])
                else:
                    print("Invalid command or arguments")
                    
            except KeyboardInterrupt:
                break
        
        print(f"User {self.user_name} session ended")
    
    def decommission_dss(self, dss_name):
        """Decommission a DSS"""
        response = self.send_command_to_manager("decommission-dss", dss_name)
        
        if response == SUCCESS:
            print(f"Successfully decommissioned DSS {dss_name}")
            return True
        else:
            print(f"Failed to decommission DSS {dss_name}")
            return False
            

def main():
    if len(sys.argv) != 6:
        print("Usage: python user.py <user_name> <manager_ip> <manager_port> <m_port> <c_port>")
        sys.exit(1)
    
    user_name = sys.argv[1]
    manager_ip = sys.argv[2]
    manager_port = int(sys.argv[3])
    m_port = int(sys.argv[4])
    c_port = int(sys.argv[5])
    
    user = DSS_User(user_name, manager_ip, manager_port, m_port, c_port)
    
    # Register with manager
    if not user.register_with_manager():
        print("Failed to register with manager")
        sys.exit(1)
    
    # Start interactive mode
    try:
        user.interactive_mode()
    except KeyboardInterrupt:
        print(f"\nShutting down user {user_name}...")

if __name__ == "__main__":
    main()