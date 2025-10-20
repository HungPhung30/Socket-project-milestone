#!/usr/bin/env python3

import socket
import threading
import sys
import os
import math
import random
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
            # Notify manager of completion WITH file size
            self.send_command_to_manager("copy-complete", dss_name, file_name, self.user_name, str(file_size))
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
                
                threads = []
                results = [None] * n
                
                def write_block(idx, disk_idx, block_data, block_type):
                    results[idx] = self.store_block_on_disk(
                        disk_info[disk_idx], dss_name, file_name, 
                        stripe_num, block_type, block_data
                    )
                
                for i, block_data in enumerate(data_blocks):
                    disk_idx = i if i < parity_disk_idx else i + 1
                    t = threading.Thread(target=write_block,
                                         args=(i, disk_idx, block_data, "data"))
                    threads.append(t)
                    t.start()
                    
                t = threading.Thread(target=write_block,
                                     args=(n-1, parity_disk_idx, parity_block, "parity"))
                threads.append(t)
                t.start()
                
                for t in threads:
                    t.join()
                
                if not all(results[:len(threads)]):
                    print(f"Failed to store stripe {stripe_num}")
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
            
            sock.sendall(block_data)
            
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
        
        print(f"Manager response: {response}")  # Debug line
        
        if not response.startswith(SUCCESS):
            if "ownership" in response.lower() or "owner" in response.lower():
                print(f"Access denied: You don't own the file {file_name}")
            else:
                print(f"Failed to initiate read operation: {response}")
            return False
        
        # Parse manager response
        parts = response.split()
        if len(parts) < 5:
            print(f"Invalid response from manager: {response}")
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
        print(f"File size: {file_size} bytes, n={n}, striping_unit={striping_unit}")
        
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
    
    def perform_read_operation(self, dss_name, file_name, file_size, disk_info, n, striping_unit, error_probability=0):
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
            
            # Check if response is valid
            if response is None:
                print(f"No response from disk {disk_info['name']}")
                sock.close()
                return None
            
            if response.startswith(SUCCESS):
                parts = response.split()
                if len(parts) < 3:
                    print(f"Invalid response format from disk {disk_info['name']}: {response}")
                    sock.close()
                    return None
                
                data_size = int(parts[1])
                block_type = parts[2]
                
                # Receive the actual block data
                block_data = b''
                while len(block_data) < data_size:
                    chunk = sock.recv(min(4096, data_size - len(block_data)))
                    if not chunk:
                        print(f"Connection closed while receiving data from {disk_info['name']}")
                        sock.close()
                        return None
                    block_data += chunk
                
                sock.close()
                print(f"✓ Read {len(block_data)} bytes from {disk_info['name']} (stripe {stripe_num})")
                return block_data
            else:
                print(f"Failed response from disk {disk_info['name']}: {response}")
                sock.close()
                return None
            
        except Exception as e:
            print(f"Error reading block from disk {disk_info['name']}: {e}")
            return None
    
    def simulate_disk_failure(self, dss_name):
        """Simulate a disk failure in the DSS - simple version"""
        # Use the full implementation
        return self.disk_failure_with_recovery(dss_name)
            
    def disk_failure_with_recovery(self, dss_name):
        """Complete disk failure and recovery implementation"""
        import random
        
        # Phase 1: Get DSS parameters from manager
        response = self.send_command_to_manager("disk-failure", dss_name)
        
        if not response.startswith(SUCCESS):
            print(f"Failed to initiate disk failure for DSS {dss_name}")
            return False
        
        # Parse response to get DSS parameters
        parts = response.split()
        n = int(parts[1])
        striping_unit = int(parts[2])
        num_disks = int(parts[3])
        
        # Parse disk information
        disk_info = []
        for i in range(num_disks):
            base_idx = 4 + i * 3
            if base_idx + 2 < len(parts):
                disk_info.append({
                    'name': parts[base_idx],
                    'address': parts[base_idx + 1],
                    'port': int(parts[base_idx + 2])
                })
        
        print(f"\n=== Disk Failure Simulation ===")
        print(f"DSS {dss_name}: n={n}, striping_unit={striping_unit}")
        print(f"Disks: {[d['name'] for d in disk_info]}")
        
        # Select a random disk to fail
        failed_disk_idx = random.randint(0, n - 1)
        failed_disk = disk_info[failed_disk_idx]
        
        print(f"\n→ Failing disk: {failed_disk['name']} (index {failed_disk_idx})")
        
        # Send fail command to the selected disk
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((failed_disk['address'], failed_disk['port']))
            
            message = Message.encode_message("fail")
            Message.send_message(sock, message)
            
            response = Message.receive_message(sock)
            sock.close()
            
            if response != "FAIL-COMPLETE":
                print(f"Failed to simulate disk failure")
                return False
            
            print(f"✓ Disk {failed_disk['name']} has failed and deleted its contents")
            
        except Exception as e:
            print(f"Error failing disk: {e}")
            return False
        
        # Phase 2: Recover the failed disk
        print(f"\n=== Recovery Process ===")
        print(f"Recovering disk {failed_disk['name']} using parity from other disks...")
        
        # Get list of files on this DSS from manager
        ls_response = self.send_command_to_manager("ls")
        files_on_dss = []
        
        if ls_response.startswith(SUCCESS):
            lines = ls_response.split("\\n")
            in_target_dss = False
            for line in lines:
                if dss_name in line and "Disk array" in line:
                    in_target_dss = True
                elif "Disk array" in line:
                    in_target_dss = False
                elif in_target_dss and line.strip() and not line.startswith(dss_name):
                    # Parse file line: "  filename size B owner"
                    parts = line.strip().split()
                    if len(parts) >= 3:
                        file_name = parts[0]
                        file_size = int(parts[1])
                        files_on_dss.append({'name': file_name, 'size': file_size})
        
        if not files_on_dss:
            print(f"No files found on DSS {dss_name} (might be empty)")
        else:
            print(f"Found {len(files_on_dss)} file(s) to recover:")
            for f in files_on_dss:
                print(f"  - {f['name']} ({f['size']} bytes)")
        
        # Recover each file
        for file_info in files_on_dss:
            print(f"\nRecovering {file_info['name']}...")
            success = self.recover_file_on_failed_disk(
                dss_name, file_info['name'], file_info['size'],
                failed_disk_idx, disk_info, n, striping_unit
            )
            if not success:
                print(f"✗ Failed to recover file {file_info['name']}")
                return False
            print(f"✓ Recovered file {file_info['name']}")
        
        # Phase 3: Notify manager of recovery completion
        response = self.send_command_to_manager("recovery-complete", dss_name, failed_disk['name'])
        
        if response == SUCCESS:
            print(f"\n=== Recovery Complete ===")
            print(f"Successfully recovered disk {failed_disk['name']}")
            return True
        else:
            print(f"Manager did not acknowledge recovery completion")
            return False

    def recover_file_on_failed_disk(self, dss_name, file_name, file_size, failed_disk_idx, disk_info, n, striping_unit):
        """Recover all stripes of a file on the failed disk using XOR"""
        try:
            # Calculate number of stripes
            data_per_stripe = (n - 1) * striping_unit
            num_stripes = math.ceil(file_size / data_per_stripe)
            
            print(f"  Total stripes to recover: {num_stripes}")
            
            # Recover each stripe
            for stripe_num in range(num_stripes):
                # Determine which disk has the parity for this stripe
                parity_disk_idx = get_disk_for_stripe(stripe_num, n)
                
                # Read blocks from all disks except the failed one
                blocks = []
                
                # Collect n-1 blocks (excluding failed disk)
                for i in range(n):
                    if i != failed_disk_idx:
                        block_data = self.read_block_from_disk(
                            disk_info[i], dss_name, file_name, stripe_num
                        )
                        
                        if block_data is None:
                            print(f"    ✗ Failed to read block from disk {disk_info[i]['name']}")
                            return False
                        
                        blocks.append(block_data)
                
                # Compute the missing block using XOR
                recovered_block = calculate_parity(blocks)
                
                # Determine if the recovered block is data or parity
                if failed_disk_idx == parity_disk_idx:
                    block_type = "parity"
                else:
                    block_type = "data"
                
                # Write the recovered block to the failed disk
                success = self.store_block_on_disk(
                    disk_info[failed_disk_idx], dss_name, file_name,
                    stripe_num, block_type, recovered_block
                )
                
                if not success:
                    print(f"    ✗ Failed to write recovered stripe {stripe_num}")
                    return False
                
                if (stripe_num + 1) % 10 == 0 or stripe_num == num_stripes - 1:
                    print(f"  Progress: {stripe_num + 1}/{num_stripes} stripes recovered")
            
            return True
            
        except Exception as e:
            print(f"Error recovering file: {e}")
            return False
    
    def interactive_mode(self):
        """Run in interactive mode"""
        print(f"DSS User {self.user_name} - Interactive Mode")
        print("Commands:")
        print("  ls - List files")
        print("  copy <file_path> - Copy file to DSS")
        print("  read <dss_name> <file_name> - Read file from DSS")
        print("  fail <dss_name> - Simulate disk failure with recovery")
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
                    self.disk_failure_with_recovery(command[1])  # Use new method
                elif command[0] == "configure" and len(command) == 4:
                    self.configure_dss(command[1], int(command[2]), int(command[3]))
                elif command[0] == "decommission" and len(command) == 2:
                    self.decommission_dss(command[1])
                elif command[0] == "deregister":
                    if self.deregister():
                        break
                else:
                    print("Invalid command or arguments")
                    
            except KeyboardInterrupt:
                break
        
        print(f"User {self.user_name} session ended")
        
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
    

    def read_stripe_with_verification(self, dss_name, file_name, stripe_num, disk_info, n, striping_unit, error_probability=0):
        """Read a stripe with parity verification and error injection"""
        max_retries = 3
        
        for attempt in range(max_retries):
            # Determine parity disk for this stripe
            parity_disk_idx = get_disk_for_stripe(stripe_num, n)
            
            # Read all blocks (data + parity) in parallel
            threads = []
            blocks = [None] * n
            
            def read_block_thread(idx, disk_idx):
                blocks[idx] = self.read_block_from_disk(
                    disk_info[disk_idx], dss_name, file_name, stripe_num
                )
            
            # Read data blocks
            for i in range(n - 1):
                disk_idx = i if i < parity_disk_idx else i + 1
                t = threading.Thread(target=read_block_thread, args=(i, disk_idx))
                threads.append(t)
                t.start()
            
            # Read parity block
            t = threading.Thread(target=read_block_thread, args=(n-1, parity_disk_idx))
            threads.append(t)
            t.start()
            
            # Wait for all reads
            for t in threads:
                t.join()
            
            # Check if all reads succeeded
            if None in blocks:
                print(f"Failed to read some blocks in stripe {stripe_num}, attempt {attempt + 1}")
                continue
            
            # Introduce error with probability p
            if error_probability > 0 and random.randint(0, 100) < error_probability:
                error_block_idx = random.randint(0, n - 1)
                error_bit_idx = random.randint(0, len(blocks[error_block_idx]) * 8 - 1)
                byte_idx = error_bit_idx // 8
                bit_idx = error_bit_idx % 8
                
                blocks[error_block_idx] = bytearray(blocks[error_block_idx])
                blocks[error_block_idx][byte_idx] ^= (1 << bit_idx)
                blocks[error_block_idx] = bytes(blocks[error_block_idx])
                
                print(f"Injected error in block {error_block_idx} of stripe {stripe_num}")
            
            # Verify parity
            data_blocks = blocks[:n-1]
            parity_block = blocks[n-1]
            
            computed_parity = calculate_parity(data_blocks)
            
            if computed_parity == parity_block:
                print(f"Parity verified for stripe {stripe_num}")
                # Combine data blocks
                stripe_data = bytearray()
                for block in data_blocks:
                    stripe_data.extend(block)
                return bytes(stripe_data)
            else:
                print(f"Parity verification failed for stripe {stripe_num}, attempt {attempt + 1}")
        
        print(f"Failed to read stripe {stripe_num} after {max_retries} attempts")
        return None

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
        
        if not response.startswith(SUCCESS):
            print(f"Failed to decommission DSS {dss_name}")
            return False
        
        # Parse response
        parts = response.split()
        n = int(parts[1])
        num_disks = int(parts[3])
        
        # Parse disk information
        disk_info = []
        for i in range(num_disks):
            base_idx = 4 + i * 3
            if base_idx + 2 < len(parts):
                disk_info.append({
                    'name': parts[base_idx],
                    'address': parts[base_idx + 1],
                    'port': int(parts[base_idx + 2])
                })
        
        print(f"Decommissioning DSS {dss_name}...")
        
        # Instruct each disk to delete its contents
        for disk in disk_info:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((disk['address'], disk['port']))
                
                message = Message.encode_message("delete-all", dss_name)
                Message.send_message(sock, message)
                
                response = Message.receive_message(sock)
                sock.close()
                
                if response == SUCCESS:
                    print(f"Deleted contents from disk {disk['name']}")
                else:
                    print(f"Failed to delete from disk {disk['name']}")
                
            except Exception as e:
                print(f"Error deleting from disk {disk['name']}: {e}")
        
        # Notify manager that decommission is complete
        response = self.send_command_to_manager("decommission-complete", dss_name)
        
        if response == SUCCESS:
            print(f"Successfully decommissioned DSS {dss_name}")
            return True
        else:
            print(f"Failed to complete decommission")
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