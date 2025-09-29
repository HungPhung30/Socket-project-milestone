import json
import struct
import socket

# Return codes
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"

class Message:
    """Class to handle message formatting and parsing"""
    
    @staticmethod
    def encode_message(command, *args):
        """Encode a command and arguments into a message string"""
        message_parts = [command] + list(args)
        message = " ".join(str(part) for part in message_parts)
        return message.encode('utf-8')
    
    @staticmethod
    def decode_message(data):
        """Decode a message string into command and arguments"""
        if isinstance(data, bytes):
            message = data.decode('utf-8').strip()
        else:
            message = str(data).strip()
        parts = message.split()
        if not parts:
            return None, []
        return parts[0], parts[1:]
    
    @staticmethod
    def send_message(sock, message):
        """Send a message with length prefix"""
        if isinstance(message, str):
            message = message.encode('utf-8')
        
        # Send length first (4 bytes), then message
        length = len(message)
        sock.sendall(struct.pack('!I', length))
        sock.sendall(message)
    
    @staticmethod
    def receive_message(sock):
        """Receive a message with length prefix"""
        try:
            # Receive length first
            length_data = b''
            while len(length_data) < 4:
                chunk = sock.recv(4 - len(length_data))
                if not chunk:
                    return None
                length_data += chunk
            
            length = struct.unpack('!I', length_data)[0]
            
            # Receive message
            message_data = b''
            while len(message_data) < length:
                chunk = sock.recv(length - len(message_data))
                if not chunk:
                    return None
                message_data += chunk
            
            return message_data.decode('utf-8')
        except Exception as e:
            print(f"Error receiving message: {e}")
            return None

def calculate_parity(data_blocks):
    """Calculate XOR parity for a list of data blocks"""
    if not data_blocks:
        return b''
    
    parity = bytearray(data_blocks[0])
    for block in data_blocks[1:]:
        for i in range(min(len(parity), len(block))):
            parity[i] ^= block[i]
    
    return bytes(parity)

def pad_block(data, block_size):
    """Pad data to block_size with null bytes"""
    if len(data) >= block_size:
        return data[:block_size]
    return data + b'\x00' * (block_size - len(data))

def get_disk_for_stripe(stripe_num, total_disks):
    """Calculate which disk should store the parity for a given stripe"""
    return (total_disks - 1 - (stripe_num % total_disks)) % total_disks