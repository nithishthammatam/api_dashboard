import socket
import ssl

def check_fcm_connection():
    host = "fcm.googleapis.com"
    port = 443
    print(f"Checking connection to {host}:{port}...")
    
    try:
        # Standard socket
        sock = socket.create_connection((host, port), timeout=10)
        print("✅ Socket connection successful")
        
        # SSL wrapper
        context = ssl.create_default_context()
        ssl_sock = context.wrap_socket(sock, server_hostname=host)
        print(f"✅ SSL handshake successful. Protocol: {ssl_sock.version()}")
        
        cert = ssl_sock.getpeercert()
        print("✅ Peer certificate retrieved")
        
        ssl_sock.close()
        print("Done.")
        return True
    except Exception as e:
        print(f"❌ Connection failed: {type(e).__name__}: {e}")
        return False

if __name__ == "__main__":
    check_fcm_connection()
