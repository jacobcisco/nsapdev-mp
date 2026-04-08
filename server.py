import socket
import threading
import re

HOST = "0.0.0.0"
PORT = 8080

# Thread-safe shared log storage
log_entries = []
lock = threading.Lock()

# Syslog regex parser
SYSLOG_REGEX = re.compile(
    r'^(?P<timestamp>\w{3}\s+\d+\s[\d:]+)\s+'
    r'(?P<hostname>\S+)\s+'
    r'(?P<process>[^\:]+):\s+'
    r'(?P<message>.*)$'
)

# PARSING 
def parse_syslog_line(line):
    match = SYSLOG_REGEX.match(line)
    if match:
        data = match.groupdict()
        data["severity"] = extract_severity(data["message"])
        data["raw"] = line.strip()
        return data
    return None


def extract_severity(message):
    severities = ["ERROR", "error", "WARN", "warn", "INFO", "info", "DEBUG", "debug", "CRITICAL", "critical"]
    for sev in severities:
        if sev in message.upper():
            return sev
    return "INFO"


# CLIENT HANDLER
def handle_client(conn, addr):
    print(f"[CONNECTED] {addr}")

    try:
        conn.settimeout(10)

        try:
            data = conn.recv(1024).decode().strip()
        except socket.timeout:
            conn.send(b"ERROR: Request timeout.")
            return

        if not data:
            conn.send(b"ERROR: Empty request.")
            return

        if data.startswith("INGEST"):
            receive_file_streaming(conn)

        elif data.startswith("QUERY"):
            process_query(conn, data)

        elif data.startswith("PURGE"):
            purge_logs(conn)

        else:
            conn.send(b"ERROR: Unknown command")

    except Exception as e:
        try:
            conn.send(f"ERROR: Server exception -> {str(e)}".encode())
        except:
            pass

    finally:
        conn.close()
        print(f"[DISCONNECTED] {addr}")


# STREAMING INGEST
def receive_file_streaming(conn):
    try:
        conn.send(b"READY")
    except:
        return

    buffer = ""
    count = 0

    try:
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break

            try:
                buffer += chunk.decode(errors="ignore")
            except:
                continue

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)

                parsed = parse_syslog_line(line)

                if parsed:
                    with lock:
                        log_entries.append(parsed)
                    count += 1

        conn.send(f"SUCCESS: {count} entries ingested.".encode())

    except Exception as e:
        try:
            conn.send(f"ERROR: Ingestion failed -> {str(e)}".encode())
        except:
            pass


# QUERY PROCESSING (STREAMING OUTPUT)
def process_query(conn, command):
    try:
        parts = command.split(" ", 2)

        if len(parts) < 3:
            conn.send(b"ERROR: Invalid QUERY format.")
            return

        _, _, query_body = parts
        query_parts = query_body.split(" ", 1)

        if len(query_parts) < 2:
            conn.send(b"ERROR: Invalid QUERY command.")
            return

        query_type = query_parts[0]
        argument = query_parts[1].strip('"')

        with lock:
            if query_type == "SEARCH_DATE":
                results = [e["raw"] for e in log_entries if argument in e["timestamp"]]

            elif query_type == "SEARCH_HOST":
                results = [e["raw"] for e in log_entries if e["hostname"] == argument]

            elif query_type == "SEARCH_DAEMON":
                results = [e["raw"] for e in log_entries if argument in e["process"]]

            elif query_type == "SEARCH_SEVERITY":
                results = [e["raw"] for e in log_entries if e["severity"] == argument.upper()]

            elif query_type == "SEARCH_KEYWORD":
                results = [e["raw"] for e in log_entries if argument in e["message"]]

            elif query_type == "COUNT_KEYWORD":
                count = sum(1 for e in log_entries if argument in e["message"])
                conn.send(f"COUNT: {count}".encode())
                return

            else:
                conn.send(b"ERROR: Invalid QUERY type.")
                return

        conn.send(f"Found {len(results)} entries:\n".encode())

        for i, line in enumerate(results, 1):
            try:
                conn.send(f"{i}. {line}\n".encode())
            except:
                break

    except Exception as e:
        try:
            conn.send(f"ERROR: Query failed -> {str(e)}".encode())
        except:
            pass


# PURGE
def purge_logs(conn):
    with lock:
        count = len(log_entries)
        log_entries.clear()

    conn.send(f"SUCCESS: Deleted {count} entries.".encode())


# SERVER
def start_server():
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            server.bind((HOST, PORT))
        except OSError:
            print("ERROR: Port already in use.")
            return

        server.listen()
        print(f"[LISTENING] Server running on port {PORT}")

        while True:
            try:
                conn, addr = server.accept()
                thread = threading.Thread(target=handle_client, args=(conn, addr))
                thread.daemon = True
                thread.start()
            except Exception as e:
                print(f"ERROR: Accept failed -> {e}")

    except Exception as e:
        print(f"ERROR: Server startup failed -> {e}")


if __name__ == "__main__":
    start_server()
