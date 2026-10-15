"""HTTP receiver used only by the test suite."""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from time import time

events = {}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        data = json.dumps(events).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        event_id = data["event_id"]
        entry = events.setdefault(event_id, {"payment_id": data["payment_id"], "attempts": []})
        entry["attempts"].append(time())
        failed = self.path == "/fail" or (self.path == "/flaky" and len(entry["attempts"]) == 1)
        self.send_response(503 if failed else 204)
        self.send_header("Content-Length", "0")
        self.end_headers()
        print(f"event_id={event_id} attempts={len(entry['attempts'])} failed={failed}", flush=True)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
