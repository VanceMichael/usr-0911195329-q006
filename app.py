import json,os
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
SERVICE="solar-calibration"
def health_payload(): return {"service":SERVICE,"status":"ok"}
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  if self.path!="/health": self.send_error(404); return
  b=json.dumps(health_payload()).encode(); self.send_response(200); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(b))); self.end_headers(); self.wfile.write(b)
 def log_message(self,*a): pass
if __name__=="__main__": ThreadingHTTPServer(("0.0.0.0",int(os.getenv("PORT","8080"))),H).serve_forever()
