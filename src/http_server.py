from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging



class NodeHTTPServer:
    def __init__ (self, node):
        self.node = node
        self.port = node.http_port
        
    def start(self):
        with HTTPServer(('', self.port), lambda *args: handler(self.node, *args)) as self.httpd:
            self.httpd.serve_forever()
    
    def shutdown(self):
        self.httpd.shutdown()


class handler(BaseHTTPRequestHandler):
    
    def __init__(self, node, *args, **kwargs):
        self.node = node
        super().__init__(*args, **kwargs) 
    

    def do_GET(self):

        if self.path == '/stats':
            self.send_response(200)
            self.send_header('Content-type','application/json')
            self.end_headers()

            stats_info = self.node.handle_stats_request()
            stats_info_json = to_json_format_2(stats_info)
            self.wfile.write(bytes(json.dumps(stats_info_json), "utf8"))

        elif self.path == '/network':
            self.send_response(200)
            self.send_header('Content-type','application/json')
            self.end_headers()

            network_info = self.node.handle_network_request()
            network_info_json = to_json_format(network_info)
            self.wfile.write(bytes(json.dumps(network_info_json), "utf8"))


    def do_POST(self):

        if self.path == '/solve':
            content_length = self.headers.get('Content-Length')
            if content_length is not None:
                content_length = int(content_length)
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode('utf-8'))
                
                if 'sudoku' in data:
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    
                    
                    sudoku = data['sudoku']
                    logging.info(f'{self.node.http_port} -> HTTP THREAD: Received Sudoku: {sudoku}')
                    solved_sudoku = self.node.handle_http_solve_request(sudoku)
                    
                    self.wfile.write(json.dumps(solved_sudoku).encode('utf-8'))
                    return
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'Missing or invalid Sudoku data')


def to_json_format(dic):
    endDic = {}
    for addr in dic.keys():
        key = f"{addr[0]}:{addr[1]}"
        endDic[key] = []
        for node in dic[addr]:
            endDic[key].append(f"{node[0]}:{node[1]}")
    return endDic
    
    
def to_json_format_2(dic):
    endDic = {str(key): value for key, value in dic.items()}
    return endDic