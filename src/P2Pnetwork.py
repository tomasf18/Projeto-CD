import json
import selectors
import socket
import logging
import time
import traceback
import threading
from P2Pprotocol import P2PProtocol

logging.basicConfig(filename="P2Pnetwork.py.log", level=logging.DEBUG)

class P2PNetwork:
    def __init__(self, node):
        """Initializes the P2P interface of the node."""
        # ----- Node information on the P2P network ----- #
        self.node = node
        self.port = node.p2p_port
        self.addr = ('', self.port)
        self.selector = node.selector
    
        # ----- Node's P2P network state information ----- #
        self.last_stats = {}                                    # Dictionary of last STATS messages received: {peer: time, ...}
        
        self.peers = []                                         # List of PEER ADDRESSES: [(addr, port), ...]
        self.peers_sock = {}                                    # Dictionary of PEER ADDRESSES AND MY CONNECTION TO THEM: {(addr, port): socket, ...}
        self.network_info = {}                                  # Dictionary of nodes and their peers (NETWORK INFO): {peer: [peerOfPeer1, ...], ...}
        
        self.stats_info = {                                     # Dictionary of nodes and their validations (STATS INFO): {peer: validations, ...}
            "all": {
                "solved": 0,
                "validations": 0
            },
            "nodes": [
                {
                    "address": f'{self.addr[0]}:{self.addr[1]}',
                    "validations": 0
                }
            ]
        }
        
        self.solved_sudokus = {}                                # Dictionary of solved sudokus: {sudoku: solution, ...}                    
        
        # ----- Load Balancer variables ----- #
        self.response_times = {self.addr: 0}                    # Dictionary of response times of nodes: {peer: response_time, ...}
        self.avg_response_times = {self.addr: 0}                # Dictionary of average response times of nodes: {peer: avg_response_time, ...
        self.past_response_times = {self.addr: []}              # Dictionary of past response times of nodes: {peer: [past_response_times], ...}
        self.current = -1

        # ----- Sudoku node variables ----- #
        self.sudoku_node = False
        self.sudoku = None
        self.generated_solutions = {}
        self.work_units = {}                                    # Dictionary of work units: {work_unit: (start, end), ...}
        self.nodes_available = [self.addr]                      # Nodes available for work (including myself)
        self.nodes_jobs = {self.addr: None}                     # Working nodes with their current work (including myself)


    ### Start Functions ###
    def start(self):
        """Starts the P2P network."""
        if self.node.first_peer:
            self.listen_for_connections()
        else:
            self.connect_to_initial_peer()
        

    def listen_for_connections(self):
        """Listens for incoming P2P connections."""
        self.node_p2p_endpoint = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.node_p2p_endpoint.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.node_p2p_endpoint.bind(self.addr)
        self.node_p2p_endpoint.listen(100)
        self.selector.register(self.node_p2p_endpoint, selectors.EVENT_READ, self.handle_new_connection)
        print(f'Node is listening for P2P connections on port {self.port}')
        logging.debug(f'{self.addr} -> Node is listening for P2P connections on port {self.port}')


    def connect_to_initial_peer(self):
        """Connects to the initial peer."""
        try:
            self.listen_for_connections()
            self.node_p2p_endpoint = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.node_p2p_endpoint.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.node_p2p_endpoint.connect(self.node.initial_peer_addr)
            self.node_p2p_endpoint.setblocking(False)
            self.selector.register(self.node_p2p_endpoint, selectors.EVENT_READ, self.read_new_message)
            logging.debug(f'{self.addr} -> Connected to initial peer {self.node.initial_peer_addr}')
            get_list_message = P2PProtocol.get_list((self.addr))
            P2PProtocol.send_message(self.node_p2p_endpoint, get_list_message)
            get_stats_message = P2PProtocol.get_stats(self.addr)
            P2PProtocol.send_message(self.node_p2p_endpoint, get_stats_message)
            logging.debug(f'{self.addr} -> Sent GET_LIST and GET_STATS messages to {self.node.initial_peer_addr}')
        except Exception as e:
            logging.error(f'Failed to connect to initial peer: {e}')
            logging.error(traceback.format_exc())


    def handle_new_connection(self, sock, mask):
        """Handles a new connection from another node."""
        conn_with_other, other_addr = sock.accept()
        logging.debug(f'{self.addr} -> Accepted connection from {other_addr}')
        conn_with_other.setblocking(False)
        self.selector.register(conn_with_other, selectors.EVENT_READ, self.read_new_message)


    ### Message Reader ###
    def read_new_message(self, other_node_sock, mask):
        """Reads a new message from another node."""

        try:
            data = P2PProtocol.receive_message(other_node_sock)
            print(f"Data: {data}")

            if data is None:
                print('Connection closed by peer.')
                disconnected_node_addr = self.get_addr_from_sock(other_node_sock)
                if disconnected_node_addr is not None:  # If I didn't already remove the peer
                    self.handle_node_disconnect(disconnected_node_addr)
                else:
                    other_node_sock.close()
                return
            
            message = json.loads(str(data))
            command = message["command"]
            args = message["args"]

            logging.debug(f'{self.addr} -> Received message: {message}')


            if command == "STATS":
                other_node_addr = tuple(args["addr"])
                other_node_validations = args["validations"]
                confirmed_work = args["confirm_work"]
                self.handle_stats(other_node_sock, other_node_addr, other_node_validations, confirmed_work)

            elif command == "GET_STATS":
                other_node_addr = tuple(args)
                stats = self.stats_info
                stats_message = P2PProtocol.stats_rep(self.addr, stats)
                P2PProtocol.send_message(other_node_sock, stats_message)
                logging.debug(f'{self.addr} -> Sent STATS_REP to {other_node_addr}')

            elif command == "STATS_REP":
                stats = args["stats"]
                other_node_addr = tuple(args["addr"])
                self.stats_info = stats
                logging.debug(f'{self.addr} -> Received STATS_REP from {other_node_addr}')

            elif command == "GET_LIST":
                other_node_addr = tuple(args)
                self.handle_get_list(other_node_sock, other_node_addr)

            elif command == "LIST_REP":
                nodes_list = args["nodes"]
                other_node_addr = tuple(args["addr"])
                self.handle_list_rep(other_node_sock, other_node_addr, nodes_list)

            elif command == "JOIN_REQ":
                other_node_addr = tuple(args)
                self.handle_join_req(other_node_sock, other_node_addr)

            elif command == "JOIN_REP":
                other_node_addr = tuple(args)
                self.handle_join_rep(other_node_sock, other_node_addr)

            elif command == "TOPOLOGY_CHANGE":
                other_node_addr = tuple(args["addr"])
                node_updated_peers = args["peers"]
                self.handle_topology_change(other_node_addr, node_updated_peers)

            elif command == "DO_WORK":
                self.node.handle_p2p_solve_request(data)

            elif command == "WORK_DONE":
                other_node_addr = tuple(args["addr"])
                self.handle_work_done(other_node_addr)

            elif command == "SOLVED_SUDOKU":
                other_node_addr = tuple(args["addr"])
                sudoku = args["sudoku"]
                solved_sudoku = args["solved_sudoku"]
                self.handle_solved_sudoku(other_node_addr, sudoku, solved_sudoku)

            else:
                print('Unknown command')

        except Exception as e:
            logging.error(f'Error handling message: {e}')
            logging.error(traceback.format_exc())
            self.handle_node_disconnect(other_node_sock)



    ### Message Handlers ###
    
    def handle_stats(self, other_node_conn, other_node_addr, other_node_validations, confirmed_work):
        self.last_stats[other_node_addr] = time.monotonic()                             # Update last_stats list
        logging.debug(f'{self.addr} -> CONFIRMED_WORK: {confirmed_work}')
        if confirmed_work == 1:
            self.update_stats(other_node_addr, other_node_validations)                # Update stats_info list
        logging.debug(f'{self.addr} -> Received STATS from {other_node_addr} ' + ('and updated stats_info' if confirmed_work == 1 else ''))
        

    def handle_get_list(self, other_node_conn, other_node_addr):
        self.add_peer(other_node_addr, other_node_conn)                                 # Before sending the list, add the node that requested the list, in case two requests are made at the same time         
        list_rep_message = P2PProtocol.list_rep(self.addr, self.peers)
        P2PProtocol.send_message(other_node_conn, list_rep_message)
        logging.debug(f'{self.addr} -> Sent list of peers to {other_node_addr}, but first added to peers list')


    def handle_list_rep(self, other_node_sock, other_node_addr, nodes_list):
        if len(self.peers) == 0:                                                        # Means i'm trying to enter the network
            nodes_list.append(other_node_addr)                                          # [node1, node2, node3, other_node_addr], where node1, node2, node3 are the peers of other_node_addr
            for node in nodes_list:
                if tuple(node) != other_node_addr and tuple(node) != self.addr:
                    self.connect_to_peer(tuple(node))
                elif node == other_node_addr:
                    join_req_message = P2PProtocol.join_req(self.addr)
                    P2PProtocol.send_message(other_node_sock, join_req_message)
                    logging.debug(f'{self.addr} -> Sent JOIN_REQ to {other_node_addr}')


    def handle_join_req(self, other_node_conn, other_node_addr):
        if other_node_addr not in self.peers:                                           # Because I might be the one whe received before the GET_LIST and already added the node
            self.add_peer(other_node_addr, other_node_conn)
            logging.debug(f'{self.addr} -> Added {other_node_addr} to peers list')
            if self.sudoku_node:                                                        # If I'm the sudoku node, I need to distribute the solutions to the new node
                self.distribute_solutions()

        join_rep_message = P2PProtocol.join_rep(self.addr)
        P2PProtocol.send_message(other_node_conn, join_rep_message)
        topology_change_message = P2PProtocol.topology_change(self.addr, self.peers)
        self.send_broadcast(topology_change_message)
        logging.debug(f'{self.addr} -> Sent JOIN_REP to {other_node_addr} ')


    def handle_join_rep(self, other_node_conn, other_node_addr):
        self.add_peer(other_node_addr, other_node_conn)
        logging.debug(f'{self.addr} -> Added {other_node_addr} to peers list')
        print(f"Self peers: {self.peers}")
        topology_change_message = P2PProtocol.topology_change(self.addr, self.peers)
        self.send_broadcast(topology_change_message)                                    # When this is the last node in the network, none TOPOLOGY_CHANGE is sent, therefore this same node doesn't reveive any message


    def handle_topology_change(self, other_node_addr, node_updated_peers):
        self.network_info[other_node_addr] = node_updated_peers
        for peer in self.peers:
            if peer not in self.nodes_available and self.nodes_jobs[peer] is None:
                self.nodes_available.append(peer)
        logging.debug(f'{self.addr} -> My updated topology info: {self.network_info}')


    def handle_work_done(self, other_node_addr):
        self.update(other_node_addr)
        work_done = self.nodes_jobs[other_node_addr]

       
        self.nodes_available.append(other_node_addr)
        self.nodes_jobs[other_node_addr] = None
    
        logging.debug(f'{self.addr} -> Received WORK_DONE from {other_node_addr} and popped work {work_done} from work_units')
        logging.debug(f'{self.addr} -> Nodes available for work: {self.nodes_available}')

        self.distribute_solutions()


    def handle_solved_sudoku(self, other_node_addr, sudoku, solved_sudoku):
        # solved_sudoku is the key of the solution in the generated_solutions dictionary when the node is the sudoku node    
        logging.debug(f'{self.addr} -> Received SOLVED_SUDOKU from {other_node_addr}, {solved_sudoku}')
        if self.sudoku_node and self.sudoku == sudoku:
            self.solved_sudokus[str(sudoku)] = self.generated_solutions[solved_sudoku]
            self.node.solved += 1
            print(self.node.solved)
            self.stats_info["all"]["solved"] = self.node.solved
            self.terminate_sudoku_node_state(self.solved_sudokus[str(sudoku)])
        else:
            self.solved_sudokus[str(sudoku)] = solved_sudoku
            self.node.solved += 1
            print(self.node.solved)
            self.stats_info["all"]["solved"] = self.node.solved


    ### Other Handlers ###

    def handle_node_disconnect(self, leaving_node_addr):
        logging.debug(f'{self.addr} -> Connection closed by peer {leaving_node_addr}, disconnecting...') 

        leaving_node_sock = self.peers_sock[leaving_node_addr]

        self.remove_peer(leaving_node_addr, leaving_node_sock)

        topology_change_message = P2PProtocol.topology_change(self.addr, self.peers)
        self.send_broadcast(topology_change_message)


    def handle_network_request(self):
        logging.debug(f'{self.addr} -> Received network request. Sending network info...')
        return self.network_info
    
    
    def handle_stats_request(self):
        logging.debug(f'{self.addr} -> Received stats request. Sending stats...')
        self.update_stats(self.addr, self.node.validations)
        return self.stats_info



    ### Message Senders ###
        
    def send_stats(self, confirmed_work):
        if (self.peers == []):
            return
        
        # update the validations for the node
        if confirmed_work == 1:
            self.update_stats(self.addr, self.node.validations)

        stats_message = P2PProtocol.stats(self.addr, self.node.validations, confirmed_work)
        self.send_broadcast(stats_message)
        
    

    def calculate_work_units(self, batch, sudoku, generated_solutions):
        logging.debug(f'{self.addr} -> I\'M THE SUDOKU NODE!')
        self.sudoku_node = True
        self.sudoku = sudoku
        self.generated_solutions = generated_solutions

        n_solutions = len(generated_solutions)
        self.work_units = {}
        n_work_units = n_solutions // batch + 1
        for i in range(1, n_work_units):
            self.work_units[i-1] = ((batch*(i-1)), (batch*i)-1)
        self.work_units[n_work_units-1] = (n_solutions-(n_solutions%batch), n_solutions-1)
        logging.debug(f'{self.addr} -> Work units calculated: {self.work_units}')
        self.distribute_solutions()


    def distribute_solutions(self):
        if len(self.work_units) == 0:
            for (node, work) in self.nodes_jobs.items():                                    # Although I have no more worke to distribute, if I have nodes with work, I need to wait for them to finish
                if work is not None:
                    return
            logging.debug(f'{self.addr} -> No work units to distribute for sudoku {self.sudoku}, returning random value (only for testing) to HTTP')
            # No solution found
            self.solved_sudokus[str(self.sudoku)] = -1
            self.node.solved += 1
            self.stats_info["all"]["solved"] = self.node.solved
            self.terminate_sudoku_node_state(-1)
            logging.debug(f'{self.addr} -> I\'M NOT THE SUDOKU NODE ANYMORE!')
            return
        
        logging.debug(f'{self.addr} -> Distribution of the solutions for {self.sudoku}')

        while len(self.nodes_available) > 0 and len(self.work_units) > 0:
            logging.debug(f'{self.addr} -> Nodes available for work BEFORE: {self.nodes_available}')
            logging.debug(f'{self.addr} -> Average response times: {self.avg_response_times}')
            node_to_send_work = self.select_peer()
            work = next(iter(self.work_units.items()))
            range_of_solutions = work[1]
            logging.debug(f'{self.addr} -> Work to be sent: {work}')
            do_work_message = P2PProtocol.do_work(self.addr, self.sudoku, {k: self.generated_solutions[str(k)] for k in range(range_of_solutions[0], range_of_solutions[1]+1)})
            if node_to_send_work == self.addr:
                self.node.handle_p2p_solve_request(do_work_message)                         # If I'm the node that will do the work, I don't need to send the message, I just call the function
            else:
                P2PProtocol.send_message(self.peers_sock[node_to_send_work], do_work_message)
            self.nodes_available.remove(node_to_send_work)
            self.work_units.pop(work[0])                                                    # I can remove here, because I still store it in nodes_jobs, and if I need to change the node because of some reason, I take that and distribute to other
            self.nodes_jobs[node_to_send_work] = work
            logging.debug(f'{self.addr} -> Sent DO_WORK to {node_to_send_work} with work {work}')
            logging.debug(f'{self.addr} -> Nodes available for work: {self.nodes_available}')
            logging.debug(f'{self.addr} -> Nodes with jobs: {self.nodes_jobs}')
            

    def work_done(self, origin_addr, addr):
        origin_addr = tuple(origin_addr)
        if origin_addr == self.addr:
            self.handle_work_done(origin_addr)
            return
        work_done_message = P2PProtocol.work_done(origin_addr, addr)
        P2PProtocol.send_message(self.peers_sock[origin_addr], work_done_message)
        logging.debug(f'{self.addr} -> Sent WORK_DONE to {origin_addr}')
    
    
    def terminate_sudoku_node_state(self, solved_sudoku):
        logging.debug(f'{self.addr} -> Terminating sudoku node state...')
        solved_sudoku_message = P2PProtocol.solved_sudoku(self.addr, self.addr, self.sudoku, solved_sudoku)
        self.send_broadcast(solved_sudoku_message)
        logging.debug(f'{self.addr} -> PUT SOLVED_SUDOKU in HTTP queue: {solved_sudoku}')
        self.node.http_queue.put(solved_sudoku)
        self.sudoku_node = False
        self.sudoku = None
        self.generated_solutions = {}
        self.work_units = {}
        logging.debug(f'{self.addr} -> SELF.PEERS: {self.peers}')
        self.nodes_available = self.peers.copy() + [self.addr]
        logging.debug(f'{self.addr} -> NODES AVAILABLE: {self.nodes_available}')
        self.nodes_jobs = {node: None for node in self.nodes_available}
        logging.debug(f'{self.addr} -> NODES JOBS: {self.nodes_jobs}')


    def found_solution(self, origin_addr, sudoku, solved_sudoku_key):
        origin_addr_tuple = tuple(origin_addr)
        if origin_addr_tuple == self.addr:
            logging.debug(f'{self.addr} -> Solution found by sudoku_node')
            self.handle_solved_sudoku(origin_addr, sudoku, solved_sudoku_key)
            return
        solved_sudoku_message = P2PProtocol.solved_sudoku(origin_addr, self.addr, sudoku, solved_sudoku_key)
        P2PProtocol.send_message(self.peers_sock[origin_addr_tuple], solved_sudoku_message)
        logging.debug(f'{self.addr} -> Sent SOLVED_SUDOKU to {origin_addr}')


    def send_broadcast(self, message):
        logging.debug(f'{self.addr} -> Broadcasting {message.command} to all peers: {self.peers}')
        for peer in self.peers:
            P2PProtocol.send_message(self.peers_sock[peer], message)
            logging.debug(f'{self.addr} -> Sent {message.command} to {peer} {message}')



    ### Helper Functions ###
    
    def update_stats(self, other_node_addr, other_node_validations):
        formatted_addr = f'{other_node_addr[0]}:{other_node_addr[1]}'
        
        # Find the node in the stats_info list
        node = next((node for node in self.stats_info["nodes"] if node["address"] == formatted_addr), None)
        
        # If the node was found
        if node is not None:
            # Get the current number of validations for the other node
            current_validations = node["validations"]

            # Check if the number of validations has changed
            if other_node_validations != current_validations:
                # Calculate the difference
                diff = other_node_validations - current_validations

                # Update the number of validations
                node["validations"] = other_node_validations

                # Update the total number of validations
                self.stats_info["all"]["validations"] += diff

                logging.debug(f'{self.addr} -> Updated validations for {other_node_addr} in stats_info list')


    def connect_to_peer(self, peer_addr):
        try:
            peer_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_conn.connect(peer_addr)
            peer_conn.setblocking(False)
            self.selector.register(peer_conn, selectors.EVENT_READ, self.read_new_message)
            join_req_message = P2PProtocol.join_req(self.addr)
            P2PProtocol.send_message(peer_conn, join_req_message)
            logging.debug(f'{self.addr} -> Connected and sent JOIN_REQ to {peer_addr}')
        except Exception as e:
            logging.error(f'Failed to connect to peer {peer_addr}: {e}')
            logging.error(traceback.format_exc())


    def get_addr_from_sock(self, sock):
        for addr, s in self.peers_sock.items():
            if s == sock:
                return addr


    def add_peer(self, peer_addr, peer_conn):
        self.peers.append(peer_addr)
        self.peers_sock[peer_addr] = peer_conn              # Every time I do self.peers.append or self.peers.remove, I need to update my registry of known nodes in self.peers_sock
        self.network_info[peer_addr] = {}
        self.network_info[self.addr] = self.peers           # Every time I do self.peers.append or self.peers.remove, I need to update my registry of known nodes in self.network_info

        self.response_times[peer_addr] = 0
        self.past_response_times[peer_addr] = []
        self.avg_response_times[peer_addr] = 0

        # self.nodes_available.append(peer_addr)
        self.nodes_jobs[peer_addr] = None
        self.last_stats[peer_addr] = time.monotonic()
        if not any(node["address"] == f'{peer_addr[0]}:{peer_addr[1]}' for node in self.stats_info["nodes"]):
            self.stats_info["nodes"].append({"address": f'{peer_addr[0]}:{peer_addr[1]}', "validations": 0})

        if self.sudoku_node:                                # If I'm the sudoku node, I need to distribute the solutions to the new node
            self.distribute_solutions()

    
    def remove_peer(self, peer_addr, peer_conn):
        self.peers.remove(peer_addr)
        self.peers_sock.pop(peer_addr)
        self.network_info.pop(peer_addr)
        self.network_info[self.addr] = self.peers
        
        self.response_times.pop(peer_addr)
        self.past_response_times.pop(peer_addr)
        self.avg_response_times.pop(peer_addr)
        
        if peer_addr in self.nodes_available:       # Because I might be the sudoku node and the node that is leaving was doing work for me, so it won't be in the nodes_available list
            self.nodes_available.remove(peer_addr)
        else:   # I'm the sudoku node 
            work = self.nodes_jobs[peer_addr]       # Fetch de work that the leaving node was doing
            if work is not None:
                self.work_units[work[0]] = work[1]

        self.nodes_jobs.pop(peer_addr)
        self.last_stats.pop(peer_addr)
        self.stats_info["nodes"] = [node for node in self.stats_info["nodes"] if node["address"] != f'{peer_addr[0]}:{peer_addr[1]}']
        
        logging.debug(f'{self.addr} -> Removed {peer_addr} from last_stats list')
        logging.debug(f'{self.addr} -> Removed {peer_addr} from peers list')
        
        self.selector.unregister(peer_conn) 
        peer_conn.close()

        logging.debug(f'{self.addr} -> My updated peers list: {self.peers}')
        logging.debug(f'{self.addr} -> My updated peers_sock list: {self.peers_sock}')

        if self.sudoku_node:                                # If I'm the sudoku node, I need to redistribute the solutions to the new node
            self.distribute_solutions()


    ### Load Balancer Functions ###

    def select_peer(self):
        # find the peer with the least response time
        peer = min(self.avg_response_times.values())
        
        for i in range(len(self.nodes_available)):  # iterate over the list of available nodes for work
            # increment the current peer index if it reaches the end of the list, reset it to -1
            if self.current + 1 >= len(self.nodes_available):
                self.current = -1
            self.current += 1
            # if the peer with the least response time is found break the loop
            if self.avg_response_times[self.nodes_available[self.current]] == peer:
                break
        # start time of the current peer
        self.response_times[self.nodes_available[self.current]] = time.time()
        logging.debug(f'{self.addr} -> Selected peer by LRT Load Balancer: {self.nodes_available[self.current]}')
        return self.nodes_available[self.current]
        

    def update(self, peer):
        # update the response time of the peer by calculating the difference between the current time and the start time
        response_times = time.time() - self.response_times[peer]
        self.response_times[peer] = response_times
        # add the response time to the list of past response times
        self.past_response_times[peer].append(response_times)
        # calculate the average response time of the peer
        self.avg_response_times[peer] = sum(self.past_response_times[peer]) / len(self.past_response_times[peer])
        logging.debug(f'{self.addr} -> Updated response time of {peer} to {self.avg_response_times[peer]}')



    ### Main Loop & Close ###

    def loop(self):
        time_start = time.monotonic()
        while not self.node.shutdown_flag.is_set():
            events = self.selector.select(timeout=1)
            for (key, mask) in events:
                callback = key.data
                callback(key.fileobj, mask)
            if time.monotonic() - time_start > 10:
                self.send_stats(1)
                time_start = time.monotonic()
            
            for peer, last_time in  list(self.last_stats.items()):
                # If a peer has not sent a stats message in the last 30 seconds
                if time.monotonic() - last_time > 25:
                    if peer in self.peers:
                        self.handle_node_disconnect(peer)
                        logging.debug(f'{self.addr} -> Peer {peer} has not sent a stats message in the last 20 seconds. Disconnecting...')


    def close(self):
        keys = list(self.selector.get_map().values())
        for key in keys:
            sock = key.fileobj
            try:
                self.selector.unregister(sock)
                sock.close()
            except Exception as e:
                logging.error(f'Error closing socket: {e}')
        self.selector.close()