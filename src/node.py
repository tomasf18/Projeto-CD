import time
import signal
import sys
import selectors
import threading
from queue import Queue
from multiprocessing import Queue as MPQueue
import logging
import argparse
from P2Pnetwork import P2PNetwork
from http_server import NodeHTTPServer
from generateSolutions import SudokuGenerator
from sudoku import Sudoku
import P2Pprotocol
import json

class Node:

    ### Node start ###

    def __init__(self):
        """Initializes the node."""

        # ----- Node configurations ----- #
        self.first_peer = False
        self.http_server = None
        self.http_port = None
        self.p2p_network = None
        self.p2p_port = None
        self.initial_peer_addr = None
        self.handicap = None
        self.selector = selectors.DefaultSelector()
        self.solver = Sudoku([], 0.01)

        
        # ----- Node Threads ----- #
        self.http_thread = threading.Thread(target=self.start_http_server)
        self.p2p_thread = threading.Thread(target=self.start_p2p_network)
        self.worker_thread = threading.Thread(target=self.check_work)
        
        # ----- Node state ----- #
        self.solved = 0
        self.validations = 0

        # ----- Node thread queues ----- #
        self.http_queue = Queue()
        self.p2p_queue = MPQueue()        
        self.worker_queue = Queue()           

        # ----- Node Shutdown ----- #
        self.shutdown_flag = threading.Event()  # Flag to signal the node to shutdown


    def parse_arguments(self):
        """Parses the arguments passed to the node."""
        parser = argparse.ArgumentParser(description='Node', add_help=False)
        parser.add_argument('-p', '--http-port', type=int, help='HTTP port of the node')
        parser.add_argument('-s', '--p2p-port', type=int, help='P2P port of the node')
        parser.add_argument('-a', '--initial-peer', help='Address and port of the initial peer of the node (format: address:port)')
        parser.add_argument('-h', '--handicap', type=float, help='Handicap in milliseconds for the Sudoku validation function check()')
        parser.add_argument('-H', '--help', action='help', help='Show this help message and exit') # Add help argument manually
        args = parser.parse_args()
        
        # Set node configurations, obtaining it from the arguments
        if args.http_port:
            self.http_port = args.http_port
        if args.p2p_port:
            self.p2p_port = args.p2p_port
        if args.initial_peer:
            self.initial_peer_addr = (str(args.initial_peer.split(':')[0]), int(args.initial_peer.split(':')[1]))
        else:
            self.first_peer = True
        if args.handicap:
            self.handicap = args.handicap
            self.solver = Sudoku([], self.handicap)


    def start(self):
        """Starts the node."""
        # Obtain node configurations from the arguments
        self.parse_arguments()
        print('---- Node configurations ----')
        print(f'Node http_port: {self.http_port}')
        print(f'Node p2p_port: {self.p2p_port}')
        print(f'Adress and port of node to connect: {self.initial_peer_addr}')
        print(f'Node handicap: {self.handicap}\n')
        print('-----------------------------')

        # Start http server, p2p server and worker in separate threads
        self.http_thread.start()
        self.p2p_thread.start()
    


    ### HTTP Server Thread -> start ###

    def start_http_server(self):
        """Starts the HTTP server."""
        print('Starting HTTP server...')
        self.http_server = NodeHTTPServer(self)
        logging.debug(f'{self.http_server.port} -> Starting HTTP server on port {self.http_port}')
        try:
            self.http_server.start()
        except OSError as e:
            if e.errno == 98:
                logging.error(f"Port {self.http_port} is already in use. Please choose a different port.")
            else:
                logging.error(f"Failed to start HTTP server on port {self.http_port}: {e}")
              


    ### HTTP Server Thread -> /network Endpoint Handler ###


    def handle_network_request(self):
        """Handles a network request."""
        network = self.p2p_network.handle_network_request()
        return network



    ### HTTP Server Thread -> /stats Endpoint Handler ###
    
    def handle_stats_request(self):
        """Handles a stats request."""
        stats = self.p2p_network.handle_stats_request()
        return stats



    ### HTTP Server Thread -> /solve Endpoint Handler ###
    

    def handle_http_solve_request(self, sudoku):
        """Handles a solve request."""  
        if str(sudoku) in self.p2p_network.solved_sudokus:
            logging.debug(f'{self.p2p_network.addr} -> HTTP THREAD: Sudoku {sudoku} already solved')
            return self.p2p_network.solved_sudokus[str(sudoku)]
        logging.debug(f'{self.p2p_network.addr} -> HTTP THREAD: Received solve request: {sudoku}')
        self.generate_solutions(sudoku)
        logging.debug(f'{self.p2p_network.addr} -> HTTP THREAD: Start waiting for the solution of {sudoku}')
        solved_sudoku = self.http_queue.get(block=True)
        return solved_sudoku
    
    
    def generate_solutions(self, sudoku):
        """Generates all the sudoku solutions to be checked."""
        logging.debug(f'{self.p2p_network.addr} -> HTTP THREAD: Generating solutions for {sudoku}')
        current_sudoku_solutions = SudokuGenerator(sudoku).generate_solutions()        
        new_sudoku_message = P2Pprotocol.NewSudokuSolveMessage(sudoku, current_sudoku_solutions)
        self.p2p_queue.put(new_sudoku_message)
        logging.debug(f"{self.p2p_network.addr} -> HTTP THREAD: Delivered NEW_SUDOKU message in p2p thread's queue")
        return current_sudoku_solutions



    ### P2P Network Thread -> start ###

    def start_p2p_network(self):
        """Starts the P2P network."""
        print('Starting P2P server...')
        self.p2p_network = P2PNetwork(self)
        self.worker_thread.start()
        logging.debug(f'{self.p2p_network.addr} -> Starting P2P server on port {self.p2p_port}')
        self.selector.register(self.p2p_queue._reader.fileno(), selectors.EVENT_READ, self.handle_p2p_queue_put)
        self.p2p_network.start()
        self.p2p_network.loop()
    
    
    def handle_p2p_queue_put(self, fileobj, mask):
        """Handles a p2p queue put event."""
        while not self.p2p_queue.empty():
            message_json = self.p2p_queue.get()
            message = json.loads(str(message_json))
            command = message["command"]
            args = message["args"]

            logging.debug(f'{self.p2p_network.addr} -> P2P THREAD: Fetch the new message in p2p thread queue: {message}')

            if command == "NEW_SUDOKU":
                sudoku = args["sudoku"]
                generated_solutions = args["generated_solutions"]
                self.p2p_network.calculate_work_units(1, sudoku, generated_solutions)
            
            elif command == "WORK_DONE":
                origin_addr = args["origin_addr"]
                addr = args["addr"]
                self.p2p_network.work_done(origin_addr, addr)
                self.p2p_network.send_stats(1)
           
            elif command == "SOLVED_SUDOKU":
                origin_addr = args["origin_addr"]
                sudoku = args["sudoku"]
                solved_sudoku_key = args["solved_sudoku"]
                self.p2p_network.send_stats(1)
                self.p2p_network.found_solution(origin_addr, sudoku, solved_sudoku_key)
            

    def handle_p2p_solve_request(self, do_work_message):
        """Handles a work request, delegating it to work thread."""
        self.worker_queue.put(do_work_message) 
        logging.debug(f'{self.p2p_network.addr} -> Received work request and enqueued it in worker thread queue')
    


    ### Worker Thread ###

    def check_work(self):
        """Continuously checks for new work to do without recursion."""
        while True:
            logging.debug(f'{self.p2p_network.addr} -> WORKER THREAD: Checking for new work...')    
            message_json = self.worker_queue.get(block=True)  # Worker is waiting for work
            message = json.loads(str(message_json))
            logging.debug(f'{self.p2p_network.addr} -> WORKER THREAD: Fetched a new message from the worker queue: {message}')
            
            args = message["args"]
            origin_addr = args["origin_addr"]
            sudoku = args["sudoku"]
            solutions_to_check = args["solutions_to_check"]
            
            self.solve(origin_addr, sudoku, solutions_to_check)


    def solve(self, origin_addr, sudoku, solutions): 
        """Simulates work without using recursion."""
        if str(sudoku) in self.p2p_network.solved_sudokus:
            logging.debug(f'{self.p2p_network.addr} -> WORKER THREAD: Sudoku {sudoku} already solved')
            return
        
        logging.debug(f'{self.p2p_network.addr} -> WORKER THREAD: Simulating checks') 
        for solution in solutions.items(): 
            self.solver.grid = solution[1]
            is_sudoku_valid = self.solver.check()
            
            self.validations += 1
            if is_sudoku_valid:
                solved_sudoku_message = P2Pprotocol.SolvedSudokuMessage(origin_addr, self.p2p_network.addr, sudoku, solution[0])
                self.p2p_queue.put(solved_sudoku_message)
                logging.debug(f'{self.p2p_network.addr} -> WORKER THREAD: Found solution {solution} for {sudoku}')
                return  # Exit the method after finding a valid solution
        
        if str(sudoku) not in self.p2p_network.solved_sudokus:
            work_done_message = P2Pprotocol.WorkDoneMessage(origin_addr, self.p2p_network.addr)
            self.p2p_queue.put(work_done_message)
            logging.debug(f'{self.p2p_network.addr} -> WORKER THREAD: Finished working on solutions for {sudoku}, and sent WORK_DONE message to p2p thread')
        else:
            logging.debug(f'{self.p2p_network.addr} -> WORKER THREAD: Sudoku {sudoku} already solved, no need to send WORK_DONE message')


if __name__ == '__main__':
    
    # Create a node
    node = Node()
    node.start()



    