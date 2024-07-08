"""Protocol for P2P network - Distributed Computing Final Project"""
import json
import socket
from datetime import datetime


''' --- Definition of what kind of messages can be used by nodes --- '''
class Message:
    """Message Type."""
    def __init__(self, command):
        self.command = command


class StatsMessage(Message):
    """Message to send the stats of the node."""

    def __init__(self, addr, validations, confirm_work):
        super().__init__("STATS")
        self.nodeAddr = addr
        self.validations = validations
        self.confirm_work = confirm_work

    def __repr__(self) -> str:
        return json.dumps({
            "command": "STATS",
            "args": {
                "addr": self.nodeAddr,
                "validations": self.validations,
                "confirm_work": self.confirm_work
            }
        })
    

class GetStatsMessage(Message):
    """Message to get the stats of the network."""

    def __init__(self, addr):
        super().__init__("GET_STATS")
        self.nodeAddr = addr

    def __repr__(self) -> str:
        return json.dumps({
            "command": "GET_STATS",
            "args": self.nodeAddr
        })


class StatsRepMessage(Message):
    """Message to reply with the stats of the network."""

    def __init__(self, addr, stats):
        super().__init__("STATS_REP")
        self.nodeAddr = addr
        self.stats_info = stats

    def __repr__(self) -> str:
        return json.dumps({
            "command": "STATS_REP",
            "args": {
                "addr": self.nodeAddr,
                "stats": self.stats_info
            }
        })
        

class GetListMessage(Message):
    """Message to get the list of known nodes."""

    def __init__(self, addr):
        super().__init__("GET_LIST")
        self.nodeAddr = addr

    def __repr__(self) -> str:
        return json.dumps({
            "command": "GET_LIST",
            "args": self.nodeAddr
        })


class ListRepMessage(Message):
    """Message to reply with the list of known nodes."""

    def __init__(self, addr, node_list):
        super().__init__("LIST_REP")
        self.nodeAddr = addr
        self.nodeList = node_list

    def __repr__(self) -> str:
        return json.dumps({
            "command": "LIST_REP",
            "args": {
                "addr": self.nodeAddr,
                "nodes": self.nodeList
            }
        })


class JoinMessage(Message):
    """Message to join the network."""

    def __init__(self, addr):
        super().__init__("JOIN_REQ")
        self.nodeAddr = addr

    def __repr__(self) -> str:
        return json.dumps({
            "command": "JOIN_REQ",
            "args": self.nodeAddr
        })


class JoinRepMessage(Message):
    """Message to reply to a join request."""

    def __init__(self, addr):
        super().__init__("JOIN_REP")
        self.nodeAddr = addr

    def __repr__(self) -> str:
        return json.dumps({
            "command": "JOIN_REP",
            "args": self.nodeAddr
        })
    

class TopologyChangeMessage(Message):
    """Message to reply to a join request."""

    def __init__(self, addr, updatedPeers):
        super().__init__("TOPOLOGY_CHANGE")
        self.nodeAddr = addr
        self.nodePeers = updatedPeers

    def __repr__(self) -> str:
        return json.dumps({
            "command": "TOPOLOGY_CHANGE",
            "args": {
                "addr": self.nodeAddr,
                "peers": self.nodePeers
            }
        })

class NewSudokuSolveMessage(Message):
    """Message to inform p2p thread that there is a new puzzle to solve."""

    def __init__(self, sudoku, generated_solutions):
        super().__init__("NEW_SUDOKU")
        self.sudoku = sudoku
        self.generated_solutions = generated_solutions

    def __repr__(self) -> str:
        return json.dumps({
            "command": "NEW_SUDOKU",
            "args": {
                "sudoku": self.sudoku,
                "generated_solutions": self.generated_solutions
            }
        })
    
    
class DoWorkMessage(Message):
    """Message to send for available peers and then, in each one, delegate the work from the p2p thread to work thread."""

    def __init__(self, origin_addr, sudoku, solutions):
        super().__init__("DO_WORK")
        self.origin_addr = origin_addr
        self.sudoku = sudoku
        self.solutions_to_check = solutions

    def __repr__(self) -> str:
        return json.dumps({
            "command": "DO_WORK",
            "args": {
                "origin_addr": self.origin_addr,
                "sudoku": self.sudoku,
                "solutions_to_check": self.solutions_to_check
            }
        })
    

class SolvedSudokuMessage(Message):
    """Message to inform that a puzzle was solved."""

    def __init__(self, origin_addr, addr, sudoku, solved_sudoku):
        super().__init__("SOLVED_SUDOKU")
        self.origin_addr = origin_addr
        self.nodeAddr = addr
        self.sudoku = sudoku
        self.solvedSudoku = solved_sudoku

    def __repr__(self) -> str:
        return json.dumps({
            "command": "SOLVED_SUDOKU",
            "args": {
                "origin_addr": self.origin_addr,        # Address of the node who received the /solve and sent the distribution of work
                "addr": self.nodeAddr,
                "sudoku": self.sudoku,
                "solved_sudoku": self.solvedSudoku
            }
        })
    

class WorkDoneMessage(Message):
    """Message to inform that a work was done."""

    def __init__(self, origin_addr, addr):
        super().__init__("WORK_DONE")
        self.origin_addr = origin_addr
        self.nodeAddr = addr

    def __repr__(self) -> str:
        return json.dumps({
            "command": "WORK_DONE",
            "args": {
                "origin_addr": self.origin_addr,
                "addr": self.nodeAddr
            }
        })



''' ------- Protocol Definition ------- '''
class P2PProtocol:
    
    # ------ Message creation ------ #

    @classmethod
    def stats(cls, addr, validations, confirm_work) -> StatsMessage:
        """Creates a StatsMessage object."""
        return StatsMessage(addr, validations, confirm_work)
    
    @classmethod
    def get_stats(cls, addr) -> GetStatsMessage:
        """Creates a GetStatsMessage object."""
        return GetStatsMessage(addr)
    
    @classmethod
    def stats_rep(cls, addr, stats) -> StatsRepMessage:
        """Creates a StatsRepMessage object."""
        return StatsRepMessage(addr, stats)

    @classmethod
    def get_list(cls, addr) -> GetListMessage:
        """Creates a GetListMessage object."""
        return GetListMessage(addr)

    @classmethod
    def list_rep(cls, addr, node_list) -> ListRepMessage:
        """Creates a ListRepMessage object."""
        return ListRepMessage(addr, node_list)

    @classmethod
    def join_req(cls, addr) -> JoinMessage:
        """Creates a JoinMessage object."""
        return JoinMessage(addr)

    @classmethod
    def join_rep(cls, addr) -> JoinRepMessage:
        """Creates a JoinRepMessage object."""
        return JoinRepMessage(addr)
    
    @classmethod
    def topology_change(cls, addr, updatedPeers) -> TopologyChangeMessage:
        """Creates a TopologyChangeMessage object."""
        return TopologyChangeMessage(addr, updatedPeers)
    
    @classmethod
    def new_sudoku(cls, sudoku, generated_solutions) -> NewSudokuSolveMessage:
        """Creates a NewSudokuSolveMessage object."""
        return NewSudokuSolveMessage(sudoku, generated_solutions)
    
    @classmethod
    def do_work(cls, origin_addr, sudoku, solutions) -> DoWorkMessage:
        """Creates a DoWorkMessage object."""
        return DoWorkMessage(origin_addr, sudoku, solutions)
    
    @classmethod
    def work_done(cls, origin_addr, addr) -> WorkDoneMessage:
        """Creates a WorkDoneMessage object."""
        return WorkDoneMessage(origin_addr, addr)


    @classmethod
    def solved_sudoku(cls, origin_addr, addr, sudoku, solved_sudoku) -> SolvedSudokuMessage:
        """Creates a SolvedSudokuMessage object."""
        return SolvedSudokuMessage(origin_addr, addr, sudoku, solved_sudoku)

    # ------ Message sending and receiving ------ #

    @classmethod
    def send_message(cls, connection: socket, msg: Message):
        """Sends through the connection a Message object."""
        message_to_send = str(msg)
        message_size = len(message_to_send).to_bytes(4, 'big')
        connection.send(message_size + message_to_send.encode('utf-8'))

    @classmethod
    def receive_message(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        message_size = int.from_bytes(connection.recv(4), 'big')

        if message_size == 0:
            return None

        message = connection.recv(message_size).decode('utf-8')
        
        try:
            message_to_json = json.loads(message)  # Try to convert to json
        except json.JSONDecodeError:
            raise P2PProtocolBadFormat(message)

        if "command" not in message_to_json.keys() or "args" not in message_to_json.keys():  # Certifies that 'command' and 'args' are in message
            raise P2PProtocolBadFormat(message_to_json)

        command = message_to_json["command"]
        args = message_to_json["args"]

        # ------ Handling message type ------
        if command == "STATS":
            return P2PProtocol.stats(args["addr"], args["validations"], args["confirm_work"])
        elif command == "GET_STATS":
            return P2PProtocol.get_stats(args)
        elif command == "STATS_REP":
            return P2PProtocol.stats_rep(args["addr"], args["stats"])
        elif command == "GET_LIST":
            return P2PProtocol.get_list(args)
        elif command == "LIST_REP":
            return P2PProtocol.list_rep(args["addr"], args["nodes"])
        elif command == "JOIN_REQ":
            return P2PProtocol.join_req(args)
        elif command == "JOIN_REP":
            return P2PProtocol.join_rep(args)
        elif command == "TOPOLOGY_CHANGE":
            return P2PProtocol.topology_change(args["addr"], args["peers"])
        elif command == "DO_WORK":
            return P2PProtocol.do_work(args["origin_addr"], args["sudoku"], args["solutions_to_check"])
        elif command == "WORK_DONE":
            return P2PProtocol.work_done(args["origin_addr"], args["addr"])
        elif command == "SOLVED_SUDOKU":
            return P2PProtocol.solved_sudoku(args["origin_addr"], args["addr"], args["sudoku"], args["solved_sudoku"])
        else:
            return None


''' ------- Protocol exceptions definition ------- '''
class P2PProtocolBadFormat(Exception):
    """Exception when source message is not P2PProtocol."""

    def __init__(self, original_msg: bytes = None):
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")
