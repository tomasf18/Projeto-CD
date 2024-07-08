import random
import sys
from sudoku import Sudoku


def solve_sudoku(board):
    """Solve the Sudoku puzzle using backtracking - this is NOT a distributed solution."""

    row, col = None, None

    for i in range(9):
        for j in range(9):
            if board[i][j] == 0:
                row, col = (i, j)
                break

    if row is None or col is None:
        return True  # No empty spaces left, puzzle is solved

    for num in range(1, 10):
        sudoku = Sudoku(board, base_delay=0.01, interval=20, threshold=10)
        if sudoku.check_is_valid(row, col, num):
            board[row][col] = num
            if solve_sudoku(board):
                return True
            board[row][col] = 0  # Reset if not a solution

    return False


def generate_sudoku(empty_boxes=0):
    """Generate a Sudoku puzzle."""
    board = [[0] * 9 for _ in range(9)]

    # Fill the diagonal 3x3 squares randomly (these don't interfere with each other)
    for n in range(0, 9, 3):
        nums = random.sample(range(1, 10), 9)
        for i in range(3):
            for j in range(3):
                board[n + i][n + j] = nums.pop()

    # Solve the puzzle (fill the rest of the board)
    solve_sudoku(board)

    # Remove some numbers to create empty boxes
    for _ in range(empty_boxes):
        row, col = random.randint(0, 8), random.randint(0, 8)
        while board[row][col] == 0:
            row, col = random.randint(0, 8), random.randint(0, 8)
        board[row][col] = 0

    return Sudoku(board)


if __name__ == "__main__":
    # Generate and print a solved Sudoku puzzle
    empty_boxes = int(sys.argv[1])

    new_puzzle = generate_sudoku(empty_boxes)

    print(new_puzzle)

    print(
        "curl http://localhost:8001/solve -X POST -H 'Content-Type: application/json' -d '{\"sudoku\": %s}'"
        % (new_puzzle.grid)
    )
