from gen import generate_sudoku


if __name__ == "__main__":
    sudoku = generate_sudoku(3)
    print(sudoku.grid)