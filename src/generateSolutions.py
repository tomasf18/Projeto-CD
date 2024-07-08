
class SudokuGenerator:
    def __init__(self, sudoku):
        self.sudoku = sudoku

    def find_empty(self):
        zeros_cords = []
        for col in range(9):
            for row in range(9):
                if self.sudoku[col][row] == 0:
                    zeros_cords.append((col, row))
        return zeros_cords
    
    def add_number_to_empty(self, list_sudoku, cordinates):
        result = []
        for sudoku in list_sudoku:
            for number in range(1, 10):
                sudoku_solution = [list(line) for line in sudoku]
                sudoku_solution[cordinates[0]][cordinates[1]] = number
                result.append(sudoku_solution)
        return result
    
    def solutions_to_dict(self, solutions):
        result = {}
        for i, solution in enumerate(solutions):
            result[i] = solution
        return result
    
    def generate_solutions(self):
        result = [self.sudoku]
        for cordinates in self.find_empty():
            result = self.add_number_to_empty(result, cordinates)
        result = self.solutions_to_dict(result)
        return result
    