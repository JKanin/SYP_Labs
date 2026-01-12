import re
import sys

class CppSubsetTranslator:
    def __init__(self):
        self.cpp_code = ""
        self.py_code = []
        self.indent_level = 0
        self.variables = set()
        self.errors = []

    def load_cpp_code(self, cpp_code):
        self.cpp_code = cpp_code.strip()
        self.py_code = []
        self.indent_level = 0
        self.variables.clear()
        self.errors.clear()

    def translate(self):
        lines = self.cpp_code.split('\n')
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith('if'):
                i = self._translate_if(lines, i)
            elif line.startswith('switch'):
                i = self._translate_switch(lines, i)
            elif line.startswith('int') or line.startswith('bool'):
                self._translate_variable_declaration(line)
            elif line:
                self._translate_expression(line)
            i += 1
        if self.errors:
            raise ValueError("Ошибки в трансляции: " + "\n".join(self.errors))
        return '\n'.join(self.py_code)

    def _indent(self):
        return '    ' * self.indent_level

    def _translate_variable_declaration(self, line):
        match = re.match(r'(int|bool)\s+(\w+)\s*=\s*(.+);', line)
        if match:
            var_type, var_name, value = match.groups()
            self.variables.add(var_name)
            if var_type == 'bool':
                value = value.lower() in ('true', '1') and 'True' or 'False'
            self.py_code.append(self._indent() + f"{var_name} = {value}")
        else:
            self.errors.append(f"Неверное объявление: {line}")

    def _translate_expression(self, line):
        if 'cout' in line:
            match = re.search(r'cout\s*<<\s*"([^"]*)"\s*;', line)
            if match:
                self.py_code.append(self._indent() + f'print("{match.group(1)}")')
            else:
                self.errors.append(f"Неверный cout: {line}")
        elif '=' in line and line.endswith(';'):
            parts = line.split('=')
            var = parts[0].strip()
            if var in self.variables:
                value = '='.join(parts[1:]).strip(';').strip()
                self.py_code.append(self._indent() + f"{var} = {value}")
            else:
                self.errors.append(f"Необъявленная переменная: {var}")
        else:
            self.errors.append(f"Неизвестное выражение: {line}")

    def _translate_if(self, lines, start):
        i = start
        line = lines[i].strip()
        match = re.match(r'if\s*\((.+)\)\s*{?', line)
        if not match:
            self.errors.append(f"Неверный if: {line}")
            return i
        condition = match.group(1).strip()
        self.py_code.append(self._indent() + f"if {condition}:")
        self.indent_level += 1
        i += 1
        while i < len(lines) and not lines[i].strip().startswith('}'):
            self._process_body_line(lines[i])
            i += 1
        if i < len(lines) and lines[i].strip().startswith('}'):
            i += 1
        self.indent_level -= 1

        while i < len(lines):
            line = lines[i].strip()
            if line.startswith('else if'):
                match = re.match(r'else if\s*\((.+)\)\s*{?', line)
                if match:
                    condition = match.group(1).strip()
                    self.py_code.append(self._indent() + f"elif {condition}:")
                    self.indent_level += 1
                    i += 1
                    while i < len(lines) and not lines[i].strip().startswith('}'):
                        self._process_body_line(lines[i])
                        i += 1
                    if i < len(lines) and lines[i].strip().startswith('}'):
                        i += 1
                    self.indent_level -= 1
                else:
                    self.errors.append(f"Неверный else if: {line}")
            elif line.startswith('else'):
                self.py_code.append(self._indent() + "else:")
                self.indent_level += 1
                i += 1
                while i < len(lines) and not lines[i].strip().startswith('}'):
                    self._process_body_line(lines[i])
                    i += 1
                if i < len(lines) and lines[i].strip().startswith('}'):
                    i += 1
                self.indent_level -= 1
                break
            else:
                break
        return i - 1

    def _translate_switch(self, lines, start):
        i = start
        line = lines[i].strip()
        match = re.match(r'switch\s*\((.+)\)\s*{', line)
        if not match:
            self.errors.append(f"Неверный switch: {line}")
            return i
        var = match.group(1).strip()
        if var not in self.variables:
            self.errors.append(f"Необъявленная переменная в switch: {var}")
        self.py_code.append(self._indent() + f"# switch emulation for {var}")
        self.py_code.append(self._indent() + f"_switch_var = {var}")
        i += 1
        cases = []
        default = None
        while i < len(lines) and not lines[i].strip().startswith('}'):
            line = lines[i].strip()
            if line.startswith('case'):
                match = re.match(r'case\s*(.+):', line)
                if match:
                    value = match.group(1).strip()
                    case_body = []
                    i += 1
                    while i < len(lines) and not lines[i].strip().startswith('break;') and not lines[
                        i].strip().startswith('case') and not lines[i].strip().startswith('default') and not lines[
                        i].strip().startswith('}'):
                        case_body.append(lines[i])
                        i += 1
                    cases.append((value, case_body))
                    if i < len(lines) and lines[i].strip().startswith('break;'):
                        i += 1
            elif line.startswith('default:'):
                default_body = []
                i += 1
                while i < len(lines) and not lines[i].strip().startswith('}'):
                    default_body.append(lines[i])
                    i += 1
                default = default_body
            else:
                i += 1
        for value, body in cases:
            self.py_code.append(self._indent() + f"if _switch_var == {value}:")
            self.indent_level += 1
            for bline in body:
                self._process_body_line(bline)
            self.indent_level -= 1
        if default:
            self.py_code.append(self._indent() + "else:")
            self.indent_level += 1
            for bline in default:
                self._process_body_line(bline)
            self.indent_level -= 1
        return i

    def _process_body_line(self, line):
        line = line.strip()
        if line.startswith('int') or line.startswith('bool'):
            self._translate_variable_declaration(line)
        elif line:
            self._translate_expression(line)

if __name__ == "__main__":
    translator = CppSubsetTranslator()
    cpp_example = """
    int x = 5;
    if (x > 0) {
        cout << "positive";
    } 
    else if (x < 0) {
        cout << "negative";
    } 
    else {
        cout << "zero";
    }

    switch (x) {
        case 1: cout << "one"; break;
        case 5: cout << "five"; break;
        default: cout << "other";
    }
    """
    translator.load_cpp_code(cpp_example)
    try:
        py_code = translator.translate()
        print("Сгенерированный Python код:")
        print(py_code)
    except ValueError as e:
        print(e)