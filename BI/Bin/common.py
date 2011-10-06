
def load_file(filename):
    f = open(filename, "r")
    data = f.read()
    f.close()
    return data

def load_lines(filename, sep=","):
    return [line.split(sep) for line in\
            load_file(filename).splitlines()[1:]]

def groupby(d,cols,callback=None):
    if hasattr(d, "keys"):
        return _group_lines_in_dict_by(d,cols,callback)
    return _group_lines_by(d,cols,callback)

def _group_lines_in_dict_by(d, cols, callback=None):
    result = {}
    for key in d:
        result.setdefault(key, {})
        value = d[key]
        by_cols = groupby(value, cols)
        if callback:
            by_cols = callback(by_cols)
        result[key] = by_cols
    return result

def _group_lines_by(lines, cols, callback=None):
    """
    input:

        lines - [line1, ..., lineN]
        cols  - [int1, ..., intN]

    returns a dict with

        col             -> [line1, ..., lineN] if len(cols) == 1
        (col1,...,colN) -> [line1, ..., lineN] otherwise
    """
    def _key(line):
        if len(cols) == 1:
            return line[cols[0]]
        return tuple([line[x] for x in cols])
    result = {}
    for line in lines:
        key = _key(line)
        result.setdefault(key, [])
        result[key].append(line)
    return result

def is_valid_matrikel_nummer(n):
    return len(n) == 8 and n.startswith("a")

def orderby(lines, col):
    """
    order the lines by the value indexed by col
    """
    ordered = [(line[col],line) for line in lines]
    ordered.sort()
    return [x[1] for x in ordered]

def dropuntil(lines, filter_):
    """
    drop lines until the filter returns True
    """
    for i,line in enumerate(lines):
        if filter_(line):
            return lines[i:]
    return []

def collectuntil(lines, filter_):
    """
    collect lines until the filter returns True
    """
    for i,line in enumerate(lines):
        if filter_(line):
            return lines[:i]
    return lines

