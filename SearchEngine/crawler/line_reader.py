import re

def read_content(content, n_lines = 40):
    lines = content.splitlines()

    for i in range(min(n_lines, len(lines))):
        line = lines[i]
        title_match = re.search(r"Title:\s*(.*)", line)
        if title_match:
            return title_match.group(1).strip()

    return None