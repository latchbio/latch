import re

MAX_LINES = 10

# for parsing out ansi escape codes
ANSI_REGEX = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
