import re

MAX_LINES = 10

# for parsing out ansi escape codes during register for pretty printing so
# that e.g. carriage returns (\r) don't break the printer
ANSI_REGEX = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

# the registry response we translate into a friendlier message
EXPIRED_TOKEN_ERROR = "denied: Your authorization token has expired."
