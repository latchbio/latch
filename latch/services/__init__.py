"""Services or actions exposed through the SDK.

Services take one or more domain objects and/or values and perform some
transformation. These are the actions a user will interact with through the
CLI, eg. to "register" a workflow given login context.
"""

from latch.services.cp import cp
from latch.services.init import init
from latch.services.login import login
from latch.services.register import register
