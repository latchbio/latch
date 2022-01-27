latch
-----

## Quickstart

```
git clone git@github.com:latchbio/latch.git
cd latch; pip install -e .
```

```
# Create a register a test workflow
latch init test
cd test
latch register .

# Copy local data to the platform.
# The second path is a remote path on latch.
# Functions similarly to UNIX cp.
latch cp latch/version /new_version
```
