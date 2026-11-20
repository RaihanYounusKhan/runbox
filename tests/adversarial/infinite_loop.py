# expect: timed_out
# limits: {"timeout_ms": 1500}
#
# A CPU spin. The CPU quota keeps it from starving other jobs; the wall-clock
# timeout kills it.
while True:
    pass
