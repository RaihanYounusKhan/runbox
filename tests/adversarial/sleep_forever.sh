# expect: timed_out
# limits: {"timeout_ms": 1000}
#
# A process that just sleeps (no CPU use) is still stopped by the wall-clock limit.
sleep 1000
