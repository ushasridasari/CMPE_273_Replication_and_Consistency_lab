"""
client.py - talk to the replicas and run the 3 tests of the lab.

    python3 client.py read primary
    python3 client.py read secondary
    python3 client.py write 8
    python3 client.py pause | resume
    python3 client.py demo          <- run all the 3 tests one by one
"""

import json
import sys
import time
import urllib.request

PRIMARY = "http://localhost:5001"
SECONDARY = "http://localhost:5002"
URLS = {"primary": PRIMARY, "secondary": SECONDARY}


def call(url, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


def read(which):
    r = call(URLS[which] + "/stock")
    print(f"  CLIENT read from {which:9} -> stock = {r['stock']}  (version {r['version']})")
    return r["stock"]


def write(value):
    r = call(PRIMARY + "/stock", "POST", {"value": value})
    print(f"  CLIENT write to primary   -> stock = {r['stock']}  (version {r['version']})")


def pause():
    call(PRIMARY + "/replication/pause", "POST", {})
    print("  CLIENT asked primary to PAUSE replication")


def resume():
    call(PRIMARY + "/replication/resume", "POST", {})
    print("  CLIENT asked primary to RESUME replication")


def wait_converge(timeout=10):
    """Poll the secondary until it is same as primary."""
    start = time.time()
    while time.time() - start < timeout:
        p = call(PRIMARY + "/stock")
        s = call(SECONDARY + "/stock")
        if p["stock"] == s["stock"] and p["version"] == s["version"]:
            return time.time() - start
        time.sleep(0.1)
    return None


def header(title):
    print("\n" + "=" * 60)
    print(" " + title)
    print("=" * 60)


def check(ok, msg):
    print(f"  [{'PASS' if ok else 'FAIL'}] {msg}")
    return ok


def demo():
    results = []

    header("STEP 0: initial state")
    p0, s0 = read("primary"), read("secondary")
    results.append(check(p0 == s0 == 10, "both replicas start with stock = 10"))

    # ---------------------------------------------------------
    header("TEST 1: Replication (normal case)")
    write(12)
    p = read("primary")
    results.append(check(p == 12, "primary have the new value 12 right away"))
    t = wait_converge()
    print(f"  ...secondary caught up after {t:.2f}s" if t is not None
          else "  ...secondary did not catch up")
    p, s = read("primary"), read("secondary")
    results.append(check(p == s == 12, "both replicas return 12 after replication"))

    # put it back to 10 so the stale test start from 10 like the lab says
    write(10)
    wait_converge()
    print("  (reset stock back to 10 for next test)")
    read("primary"); read("secondary")

    # ---------------------------------------------------------
    header("TEST 2: Stale read (replication is delayed)")
    pause()
    write(9)
    time.sleep(1)   # give some time, still nothing reach secondary
    for i in range(3):
        print(f"  -- read round {i + 1} --")
        p, s = read("primary"), read("secondary")
        results.append(check(p == 9, "primary return 9"))
        results.append(check(s == 10, "secondary still return OLD value 10 -> STALE READ"))
        time.sleep(1)

    # ---------------------------------------------------------
    header("TEST 3: Recovery and convergence")
    resume()
    t = wait_converge()
    print(f"  ...secondary converged {t:.2f}s after resume" if t is not None
          else "  ...secondary did not converge")
    p, s = read("primary"), read("secondary")
    results.append(check(s == 9, "secondary is now updated to 9"))
    results.append(check(p == s, "primary and secondary return same value again"))

    header(f"SUMMARY: {sum(results)}/{len(results)} checks passed")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    cmd = sys.argv[1]
    if cmd == "read":
        read(sys.argv[2] if len(sys.argv) > 2 else "primary")
    elif cmd == "write":
        write(int(sys.argv[2]))
    elif cmd == "pause":
        pause()
    elif cmd == "resume":
        resume()
    elif cmd == "demo":
        demo()
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
