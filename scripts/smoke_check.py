"""Minimal startup smoke check for Docker health and CI."""

import sys
import time
import http.client


def check_health(port=3000, path="/api/health", timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = http.client.HTTPConnection("localhost", port, timeout=2)
            conn.request("GET", path)
            resp = conn.getresponse()
            if resp.status == 200:
                print("Healthcheck passed.")
                return 0
            else:
                print(f"Healthcheck failed: {resp.status}")
        except Exception as exc:
            print(f"Waiting for app: {exc}")
        time.sleep(1)
    print("Healthcheck timed out.")
    return 1


if __name__ == "__main__":
    sys.exit(check_health())
