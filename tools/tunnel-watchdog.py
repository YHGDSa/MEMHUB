#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Local-first memory stack watchdog.

Keeps the two local endpoints alive, and ONLY those two:
  * the hub port (default 127.0.0.1:3111, `node src/hub.mjs`)
  * the tunnel port (default 127.0.0.1:3112, reconnecting SSH tunnel upstream)

It never touches the upstream, other services, or any data. If a port is
listening, it does nothing. If a port is down, it (re)starts exactly that
component using a detached, proxy-free launch. Ports and commands come from
environment variables so nothing machine-specific is baked in.

Designed to be launched silently at logon. A single-instance lock prevents
duplicates.
"""
import os
import shutil
import socket
import subprocess
import sys
import time
import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # repo root
HUB_MJS = os.path.join(ROOT, "src", "hub.mjs")
HUB_CONFIG = os.environ.get("HUB_CONFIG", os.path.join(ROOT, "config.json"))
LOG = os.path.join(ROOT, "watchdog.log")
PIDFILE = os.path.join(ROOT, "watchdog.pid")
TUNNEL_PIDFILE = os.path.join(ROOT, "tunnel-wrapper.pid")
CHECK_INTERVAL = int(os.environ.get("WATCHDOG_INTERVAL", "30"))  # seconds
HUB_PORT = int(os.environ.get("HUB_PORT", "3111"))
TUNNEL_PORT = int(os.environ.get("TUNNEL_PORT", "3112"))

NODE = os.environ.get("NODE_EXE", "node")
TUNNEL_CMD = os.environ.get("TUNNEL_CMD", os.path.join(ROOT, "tunnel.cmd"))

# Node 22's fetch honours HTTP_PROXY even for 127.0.0.1 and ignores NO_PROXY.
# The hub must talk to the local tunnel directly, so strip all proxy vars.
PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
              "ALL_PROXY", "all_proxy")
DETACHED = 0x00000008 | 0x00000200  # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
CREATE_NO_WINDOW = 0x08000000  # console processes run with no visible window


def pid_alive(pid):
    """Check a Windows PID without extra dependencies."""
    import ctypes
    h = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)  # QUERY_LIMITED_INFORMATION
    if not h:
        return False
    ctypes.windll.kernel32.CloseHandle(h)
    return True


def read_tunnel_pid():
    try:
        with open(TUNNEL_PIDFILE, "r", encoding="ascii") as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def clear_tunnel_pid():
    try:
        os.remove(TUNNEL_PIDFILE)
    except OSError:
        pass


def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def port_open(port, host="127.0.0.1", timeout=1.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def clean_env():
    env = os.environ.copy()
    for k in PROXY_VARS:
        env.pop(k, None)
    node_dir = os.path.dirname(shutil.which(NODE) or NODE)
    if node_dir:
        env["PATH"] = node_dir + os.pathsep + env.get("PATH", "")
    return env


def wait_port(port, seconds=20):
    """Wait until a port is listening, so we never double-start a component."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        if port_open(port):
            return True
        time.sleep(0.5)
    return False


def start_hub():
    log(f"{HUB_PORT} down -> starting hub")
    try:
        logf = open(os.path.join(ROOT, "hub.log"), "a", encoding="utf-8")
        p = subprocess.Popen([NODE, HUB_MJS, HUB_CONFIG], cwd=ROOT, creationflags=DETACHED,
                             stdin=subprocess.DEVNULL, stdout=logf,
                             stderr=subprocess.STDOUT, close_fds=True,
                             env=clean_env())
        ok = wait_port(HUB_PORT, 20)
        log(f"hub started pid={p.pid} listening={ok}")
    except Exception as e:
        log(f"hub start FAILED: {e}")


def start_tunnel():
    # The wrapper may still be alive and reconnecting even while the port is
    # down. Starting another one just makes several ssh processes fight over
    # 3112 and pop up timeout windows. Never spawn a second live wrapper.
    old = read_tunnel_pid()
    if old and pid_alive(old):
        log(f"tunnel wrapper pid={old} still alive -> skip duplicate")
        return
    log(f"{TUNNEL_PORT} down -> starting tunnel")
    try:
        p = subprocess.Popen(["cmd.exe", "/c", TUNNEL_CMD],
                             creationflags=CREATE_NO_WINDOW,
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, close_fds=True,
                             env=os.environ.copy())
        with open(TUNNEL_PIDFILE, "w", encoding="ascii") as f:
            f.write(str(p.pid))
        ok = wait_port(TUNNEL_PORT, 30)
        log(f"tunnel wrapper started pid={p.pid} listening={ok}")
        if not ok and not pid_alive(p.pid):
            clear_tunnel_pid()
    except Exception as e:
        log(f"tunnel start FAILED: {e}")


def main():
    # Single-instance guard: autostart plus a manual launch may both trigger
    # the watchdog; only one instance should ever run.
    try:
        with open(PIDFILE, "r", encoding="ascii") as f:
            old = int(f.read().strip())
    except (OSError, ValueError):
        old = None
    if old and pid_alive(old):
        log(f"another watchdog pid={old} is alive -> exit")
        return
    with open(PIDFILE, "w", encoding="ascii") as f:
        f.write(str(os.getpid()))
    log(f"watchdog starting pid={os.getpid()}")
    # Grace period: when the PC boots (or the watchdog is restarted), an
    # existing hub/tunnel may still be binding its port. Wait before the first
    # probe so we don't spawn a duplicate that fights for the same port.
    time.sleep(12)
    while True:
        try:
            if not port_open(HUB_PORT):
                start_hub()
            if not port_open(TUNNEL_PORT):
                start_tunnel()
        except Exception as e:
            log(f"check loop error: {e}")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
