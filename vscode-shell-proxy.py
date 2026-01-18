#!/usr/bin/env python3
#
# This script acts as a proxy for the Microsoft Visual Studio Code application's
# Remote-SSH extension.  Remote-SSH must be setup to allow for remote command to
# be included in host configurations; the RemoteCommand is set to this script
# with various options permissible.
#
# Slurm is used to start an interactive shell on a compute node.  The stdio channels
# for that remote shell are proxied by this script to the stdio channels of the ssh
# session that executed this script.  In essence, i/o to/from the VSCode application
# flow through this script to the remote shell.
#
# Part of that proxy is watching the remote shell's stdout for notification of the
# TCP socket (port number) that the remote vscode software is using for control
# communications.  A TCP listener is opened by this script and its port substituted
# in that output.  The VSCode application receives the port number on the login node
# and communicates with that; this script accepts connections on that port and proxies
# them to the actual TCP listener on the compute node.
#

import asyncio
import time
import logging
import threading
import argparse
import subprocess
import json
import uuid
import shlex
import contextlib
import fcntl
import re
import sys
import os
import getpass
from enum import Enum

# This is the local TCP port on which this script is listening:
proxyPort = None

# This is the remote (compute node) hostname and TCP port on which vscode backend
# is listening:
targetHost = None
targetPort = None

# Regex for the line output by the vscode backend indicating the TCP port on which
# it is listening:
targetPortRegex = re.compile(
    r"^([^0-9]*listeningOn=[^0-9]*)(([0-9][0-9]*\.){3}[0-9][0-9]*:)?([0-9][0-9]*)([^0-9]*)$"
)

# Regex for input lines containing references to running servers bound to the
# localhost interface only:
localhostFixupNodeJSRegex = re.compile(
    r"((\$args =.*)|(\$VSCH_SERVER_SCRIPT.*))--host=127.0.0.1"
)
localhostFixupCLIListenArgsRegex = re.compile(
    r'(LISTEN_ARGS=".*)(--on-host=(([0-9][0-9]*\.){3}[0-9][0-9]*))'
)
localhostFixupCLICmdRegex = re.compile(
    r"(VSCODE_CLI_REQUIRE_TOKEN=[0-9a-fA-F-]*.*\$CLI_PATH.*command-shell )(.*)(--on-host=(([0-9][0-9]*\.){3}[0-9][0-9]*))"
)
sessionKeySanitizeRegex = re.compile(r"[^A-Za-z0-9_.-]+")

# Any commands this script itself sends to the remote shell should have their output
# prefixed with this text to indicate they are NOT in response to VSCode application
# commands:
ourShellOutputPrefix = "VSCODE_SHELL_PROXY::::"

# Default buffer size for binary TCP proxy i/o:
DEFAULT_BYTE_LIMIT = 4096

# Default connection acceptance backlog count:
DEFAULT_BACKLOG = 8

# Persistent session defaults:
DEFAULT_SESSION_STATE_DIR = "~/.slurm-connect"
DEFAULT_SESSION_IDLE_TIMEOUT = 0
DEFAULT_SESSION_HEARTBEAT_SECONDS = 30
DEFAULT_SESSION_STALE_SECONDS = 90
DEFAULT_SESSION_JOB_NAME = "slurm-connect"
DEFAULT_SESSION_WAIT_SECONDS = 300


# --- TaskGroup compatibility (Python < 3.11) -------------------------------
try:
    # Python 3.11+
    from asyncio import TaskGroup  # type: ignore
except ImportError:

    class TaskGroup:
        """
        Minimal asyncio.TaskGroup backport for Python 3.9/3.10.

        Supports:
          - `async with TaskGroup() as tg:`
          - `tg.create_task(coro)`
        Behavior:
          - waits for all tasks on exit
          - if any task raises, cancels the others and re-raises the first exception
        """

        def __init__(self):
            self._tasks = []

        async def __aenter__(self):
            return self

        def create_task(self, coro):
            t = asyncio.create_task(coro)
            self._tasks.append(t)
            return t

        async def __aexit__(self, exc_type, exc, tb):
            if not self._tasks:
                return False

            # If the `with` block raised, cancel everything.
            if exc is not None:
                for t in self._tasks:
                    t.cancel()

            results = await asyncio.gather(*self._tasks, return_exceptions=True)

            # If a task failed, cancel the rest (best-effort) and raise the first exception.
            first_exc = None
            for r in results:
                if isinstance(r, BaseException) and not isinstance(
                    r, asyncio.CancelledError
                ):
                    first_exc = r
                    break

            if first_exc is not None:
                for t in self._tasks:
                    if not t.done():
                        t.cancel()
                raise first_exc

            # Returning False means "don't suppress exceptions" from the with-block.
            return False
# --------------------------------------------------------------------------


# The script progresses through these states:
class ProxyStates(Enum):
    LAUNCH = 0
    BEGIN = 1
    START_PROXY = 2
    PROXY_STARTED = 3
    END = 4


# This condition will be used to synchronize the progression of the script through
# operational states:
proxyStateCond = threading.Condition()
proxyState = ProxyStates.LAUNCH


def checkProxyState(desiredState):
    global proxyState
    return proxyState == desiredState


async def tcp_proxy_xfer(R, W):
    """Receive data from a reader and send it on a writer, closing and exiting from this function on any errors, end-of-file, etc."""
    global cliArgs

    while not R.at_eof() and not W.is_closing():
        binaryData = await R.read(cliArgs.byteLimit)
        if binaryData:
            # Send the data on the writer:
            W.write(binaryData)
            await W.drain()
        else:
            # No data implies the reader has closed:
            W.close()
            break


async def tcp_proxy_connect(sR, sW):
    """Accept a connection opened on the proxy port.  Open a connection to the vscode backend then create two async transfer functions to forward data between them."""
    sWAddr = sW.get_extra_info("peername")
    logging.info("{:s}:{:d} connection accepted".format(*sWAddr))
    try:
        rR, rW = await asyncio.open_connection(targetHost, targetPort)
        async with TaskGroup() as tg:
            t1 = tg.create_task(tcp_proxy_xfer(sR, rW))
            t2 = tg.create_task(tcp_proxy_xfer(rR, sW))
            logging.debug("{:s}:{:d} i/o tasks scheduled".format(*sWAddr))
        logging.debug("{:s}:{:d} i/o tasks completed".format(*sWAddr))
    except Exception as E:
        logging.error("{:s}:{:d} failure: {:s}".format(sWAddr[0], sWAddr[1], str(E)))
        sW.close()


async def tcp_proxy():
    global proxyPort, proxyState, proxyStateCond, cliArgs
    """The vscode backend TCP port proxy server."""
    with proxyStateCond:
        logging.debug("    Waiting on TCP proxy server start condition...")
        proxyStateCond.wait_for(lambda: checkProxyState(ProxyStates.START_PROXY))
        logging.debug("    Starting TCP proxy server on port %d", cliArgs.listenPort)
        server = await asyncio.start_server(
            tcp_proxy_connect,
            cliArgs.listenHost,
            cliArgs.listenPort,
            reuse_port=True,
            backlog=cliArgs.backlog,
        )
        # Get the port we're listening on:
        proxyPort = server.sockets[0].getsockname()[1]

        logging.info("    Running TCP proxy server on port %d...", proxyPort)
        proxyState = ProxyStates.PROXY_STARTED
        logging.debug("[STATE] PROXY_STARTED <- TCP proxy runloop")
        proxyStateCond.notify_all()
    async with server:
        await server.serve_forever()
    logging.debug("    Terminating TCP proxy server.")


def start_tcp_proxy(loop):
    """Alternative thread that will run the vscode backend TCP port proxy runloop."""
    asyncio.set_event_loop(loop)
    logging.debug("  Entering tcp proxy event loop")
    loop.run_until_complete(tcp_proxy())
    logging.debug("  Exited tcp proxy event loop")


def stdinProxyThread(drain, copyToFile=None):
    """Target function for a thread that will consume input from this script's stdin and write it to the remote shell's stdin.  Before any forwarding begins, introspective command(s) associated with this script are sent (and their output will be consumed by the stdout-forwarding thread).  When EOF is reached on this script's stdin the state is forwarded to END, yielding the shutdown of this script -- the connection from the VSCode application has been severed."""
    global \
        proxyStateCond, \
        proxyState, \
        localhostFixupNodeJSRegex, \
        localhostFixupCLICmdRegex
    global localhostFixupCLIListenArgsRegex

    hasCLIListenArgs = False

    # Start by sending our special `hostname` command:
    hostnameCmd = 'echo "{:s}HOSTNAME=$(hostname)"\n'.format(ourShellOutputPrefix)
    if copyToFile:
        copyToFile.write(hostnameCmd)
        copyToFile.flush()
    drain.write(hostnameCmd)
    drain.flush()
    logging.debug("Wrote startup command to remote shell: %s", hostnameCmd.strip())

    xdg_cmd = (
        "unset XDG_RUNTIME_DIR; "
        'export XDG_RUNTIME_DIR="/tmp/$USER/vscode-xdg-${SLURM_JOB_ID:-$$}"; '
        'mkdir -p "$XDG_RUNTIME_DIR" && chmod 700 "$XDG_RUNTIME_DIR"\n'
    )

    if copyToFile:
        copyToFile.write(xdg_cmd)
        copyToFile.flush()

    drain.write(xdg_cmd)
    drain.flush()

    while True:
        logging.debug("Waiting on stdin...")
        inputLine = sys.stdin.readline()
        if not inputLine:
            break

        # Localhost fixups?
        if "--host=127.0.0.1" in inputLine:
            # Confirm it's one of the lines we're expecting:
            localhostFixupMatch = localhostFixupNodeJSRegex.search(inputLine)
            if localhostFixupMatch is None:
                logging.warning(
                    "unanticipated localhost line found: %s", inputLine.strip()
                )
            else:
                logging.debug("localhost line found and fixed: %s", inputLine.strip())
                inputLine = inputLine.replace("127.0.0.1", "0.0.0.0")
        elif not hasCLIListenArgs and '"$CLI_PATH" command-shell' in inputLine:
            # Confirm it's one of the lines we're expecting:
            localhostFixupMatch = localhostFixupCLICmdRegex.search(inputLine)
            if localhostFixupMatch is None:
                logging.warning(
                    "unanticipated localhost line found: %s", inputLine.strip()
                )
            else:
                logging.debug("localhost line found and fixed: %s", inputLine.strip())
                inputLine = re.sub(
                    localhostFixupCLICmdRegex,
                    r"\g<1> --on-host=0.0.0.0 \g<2>",
                    inputLine,
                )
        elif "LISTEN_ARGS=" in inputLine:
            # Confirm it's the line we're expecting:
            localhostFixupMatch = localhostFixupCLIListenArgsRegex.search(inputLine)
            if localhostFixupMatch is None:
                logging.warning(
                    "unanticipated localhost line found: %s", inputLine.strip()
                )
            else:
                logging.debug("localhost line found and fixed: %s", inputLine.strip())
                inputLine = re.sub(
                    localhostFixupCLIListenArgsRegex,
                    r"\g<1> --on-host=0.0.0.0 ",
                    inputLine,
                )
                hasCLIListenArgs = True

        if copyToFile is not None:
            copyToFile.write(inputLine)
            copyToFile.flush()
        drain.write(inputLine)
        drain.flush()

    # All done, let everyone know:
    with proxyStateCond:
        proxyState = ProxyStates.END
        logging.debug("[STATE] END <- stdin thread")
        proxyStateCond.notify_all()


def stderrProxyThread(faucet, copyToFile=None):
    """Consume output to the remote shell's stderr and write it to this script's stderr."""
    if copyToFile is not None:
        while True:
            logging.debug("Waiting on remote stderr...")
            inputLine = faucet.readline()
            if not inputLine:
                break
            copyToFile.write(inputLine)
            copyToFile.flush()
            sys.stderr.write(inputLine)
            sys.stderr.flush()
    else:
        while True:
            inputLine = faucet.readline()
            if not inputLine:
                break
            sys.stderr.write(inputLine)
            sys.stderr.flush()


def stdoutProxyThread(faucet, copyToFile=None):
    """Consume output to the remote shell's stdout and write it to this script's stdout.  This function is far more complex compared to the stderrProxyThread() function:  the stdout lines must be scanned for output associated with commands issued by this script (e.g. to get the remote hostname) and the remote TCP port on which the vscode backend is listening.  Once those data are known, this script's TCP proxy can be started.  When EOF is reached on the remote shell's stdout the state is forwarded to END, yielding the shutdown of this script -- the connection to the remote shell has been severed."""
    global targetHost, targetPort, targetPortRegex, proxyStateCond, proxyState

    listenOnHadHost = False

    while True:
        logging.debug("Waiting on remote stdout...")
        outputLine = faucet.readline()
        if not outputLine:
            break

        omitLine = False

        # Is it one of our command(s)?
        if outputLine.startswith(ourShellOutputPrefix):
            # Drop the prefix:
            outputLine = outputLine[len(ourShellOutputPrefix) :]

            # Is it the hostname line?
            if outputLine.startswith("HOSTNAME="):
                targetHost = outputLine[len("HOSTNAME=") :].strip()
                logging.info("Remote hostname found:  %s", targetHost)

            # Never send these lines to the app:
            omitLine = True
        else:
            # Output coming back from the remote vscode stuff:
            if targetPort is None and "listeningOn=" in outputLine:
                logging.debug(
                    "Remote vscode TCP listener port found: %s", outputLine.strip()
                )
                targetPortMatch = targetPortRegex.search(outputLine)
                if targetPortMatch is not None:
                    targetPort = int(targetPortMatch.group(4))
                    logging.info("Remote TCP port found:  %d", targetPort)
                    if targetPortMatch.group(2) is not None:
                        listenOnHadHost = len(targetPortMatch.group(2)) > 0

                # Don't print the line now, stash it for output once the TCP proxy
                # has started:
                omitLine = True
                targetPortLine = outputLine

        if (
            proxyState is ProxyStates.BEGIN
            and targetHost is not None
            and targetPort is not None
        ):
            # Before going any further, start the proxy:
            with proxyStateCond:
                proxyState = ProxyStates.START_PROXY
                logging.debug("[STATE] START_PROXY <- stdout thread")
                proxyStateCond.notify_all()

            # Once it's started we can continue:
            with proxyStateCond:
                logging.debug(
                    "stdout thread waiting for TCP proxy startup completed..."
                )
                proxyStateCond.wait_for(
                    lambda: checkProxyState(ProxyStates.PROXY_STARTED)
                )

            # Reformat the line with the local listening port:
            targetHostStr = "127.0.0.1:" if listenOnHadHost else ""
            targetPortLine = re.sub(
                targetPortRegex,
                r"\g<1>{:s}{:d}\g<5>".format(targetHostStr, proxyPort),
                targetPortLine,
            )
            logging.debug(
                "Remote vscode TCP listener line rewritten: %s", targetPortLine.strip()
            )

            if copyToFile:
                copyToFile.write(targetPortLine)
                copyToFile.flush()
            sys.stdout.write(targetPortLine)

        if not omitLine:
            if copyToFile:
                copyToFile.write(outputLine)
                copyToFile.flush()
            sys.stdout.write(outputLine)

        sys.stdout.flush()

    # All done, let everyone know:
    with proxyStateCond:
        proxyState = ProxyStates.END
        logging.debug("[STATE] END <- stdout thread")
        proxyStateCond.notify_all()


def normalize_session_mode(value):
    return "persistent" if value == "persistent" else "ephemeral"


def sanitize_session_key(value):
    trimmed = (value or "").strip()
    if not trimmed:
        return "default"
    sanitized = sessionKeySanitizeRegex.sub("-", trimmed).strip(".-")
    if not sanitized:
        return "default"
    return sanitized[:64]


def resolve_state_dir(value):
    base = value or DEFAULT_SESSION_STATE_DIR
    return os.path.abspath(os.path.expanduser(base))


def get_current_user():
    try:
        return os.environ.get("USER") or getpass.getuser()
    except Exception:
        return None


@contextlib.contextmanager
def file_lock(lock_path):
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    lock_file = open(lock_path, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
        finally:
            lock_file.close()


def get_session_paths(session_key):
    base_dir = resolve_state_dir(cliArgs.sessionStateDir)
    safe_key = sanitize_session_key(session_key)
    user = get_current_user() or "unknown"
    safe_user = sanitize_session_key(user)
    sessions_root = os.path.join(base_dir, "sessions")
    namespaced_dir = os.path.join(sessions_root, safe_user, safe_key)
    legacy_dir = os.path.join(sessions_root, safe_key)
    def build_paths(session_dir):
        return {
            "session_dir": session_dir,
            "clients_dir": os.path.join(session_dir, "clients"),
            "job_path": os.path.join(session_dir, "job.json"),
            "lock_path": os.path.join(session_dir, "lock"),
        }
    return {
        "safe_key": safe_key,
        "safe_user": safe_user,
        "namespaced": build_paths(namespaced_dir),
        "legacy": build_paths(legacy_dir),
    }


def read_job_state(job_path):
    try:
        with open(job_path, "r") as handle:
            return json.load(handle)
    except Exception:
        return None


def write_job_state(job_path, state):
    tmp_path = job_path + ".tmp"
    with open(tmp_path, "w") as handle:
        json.dump(state, handle)
    os.replace(tmp_path, job_path)


def query_job_state(job_id):
    if not job_id:
        return None
    result = subprocess.run(
        ["squeue", "-h", "-j", str(job_id), "-o", "%T"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode == 0:
        output = result.stdout.strip()
        if output:
            return output.splitlines()[0].strip().upper()

    result = subprocess.run(
        ["scontrol", "show", "job", "-o", str(job_id)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        return None
    match = re.search(r"JobState=([A-Za-z]+)", result.stdout)
    return match.group(1).upper() if match else None


def query_job_nodelist(job_id):
    if not job_id:
        return None
    result = subprocess.run(
        ["squeue", "-h", "-j", str(job_id), "-o", "%N"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode == 0:
        output = result.stdout.strip()
        if output:
            line = output.splitlines()[0].strip()
            if line and line not in {"(null)", "None", "N/A"}:
                return line

    result = subprocess.run(
        ["scontrol", "show", "job", "-o", str(job_id)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        return None
    match = re.search(r"NodeList=([^\s]+)", result.stdout)
    if match:
        return match.group(1)
    match = re.search(r"BatchHost=([^\s]+)", result.stdout)
    if match:
        return match.group(1)
    return None


def expand_nodelist(nodelist):
    if not nodelist:
        return []
    result = subprocess.run(
        ["scontrol", "show", "hostnames", nodelist],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode == 0:
        hosts = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        if hosts:
            return hosts

    if "[" not in nodelist and "," not in nodelist:
        return [nodelist]

    match = re.match(r"^([A-Za-z0-9._-]+)\[([^\]]+)\].*$", nodelist)
    if not match:
        return []
    prefix = match.group(1)
    body = match.group(2)
    first_chunk = body.split(",")[0].strip()
    if not first_chunk:
        return []
    first_value = first_chunk.split("-")[0].strip()
    if not first_value:
        return []
    return [f"{prefix}{first_value}"]


def resolve_persistent_node(job_id, session_paths):
    nodelist = query_job_nodelist(job_id)
    nodes = expand_nodelist(nodelist)
    if not nodes:
        logging.warning("Unable to determine node list for job %s.", job_id)
        return None

    job_path = session_paths["job_path"]
    lock_path = session_paths["lock_path"]
    selected = None
    with file_lock(lock_path):
        state = read_job_state(job_path) or {}
        preferred = state.get("node")
        if preferred and preferred in nodes:
            selected = preferred
        else:
            selected = nodes[0]
            if selected != preferred:
                state["node"] = selected
                if not state.get("job_id"):
                    state["job_id"] = job_id
                write_job_state(job_path, state)
    logging.info("Using persistent node %s for job %s", selected, job_id)
    return selected


def is_job_alive(job_id):
    state = query_job_state(job_id)
    if not state:
        return False
    return state in {"RUNNING", "PENDING", "CONFIGURING"}


def wait_for_job_running(job_id, timeout_seconds):
    if timeout_seconds is None:
        timeout_seconds = DEFAULT_SESSION_WAIT_SECONDS
    if timeout_seconds <= 0:
        return True
    terminal_states = {
        "CANCELLED",
        "COMPLETED",
        "FAILED",
        "TIMEOUT",
        "NODE_FAIL",
        "PREEMPTED",
        "BOOT_FAIL",
        "OUT_OF_MEMORY",
    }
    deadline = time.time() + timeout_seconds
    last_state = None
    while True:
        state = query_job_state(job_id) or "UNKNOWN"
        if state == "RUNNING":
            return True
        if state in terminal_states:
            logging.error("Job %s ended before running (state %s).", job_id, state)
            return False
        now = time.time()
        if now >= deadline:
            logging.error(
                "Timed out waiting for job %s to start (last state %s).",
                job_id,
                state,
            )
            return False
        if state != last_state:
            logging.info("Waiting for job %s to start (state %s).", job_id, state)
            last_state = state
        time.sleep(2)


def build_idle_monitor_script(session_dir, idle_timeout, stale_seconds):
    safe_dir = shlex.quote(session_dir)
    idle_value = int(idle_timeout)
    stale_value = int(stale_seconds)
    return (
        "#!/bin/bash\n"
        "set -euo pipefail\n"
        f"SESSION_DIR={safe_dir}\n"
        'CLIENTS="$SESSION_DIR/clients"\n'
        f"IDLE_TIMEOUT={idle_value}\n"
        f"STALE_SECONDS={stale_value}\n"
        'STALE_MINUTES=$(( (STALE_SECONDS + 59) / 60 ))\n'
        'mkdir -p "$CLIENTS"\n'
        "last_seen=$(date +%s)\n"
        "while true; do\n"
        "  now=$(date +%s)\n"
        '  if [ "$STALE_SECONDS" -gt 0 ] && [ "$STALE_MINUTES" -gt 0 ]; then\n'
        '    find "$CLIENTS" -type f -mmin +$STALE_MINUTES -delete 2>/dev/null || true\n'
        "  fi\n"
        '  if compgen -G "$CLIENTS/*" > /dev/null; then\n'
        "    last_seen=$now\n"
        '  elif [ "$IDLE_TIMEOUT" -gt 0 ] && [ $((now - last_seen)) -ge "$IDLE_TIMEOUT" ]; then\n'
        '    echo "idle timeout" >> "$SESSION_DIR/monitor.log"\n'
        "    exit 0\n"
        "  fi\n"
        "  sleep 10\n"
        "done\n"
    )


def submit_persistent_job(salloc_args, session_dir, idle_timeout, stale_seconds, job_name):
    os.makedirs(session_dir, exist_ok=True)
    script = build_idle_monitor_script(session_dir, idle_timeout, stale_seconds)
    cmd = ["sbatch", "--parsable"]
    if job_name:
        cmd.append(f"--job-name={job_name}")
    def has_flag(args, flags):
        for arg in args:
            if arg in flags:
                return True
            for flag in flags:
                if arg.startswith(flag + "="):
                    return True
        return False
    if not has_flag(salloc_args or [], ["--output", "-o"]):
        cmd.append(f"--output={os.path.join(session_dir, 'slurm-%j.out')}")
    if not has_flag(salloc_args or [], ["--error", "-e"]):
        cmd.append(f"--error={os.path.join(session_dir, 'slurm-%j.err')}")
    if salloc_args:
        cmd.extend(salloc_args)
    cmd.extend(["--wrap", f"bash -lc {shlex.quote(script)}"])
    logging.debug('Submitting persistent job: "%s"', " ".join(cmd))
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "sbatch failed")
    output = result.stdout.strip()
    job_id = output.split(";")[0] if output else ""
    if not job_id:
        raise RuntimeError("sbatch did not return a job id")
    return job_id


def ensure_persistent_job(salloc_args, session_key):
    paths = get_session_paths(session_key)
    safe_key = paths["safe_key"]
    namespaced = paths["namespaced"]
    legacy = paths["legacy"]

    def try_existing(path_info, label):
        with file_lock(path_info["lock_path"]):
            state = read_job_state(path_info["job_path"])
            if state and is_job_alive(state.get("job_id")):
                job_id = state.get("job_id")
                logging.info(
                    "Reusing persistent Slurm job %s for session %s (%s)",
                    job_id,
                    safe_key,
                    label,
                )
                return job_id
        return None

    job_id = try_existing(namespaced, "namespaced")
    if job_id:
        return job_id, namespaced

    job_id = try_existing(legacy, "legacy")
    if job_id:
        return job_id, legacy

    with file_lock(namespaced["lock_path"]):
        state = read_job_state(namespaced["job_path"])
        if state and is_job_alive(state.get("job_id")):
            job_id = state.get("job_id")
            logging.info("Reusing persistent Slurm job %s for session %s (namespaced)", job_id, safe_key)
            return job_id, namespaced
        job_id = submit_persistent_job(
            salloc_args,
            namespaced["session_dir"],
            cliArgs.sessionIdleTimeout,
            cliArgs.sessionStaleSeconds,
            cliArgs.sessionJobName,
        )
        write_job_state(
            namespaced["job_path"],
            {
                "job_id": job_id,
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "args": salloc_args or [],
                "session_key": safe_key,
                "job_name": cliArgs.sessionJobName or "",
            },
        )
        logging.info("Started persistent Slurm job %s for session %s (namespaced)", job_id, safe_key)
        return job_id, namespaced


def start_session_marker(clients_dir, heartbeat_seconds):
    os.makedirs(clients_dir, exist_ok=True)
    marker_name = f"{os.getpid()}.{uuid.uuid4().hex}"
    marker_path = os.path.join(clients_dir, marker_name)
    with open(marker_path, "w") as handle:
        handle.write(str(time.time()))
    stop_event = threading.Event()

    def heartbeat():
        while not stop_event.wait(heartbeat_seconds):
            try:
                os.utime(marker_path, None)
            except FileNotFoundError:
                break

    thread = threading.Thread(name="Session-Heartbeat", target=heartbeat, daemon=True)
    thread.start()
    return marker_path, stop_event


async def runloop():
    """The main asyncio event loop for this script.  Starts the TCP port proxy so it will be awaiting a startup signal (once the remote hostname and TCP port are known).  Launches the remote shell via Slurm and connects its stdio channels to threaded i/o handlers.  The function then goes to sleep until the program state reaches END, then cleans-up the remote shell subprocess and TCP proxy runloop before exiting."""
    global proxyState, proxyStateCond, cliArgs, teeFiles

    logging.debug("Runloop start")
    with proxyStateCond:
        proxyState = ProxyStates.BEGIN
        logging.debug("[STATE] BEGIN <- main runloop")
        proxyStateCond.notify_all()

    # Get a separate thread setup for the TCP proxy:
    proxyLoop = asyncio.new_event_loop()
    proxyThread = threading.Thread(
        name="TCP-Proxy", target=start_tcp_proxy, args=(proxyLoop,), daemon=True
    )
    proxyThread.start()

    session_marker = None
    session_stop = None

    # Start the remote shell:
    if cliArgs.sessionMode == "persistent":
        job_id, session_paths = ensure_persistent_job(
            cliArgs.sallocArgs or [], cliArgs.sessionKey
        )
        session_dir = session_paths["session_dir"]
        clients_dir = session_paths["clients_dir"]
        os.makedirs(clients_dir, exist_ok=True)
        if not wait_for_job_running(job_id, cliArgs.sessionWaitSeconds):
            logging.error("Persistent job %s is not ready; exiting.", job_id)
            return
        persistent_node = resolve_persistent_node(job_id, session_paths)
        heartbeat_seconds = int(cliArgs.sessionHeartbeatSeconds)
        if heartbeat_seconds <= 0:
            heartbeat_seconds = DEFAULT_SESSION_HEARTBEAT_SECONDS
        session_marker, session_stop = start_session_marker(
            clients_dir, heartbeat_seconds
        )
        remoteShellCmd = [
            "srun",
            "--jobid",
            str(job_id),
            "--overlap",
            "--nodes=1",
            "--ntasks=1",
        ]
        if persistent_node:
            remoteShellCmd.extend(["--nodelist", persistent_node])
        remoteShellCmd.extend(["bash", "-l"])
        logging.info("Using persistent allocation job %s", job_id)
    else:
        remoteShellCmd = ["salloc"]
        if cliArgs.sallocArgs:
            remoteShellCmd.extend(cliArgs.sallocArgs)
    logging.debug('Command to launch remote shell: "%s"', " ".join(remoteShellCmd))
    remoteShellProc = subprocess.Popen(
        remoteShellCmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        #        shell=True,
    )
    logging.info("Remote shell launched with pid %d", remoteShellProc.pid)

    # Start the stdio threads:
    stdinProxy = threading.Thread(
        name="Remote-Shell-Stdin",
        target=stdinProxyThread,
        args=(remoteShellProc.stdin, teeFiles["stdin"]),
        daemon=True,
    )
    stderrProxy = threading.Thread(
        name="Remote-Shell-Stderr",
        target=stderrProxyThread,
        args=(remoteShellProc.stderr, teeFiles["stderr"]),
        daemon=True,
    )
    stdoutProxy = threading.Thread(
        name="Remote-Shell-Stdout",
        target=stdoutProxyThread,
        args=(remoteShellProc.stdout, teeFiles["stdout"]),
        daemon=True,
    )
    stdoutProxy.start()
    stderrProxy.start()
    stdinProxy.start()
    with proxyStateCond:
        logging.debug("Awaiting proxy termination...")
        proxyStateCond.wait_for(lambda: checkProxyState(ProxyStates.END))

    # Terminate the remote shell:
    logging.info("Terminating remote shell process...")
    remoteShellProc.terminate()
    try:
        remoteShellProc.wait(timeout=10)
    except:
        remoteShellProc.kill()

    if session_stop is not None:
        session_stop.set()
    if session_marker:
        try:
            os.remove(session_marker)
        except FileNotFoundError:
            pass

    # Terminate the TCP proxy event loop:
    logging.debug("Terminating TCP proxy event loop...")
    await proxyLoop.shutdown_asyncgens()
    proxyLoop.stop()

    # We don't bother joining the i/o threads, they're daemons anyway.

    logging.debug("Proxy has terminated.")


loggingLevels = [
    logging.CRITICAL,
    logging.ERROR,
    logging.WARNING,
    logging.INFO,
    logging.DEBUG,
]
baseLoggingLevel = 1

cliParser = argparse.ArgumentParser(description="vscode remote shell proxy")
cliParser.add_argument(
    "-v",
    "--verbose",
    dest="verbosity",
    default=0,
    action="count",
    help="increase the level of output as the program executes",
)
cliParser.add_argument(
    "-q",
    "--quiet",
    dest="quietness",
    default=0,
    action="count",
    help="decrease the level of output as the program executes",
)
cliParser.add_argument(
    "-l",
    "--log-file",
    metavar="<PATH>",
    dest="logFile",
    default=None,
    help='direct all logging to this file rather than stderr; the token "[PID]" will be replaced with the running pid',
)
cliParser.add_argument(
    "-0",
    "--tee-stdin",
    metavar="<PATH>",
    dest="teeStdinFile",
    default=None,
    help='send a copy of input to the script stdin to this file; the token "[PID]" will be replaced with the running pid',
)
cliParser.add_argument(
    "-1",
    "--tee-stdout",
    metavar="<PATH>",
    dest="teeStdoutFile",
    default=None,
    help='send a copy of output to the script stdout to this file; the token "[PID]" will be replaced with the running pid',
)
cliParser.add_argument(
    "-2",
    "--tee-stderr",
    metavar="<PATH>",
    dest="teeStderrFile",
    default=None,
    help='send a copy of output to the script stderr to this file; the token "[PID]" will be replaced with the running pid',
)
cliParser.add_argument(
    "-b",
    "--backlog",
    metavar="<N>",
    dest="backlog",
    default=DEFAULT_BACKLOG,
    type=int,
    help="number of backlogged connections held by the proxy socket (see man page for listen(), default {:d})".format(
        DEFAULT_BACKLOG
    ),
)
cliParser.add_argument(
    "-B",
    "--byte-limit",
    metavar="<N>",
    dest="byteLimit",
    default=DEFAULT_BYTE_LIMIT,
    type=int,
    help="maximum bytes read at one time per socket (default {:d}".format(
        DEFAULT_BYTE_LIMIT
    ),
)
cliParser.add_argument(
    "-H",
    "--listen-host",
    metavar="<HOSTNAME>",
    dest="listenHost",
    default="127.0.0.1",
    help="the client-facing TCP proxy should bind to this interface (default 127.0.0.1; use 0.0.0.0 for all interfaces)",
)
cliParser.add_argument(
    "-p",
    "--listen-port",
    metavar="<N>",
    dest="listenPort",
    default=0,
    type=int,
    help="the client-facing TCP proxy port (default 0 implies a random port is chosen)",
)
cliParser.add_argument(
    "-g",
    "--group",
    "--workgroup",
    metavar="<WORKGROUP>",
    dest="workgroup",
    default=None,
    help="the workgroup used to submit the vscode job",
)
cliParser.add_argument(
    "-S",
    "--salloc-arg",
    metavar="<SLURM-ARG>",
    dest="sallocArgs",
    action="append",
    help="used zero or more times to specify arguments to the salloc command being wrapped (e.g. --partition=<name>, --ntasks=<N>)",
)
cliParser.add_argument(
    "--session-mode",
    dest="sessionMode",
    choices=["ephemeral", "persistent"],
    default="ephemeral",
    help="allocation mode for the Slurm session (default ephemeral)",
)
cliParser.add_argument(
    "--session-key",
    dest="sessionKey",
    default="",
    help="identifier for persistent sessions (default: derived from alias)",
)
cliParser.add_argument(
    "--session-idle-timeout",
    dest="sessionIdleTimeout",
    default=DEFAULT_SESSION_IDLE_TIMEOUT,
    type=int,
    help="seconds of idle time before a persistent allocation is cancelled (default 0 = never)",
)
cliParser.add_argument(
    "--session-state-dir",
    dest="sessionStateDir",
    default=DEFAULT_SESSION_STATE_DIR,
    help="base directory for persistent session state (default ~/.slurm-connect)",
)
cliParser.add_argument(
    "--session-heartbeat-seconds",
    dest="sessionHeartbeatSeconds",
    default=DEFAULT_SESSION_HEARTBEAT_SECONDS,
    type=int,
    help="heartbeat interval in seconds for session markers (default 30)",
)
cliParser.add_argument(
    "--session-stale-seconds",
    dest="sessionStaleSeconds",
    default=DEFAULT_SESSION_STALE_SECONDS,
    type=int,
    help="consider session markers stale after this many seconds (default 90)",
)
cliParser.add_argument(
    "--session-job-name",
    dest="sessionJobName",
    default=DEFAULT_SESSION_JOB_NAME,
    help="job name for persistent allocations (default slurm-connect)",
)
cliParser.add_argument(
    "--session-wait-seconds",
    dest="sessionWaitSeconds",
    default=DEFAULT_SESSION_WAIT_SECONDS,
    type=int,
    help="seconds to wait for a persistent allocation to start before failing (default 300)",
)

cliArgs = cliParser.parse_args()
cliArgs.sessionMode = normalize_session_mode(cliArgs.sessionMode)
if cliArgs.sessionIdleTimeout < 0:
    cliArgs.sessionIdleTimeout = DEFAULT_SESSION_IDLE_TIMEOUT
if cliArgs.sessionHeartbeatSeconds <= 0:
    cliArgs.sessionHeartbeatSeconds = DEFAULT_SESSION_HEARTBEAT_SECONDS
if cliArgs.sessionStaleSeconds < 0:
    cliArgs.sessionStaleSeconds = DEFAULT_SESSION_STALE_SECONDS
if cliArgs.sessionWaitSeconds < 0:
    cliArgs.sessionWaitSeconds = DEFAULT_SESSION_WAIT_SECONDS

# Figure the logging level:
chosenLoggingLevel = min(
    max(0, baseLoggingLevel + cliArgs.verbosity - cliArgs.quietness),
    len(loggingLevels) - 1,
)
if cliArgs.logFile:
    cliArgs.logFile = cliArgs.logFile.replace("[PID]", str(os.getpid()))
logging.basicConfig(
    filename=cliArgs.logFile,
    level=loggingLevels[chosenLoggingLevel],
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Get tee files opened:
teeFiles = {"stdin": None, "stdout": None, "stderr": None}
if cliArgs.teeStdinFile:
    teeFiles["stdin"] = open(
        cliArgs.teeStdinFile.replace("[PID]", str(os.getpid())), "w"
    )
if cliArgs.teeStdoutFile:
    teeFiles["stdout"] = open(
        cliArgs.teeStdoutFile.replace("[PID]", str(os.getpid())), "w"
    )
if cliArgs.teeStderrFile:
    teeFiles["stderr"] = open(
        cliArgs.teeStderrFile.replace("[PID]", str(os.getpid())), "w"
    )

# If no workgroup was provided, find one for this user:
# if cliArgs.workgroup is None:
#    logging.debug('Looking-up a workgroup for the current user')
#    workgroupLookupProc = subprocess.Popen(['workgroup', '-q', 'workgroups'],
#                                stdout=subprocess.PIPE,
#                                stderr=subprocess.PIPE,
#                                text=True
#                            )
#    (workgroupStdout, dummy) = workgroupLookupProc.communicate()
#    # Extract the left-most <gid> <gname> pair:
#    workgroupMatch = re.match(r'^\s*[0-9]+\s*(\S+)', workgroupStdout)
#    if workgroupMatch is None:
#        logging.critical('No workgroup provided and user appears to be a member of no workgroups')
#        exit(errno.EINVAL)
##    cliArgs.workgroup = workgroupMatch.group(1)
#   logging.info('Automatically selected workgroup %s', cliArgs.workgroup)

# Run the proxy server:
asyncio.run(runloop())
