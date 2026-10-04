#!/bin/bash
# Sibline task-worker wrapper (Hermes hosts).
#
# The sibline subscriber calls this with the task prompt as $1 when a
# kind=task_request arrives. It runs a headless, non-interactive Hermes agent
# turn and prints the final answer to stdout, which the subscriber captures and
# publishes back as task_result.
#
# Wire it up in the subscriber's environment:
#   SIBLINE_WORKER_CMD=/home/you/.sibline/worker.sh
#   SIBLINE_WORKER_SHELL=0      # subscriber shlex-splits and appends the prompt as one arg
#   SIBLINE_WORKER_TIMEOUT=300
#
# --yolo avoids an approval deadlock (no human at the terminal in a daemon).
# The login shell (-lc) gives hermes its normal PATH/env.
exec bash -lc 'hermes --yolo -z "$1"' _ "$1"
