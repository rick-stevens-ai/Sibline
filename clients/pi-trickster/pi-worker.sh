#!/bin/bash
# Sibline task-worker wrapper for pi (Earendil) trickster hosts.
#
# The sibline subscriber calls this with the task prompt as $1 when a
# kind=task_request arrives. It runs a headless pi agent turn and prints the
# final answer to stdout, which the subscriber captures and publishes as
# task_result.
#
# Wire it up in the subscriber's environment (sibline.env):
#   SIBLINE_WORKER_CMD=/home/stevens/.hermes/spark-trio/pi-worker.sh
#   SIBLINE_WORKER_SHELL=0      # subscriber shlex-splits and appends the prompt as one arg
#   SIBLINE_WORKER_TIMEOUT=300
#
# Login shell (-lc) gives pi its normal PATH (~/.local/bin) and env.
exec bash -lc 'pi -p "$1"' _ "$1"
