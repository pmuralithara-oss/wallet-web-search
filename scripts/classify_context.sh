#!/usr/bin/env bash
# Usage: classify_context.sh <hit_id> <prompt_path> <result_path>
# Runs one classification via claude CLI.
set -euo pipefail

HIT_ID="$1"
PROMPT_PATH="$2"
RESULT_PATH="$3"

claude -p --model sonnet < "$PROMPT_PATH" > "$RESULT_PATH" 2>&1
exit $?
