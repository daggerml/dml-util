FROM python:3.12

COPY . /dml-lib/

RUN pip3 install --no-cache-dir /dml-lib/ ./dml-lib/submodules/python-lib/ ./dml-lib/submodules/daggerml-cli/

# Write the entrypoint script to /entrypoint.sh
RUN cat <<'EOF' > /entrypoint.sh
#!/bin/sh
# This entrypoint script expects at least one argument: the command to execute.
if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <command> [arguments...]"
  exit 1
fi

# Save the first argument as the command, then shift so "$@" contains only the additional arguments.
cmd="$1"
shift

# Execute the command with any additional arguments.
exec "$cmd" "$@"
EOF

# Make the script executable
RUN chmod +x /entrypoint.sh

# Set the entrypoint to the script
ENTRYPOINT ["/entrypoint.sh"]
