#!/bin/sh
# Wrapper script that handles both direct kubectl calls and shell commands
# If invoked with /bin/sh as first arg, pass through to shell
# Otherwise, execute kubectl with all arguments

# Enable debug mode if DEBUG=1
if [ "$DEBUG" = "1" ]; then
    set -x
fi

# Handle help and version flags at wrapper level
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    exec /app/kubectl --help
elif [ "$1" = "--version" ] || [ "$1" = "-v" ]; then
    exec /app/kubectl version --client
elif [ "$1" = "/bin/sh" ] || [ "$1" = "sh" ]; then
    # Shell command mode: execute with the specified shell
    exec "$@"
elif [ "$1" = "-c" ] || [ "$1" = "-s" ]; then
    # Direct shell flags: execute with sh
    exec /bin/sh "$@"
elif [ "$1" = "kubectl" ]; then
    # User explicitly called kubectl, shift it off and pass remaining args
    shift
    exec /app/kubectl "$@"
else
    # Direct kubectl mode: execute kubectl with all arguments
    exec /app/kubectl "$@"
fi