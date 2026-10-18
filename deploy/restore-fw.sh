#!/bin/sh
# Keeps the raw-mode firewall rules the exit node needs.
#
# The node terminates TCP in its own stack and hands the packets to the kernel through a raw socket.
# The kernel owns no socket for those flows, so it answers every reply with a real RST, and the client
# sees the tunnel as one-way. These rules drop exactly those.
#
# They are scoped by source address on purpose. The original unit used "-m mark ! --mark 0x2547",
# which expects the node to stamp its own sockets; nothing on this host carries that mark, so that
# form silences RST for the whole box. csqtt also rewrites the filter table, which is why this runs
# on a timer instead of only at service start.
set -e

RST_PORTS_RULE_MARK="openflux-rst"
ADDRESSES="13.143.65.15 13.143.65.27"

ensure() {
    if ! iptables -C "$@" 2>/dev/null; then
        iptables -A "$@"
    fi
}

for addr in $ADDRESSES; do
    ensure OUTPUT -p tcp -s "$addr" --tcp-flags RST RST -m comment --comment "$RST_PORTS_RULE_MARK" -j DROP
    ensure OUTPUT -p icmp -s "$addr" --icmp-type port-unreachable -m comment --comment "$RST_PORTS_RULE_MARK" -j DROP
done