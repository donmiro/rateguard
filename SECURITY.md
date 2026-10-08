# Security policy

## Supported versions

| Version | Supported |
|---|---|
| 0.1.x | yes |

## Reporting a vulnerability

Please do not open a public issue. Report it privately through GitHub:
**Security → Report a vulnerability** on this repository. You will get an
answer within a week; a fix, if one is needed, goes out as a patch release,
with credit to you unless you prefer otherwise.

## What is and is not a vulnerability

rateguard's gossip protocol **assumes a trusted network**, as the README says:
datagrams are neither encrypted nor authenticated. That anyone who can reach
the UDP port can join the cluster, announce demand that shifts the shares, or
declare members dead is the documented design of 0.1, not a vulnerability.
Keep the port on a private network or behind a firewall that admits only your
own instances.

Worth reporting privately, for example:

- a datagram that makes a node panic, hang, or allocate without bound;
- a way to make `check()` admit far more than the documented bounds without
  access to the gossip port, through the keys of requests alone;
- memory that grows without bound under any traffic.
