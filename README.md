# migrate
Python tool to automate simple zfs send/recv tasks. Used to send datatets to local compressed files or to remote hosts with given compression and bandwidth limit by ssh. Also handles stream interrupts and best possible compression negotiation. Depends : pv, bzip2, gzip, xz, lz4, zstd. Full help:

usage: migrate.py [-h] [-s SOURCE] [-r REMOTE] [-l LIMIT] [-t TIME] [-d DEST] [-c COMPRESSION] [--snap] [-R] [-o] [--snap_after]
                  [--update] [--sync] [--update_from_snap UPDATE_FROM_SNAP]

Usage: migrate.py -s pool/dataset -r 192.168.0.1 -l 20M -d newpool/dataset

options:
  -h, --help            show this help message and exit
  -s SOURCE, --source SOURCE
                        Source dataset
  -r REMOTE, --remote REMOTE
                        Remote host
  -l LIMIT, --limit LIMIT
                        Transfer speed limit
  -t TIME, --time TIME  Transfer time limit
  -d DEST, --dest DEST  Destination dataset
  -c COMPRESSION, --compression COMPRESSION
                        Compression algotithm: bzip2, gzip, xz, lz4, zstd
  --snap                Create new snapshot if needed
  -R, --recursive       Send dataset recursivly
  -o, --oneshot         Remove temporary snapshots
  --snap_after          Create init snapshot for recv dataset
  --update              Guess and update bootfs on remote host
  --sync                Keep latest local snapshot for replication with given host
  --update_from_snap UPDATE_FROM_SNAP
                        Snapshot from which to create update file
