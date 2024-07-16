#!/usr/bin/python3

import sys
import argparse
from zfs import zfs, zpool
import compression as comp
from pv import pv, pv_file
from datetime import datetime
import os

from ssh_extended import ssh_send


z = zfs()
zp = zpool()
class local_send():

    def __init__(self,source,destination,snap=None,snap1=None,create_snapshot=False,oneshot=False,recurse=True,verbose=False,update_bootfs=False,update_file_snapshot=None):
        self.source = source
        self.dest = destination
        self.snap = snap
        self.snap1 = snap1
        self.create_snapshot=create_snapshot
        self.oneshot=oneshot
        self.verbose=verbose
        self.recurse=True
        self.state = 0
        self.update_bootfs = update_bootfs
        self.update_file_snapshot = update_file_snapshot
        self.bootfs = None

        if self.update_bootfs or self.update_file_snapshot:
            bootfs_raw = zp.get(zpool=None, property='bootfs')

            if len(bootfs_raw.keys()) == 1:
                self.bootfs = bootfs_raw.values()[0].split('@')[0]
            else:
                if 'syspool' in bootfs_raw.keys():
                    self.bootfs = bootfs_raw['syspool'].split('@')[0]
                elif 'archive' in bootfs_raw.keys():
                    self.bootfs = bootfs_raw['archive'].split('@')[0]

        if self.update_bootfs and self.bootfs:
            if not self.dest:
                self.dest = self.bootfs
                print('Found bootfs for update %s' % self.dest)

        if self.update_file_snapshot and self.bootfs:
            if not self.source:
                self.source = self.bootfs
                print('Found bootfs to create file update %s ' % self.source)


        try:
            self.zfs = zfs()
            if self.zfs.is_zfs(self.source) or self.is_file(self.source):
                state = 1
        except:
           pass




    def is_file(self,dataset):
        if dataset[0] == '/':
            return True
        else:
            return False

    def get_dataset_type(self,dataset):
        type = None
        compression=None
        if self.is_file(dataset):
            type='file'
            try:
                ext = dataset.split('.')[1]
                if ext == 'img':
                    compression = None
                elif ext == 'gz':
                    compression = 'gzip'
                elif ext == 'lz4':
                    compression = 'lz4'
                elif ext == 'bz2':
                    compression = 'bzip2'
                elif ext == 'xz':
                    compression = 'xz'
                elif ext == 'zstd':
                    compression == 'zstd'
                else:
                    compression = None
            except:
                pass
        else:
            type = 'zfs'

        return type, compression


    def send_snapshot(self):
        src_type, src_compression = self.get_dataset_type(self.source)
        dst_type, dst_compression = self.get_dataset_type(self.dest)

        if src_type == 'file' and dst_type == "zfs":
            p = pv_file(file=self.source,verbose=self.verbose)

            #p = comp.uncompressor_file(self.source,src_compression)
            if p:
                #p1 = pv(fd=p.stdout,verbose=self.verbose,size=size)
                if src_compression:
                    p1 = comp.uncompressor(fd=p.stdout,type=src_compression)
                    p2 = z.recv_pipe(fd=p1.stdout,dataset=self.dest)

                    p2.communicate()
                else:
                    p2 = z.recv_pipe(fd=p.stdout,dataset=self.dest)
                    p2.communicate()

                if self.dest == self.bootfs:
                    snaps = self.zfs.get_snapshots(self.dest)
                    last_snap = snaps[-1]
                    bootfs = self.dest + '@' + last_snap
                    zpool = self.dest.split('/')[0]
                    print('Updating bootfs to %s' % bootfs)
                    zp.set(zpool,'bootfs',bootfs)



        elif src_type == 'zfs' and dst_type == "file":
            if z.is_zfs(self.source):
                snaps = self.zfs.get_snapshots(self.source)
                if self.update_file_snapshot:
                    found_snap = None
                    version = self.update_file_snapshot.split('.')[0]
                    for snap in snaps:
                        snap_version = snap.split('.')[0]
                        if version == snap_version:
                            found_snap = snap
                            latest_snap = snaps[-1]
                            if found_snap != latest_snap:
                                self.snap = found_snap
                                self.snap1 = latest_snap
                            else:
                                found_snap = None
                                self.snap = None
                                self.snap1 = None
                    if found_snap:
                        print('Creating update file from %s to %s' % (self.snap,self.snap1))






                if self.snap and self.snap in snaps and not self.snap1:
                    if self.verbose:
                        print('Sending full snapshot %s to file %s ' % (self.snap, self.dest))
                    send = self.zfs.adaptive_send(dataset=self.source, snap=self.snap, recurse=self.recurse,
                                                  verbose=self.verbose,compression=dst_compression)
                    out_file = open(self.dest, 'w+')

                    while send.poll() is None:
                        out_file.write(send.stdout.read(4096))
                        out_file.flush()

                    out_file.close()

                elif self.snap and self.snap1 and ( self.snap in snaps and self.snap1 in snaps):
                    if self.verbose:
                        print('Performing incremental send from %s to %s' % (self.snap, self.snap1))
                    send = self.zfs.adaptive_send(dataset=self.source, snap=self.snap, snap1=self.snap1,
                                                  recurse=self.recurse,
                                                  verbose=self.verbose, compression=dst_compression)
                    out_file = open(self.dest, 'w+')

                    while send.poll() is None:
                        out_file.write(send.stdout.read(4096))
                        out_file.flush()

                    out_file.close()

                else:
                    if not self.update_file_snapshot:
                        if not self.create_snapshot:
                            if snaps:
                                snap = snaps[-1]
                                if self.verbose:
                                    print('Sending full snapshot %s to file %s ' % (snap,self.dest))
                                send = self.zfs.adaptive_send(dataset=self.source, snap=snap, recurse=self.recurse,
                                                              verbose=self.verbose,
                                                              compression=dst_compression)
                                out_file = open(self.dest, 'w+')

                                while send.poll() is None:
                                    out_file.write(send.stdout.read(4096))
                                    out_file.flush()

                                out_file.close()

                        else:
                            now = datetime.now()
                            date = now.strftime('%y%m%d')
                            hour = now.strftime('%H')
                            minsec = now.strftime('%M%S')
                            snap_name = 'migrate-%s-%s-%s' % (date, hour, minsec)
                            if self.verbose:
                                print('Creating snapshot for send %s' % snap_name)
                            self.zfs.snapshot(self.source, snap_name, self.recurse)
                            if self.verbose:
                                print('Sending full snapshot %s' % snap_name)

                            send = self.zfs.adaptive_send(dataset=self.source, snap=snap_name, recurse=self.recurse,
                                                          verbose=self.verbose,
                                                          compression=dst_compression)

                            out_file = open(self.dest, 'w+')

                            while send.poll() is None:
                                out_file.write(send.stdout.read(4096))
                                out_file.flush()

                            out_file.close()



                            if self.oneshot:
                                print('Oneshot send. Cleaning up snapshot %s' % snap_name)
                                self.zfs.destroy(self.source + '@' + snap_name, recurse=self.recurse)





        elif src_type == 'zfs' and dst_type == "zfs":
            if z.is_zfs(self.dest):
                recv_snaps = self.zfs.get_snapshots(self.dest)
                if recv_snaps:
                    snap, snap1 = self.zfs.engociate_inc_send(self.source, recv_snaps)
                    if snap and snap1:
                        if snap != snap1:
                            if self.verbose:
                                print('Performing incremental send from %s to %s' % (snap, snap1))
                            send = self.zfs.adaptive_send(dataset=self.source, snap=snap, snap1=snap1,
                                                          recurse=self.recurse,
                                                          verbose=self.verbose,compression=None)

                            recv = self.zfs.recv_pipe(send.stdout,dataset=self.dest)

                            recv.communicate()

                        else:
                            if self.verbose:
                                print('Target dataset is up to date on %s' % snap)

                            if self.create_snapshot:
                                now = datetime.now()
                                date = now.strftime('%y%m%d')
                                hour = now.strftime('%H')
                                minsec = now.strftime('%M%S')
                                snap1 = 'migrate-%s-%s-%s' % (date, hour, minsec)
                                print('Creating snapshot for send %s' % snap1)
                                self.zfs.snapshot(self.source, snap1, self.recurse)
                                print('Performing incremental send from %s to %s' % (snap, snap1))
                                send = self.zfs.adaptive_send(dataset=self.source, snap=snap, snap1=snap1,
                                                              recurse=self.recurse,
                                                              verbose=self.verbose,compression=None)
                                recv = self.zfs.recv_pipe(send.stdout, dataset=self.dest)

                                recv.communicate()


                else:
                    if self.verbose:
                        print('Unable to setup incremental stream for zfs send. Recv dataset has no snapshots')
                    pass


            else:
                if self.snap and self.snap in self.zfs.get_snapshots(self.source):
                    if self.verbose:
                        print('Sending full snapshot %s' % self.snap)
                    send = self.zfs.adaptive_send(dataset=self.source, snap=self.snap, recurse=self.recurse,
                                                  verbose=self.verbose,compression=None)


                    recv = self.zfs.recv_pipe(send.stdout, dataset=self.dest)
                    recv.communicate()

                else:

                    if not self.create_snapshot:
                        snap_list = self.zfs.get_snapshots(self.source)
                        if snap_list:
                            snap = snap_list[-1]
                            if self.verbose:
                                print('Sending full snapshot %s' % snap)
                            send = self.zfs.adaptive_send(dataset=self.source, snap=snap, recurse=self.recurse,
                                                          verbose=self.verbose,
                                                          compression=None)

                            recv = self.zfs.recv_pipe(send.stdout,dataset=self.dest)

                            recv.communicate()

                    else:
                        now = datetime.now()
                        date = now.strftime('%y%m%d')
                        hour = now.strftime('%H')
                        minsec = now.strftime('%M%S')
                        snap_name = 'migrate-%s-%s-%s' % (date, hour, minsec)
                        if self.verbose:
                            print('Creating snapshot for send %s' % snap_name)
                        self.zfs.snapshot(self.source, snap_name, self.recurse)
                        if self.verbose:
                            print('Sending full snapshot %s' % snap_name)

                        send = self.zfs.adaptive_send(dataset=self.source, snap=snap_name, recurse=self.recurse,
                                                      verbose=self.verbose,
                                                      compression=None)

                        recv = self.zfs.recv_pipe(send.stdout, dataset=self.dest)


                        recv.communicate()

                        if self.oneshot:
                            print('Oneshot send. Cleaning up snapshot %s' % snap_name)
                            self.zfs.destroy(self.source + '@' + snap_name, recurse=self.recurse)
                            self.zfs.destroy(self.dest + '@' + snap_name, recurse=self.recurse)
















if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Usage: migrate.py -s pool/dataset -r 192.168.0.1 -l 20M -d newpool/dataset')
    parser.add_argument('-s', '--source', default=None, help='Source dataset')
    parser.add_argument('-r', '--remote', default=None, help='Remote host')
    parser.add_argument('-l', '--limit', default=0, help='Transfer speed limit')
    parser.add_argument('-t', '--time', default=0, help='Transfer time limit')
    parser.add_argument('-d', '--dest', default=0, help='Destination dataset')
    parser.add_argument('-c', '--compression', default=None, help='Compression algotithm: bzip2, gzip, xz, lz4, zstd')
    parser.add_argument('--snap', action='store_true', help='Create new snapshot if needed')
    parser.add_argument('-R', '--recursive', action='store_true', help='Send dataset recursivly')
    parser.add_argument('-o', '--oneshot', action='store_true', help='Remove temporary snapshots')
    parser.add_argument('--snap_after', action='store_true', help='Create init snapshot for recv dataset')
    parser.add_argument('--update', action='store_true', help='Guess and update bootfs on remote host')
    parser.add_argument('--sync', action='store_true', help='Keep latest local snapshot for replication with given host')
    parser.add_argument('--update_from_snap',help='Snapshot from which to create update file')
 #  parser.add_argument('-v', '--verbose', action='store_true',default=False, help='Transfer speed limit')

    args = parser.parse_args()


    if args.source:
        if '@' in args.source:
            source = args.source.split('@')[0]
            snap = args.source.split('@')[1]
        else:
            source = args.source
            snap = None
    else:
        source = None
        snap = None

    if not args.time:
        limit = args.limit
        time = 0

    else:
        limit = 0
        time = args.time

    if args.remote:
        if not args.compression:
            compression = 'auto'
        else:
            compression = args.compression
        remotes = args.remote.split(',')
        for remote in remotes:
            sender = ssh_send(host=remote,dataset=source,snapshot=snap,limit=limit,time=time,recv_dataset=args.dest,verbose=True,compress_type=compression,oneshot=args.oneshot,create_snapshot=args.snap,update_bootfs=args.update,sync=args.sync)

            if sender.state == 1:
                sender.send_snapshot()

    else:

        sender = local_send(source=source,destination=args.dest,snap=snap,create_snapshot=args.snap,oneshot=args.oneshot,verbose=True,update_bootfs=args.update,update_file_snapshot=args.update_from_snap)

        sender.send_snapshot()
