#!/usr/bin/env python
import sys
sys.path.append('/opt/lib')
from subprocess import Popen, PIPE
from execute import execute
import re
from pv import pv
from compression import compressor, uncompressor, uncompressor_file
from datetime import datetime
import os
import shutil
from collections import OrderedDict


# def execute(args):
#    p = Popen(args, stdout=PIPE, stderr=PIPE)
#    output, err = p.communicate()
#    rc = p.returncode
#    return output,err,rc


class zfs():
    def is_zfs(self, dataset):
        out, err, rc = execute(['zfs', 'list', '-H', '-p', dataset.encode('utf-8')])
        list = []
        for i in out.split('\t'):
            list.append(i)
        if list and rc == 0 and list[0] == dataset:
            return True
        else:
            return False

    def type(self, dataset):
        props = self.get_all(dataset)
        if not 'origin' in props.keys():
            return [props['type'], 'original']
        else:
            return [props['type'], 'clone']

    def snapshot(self, dataset, snap, recurse=False):
        if not recurse:
            out, err, rc = execute(['zfs', 'snapshot', dataset + '@' + snap])
        else:
            out, err, rc = execute(['zfs', 'snapshot', '-r', dataset + '@' + snap])

        return rc


    def snapshot_auto(self,dataset,tag,tag1=None, recurse=False):
        now = datetime.utcnow()
        name = tag

        if tag1:
            name += '_'
            name += str(tag1)
        name += '_'

        time_s = now.strftime('%Y-%m-%d-%H-%M')

        name += time_s

        rc = self.snapshot(dataset,name,recurse)

        return rc, name


    def autoremove(self,dataset,keep=2,tag=None,recurse=False, tags={}):
        snaps = self.get_snapshots(dataset)

        if tag:
            new_snaps = []
            for snap in snaps:
                if tag in snap:
                    new_snaps.append(snap)
            snaps = new_snaps

            if snaps and len(snaps) > keep:
                del snaps[len(snaps) - keep:]

        elif tags:
            new_snaps = []
            for tag in tags.keys():
                keep = tags[tag]
                tag_snaps = []
                for snap in snaps:
                    if tag in snap:
                        tag_snaps.append(snap)

                if tag_snaps and len(tag_snaps) > keep:
                    del tag_snaps[len(tag_snaps) - keep:]
                    new_snaps.extend(tag_snaps)

            snaps = new_snaps

        else:
            if snaps and len(snaps) > keep:
                del snaps[len(snaps) - keep:]

        for snap in snaps:
            try:
                self.destroy(dataset+'@'+snap,recurse=recurse)
            except:
                pass




    def rollback(self, dataset, snap):
        out, err, rc = execute(['zfs', 'rollback', '-r', dataset + '@' + snap])
        return rc

    def restore_files(self,dataset,snap,file_list=[]):
        ret = 1
        if self.is_zfs(dataset):
            zfs_dir = os.path.join('/'+dataset,'.zfs')
            if not os.path.isdir(zfs_dir):
                self.set(dataset,'snapdir','visible')

            restore_path = os.path.join(zfs_dir,'snapshot',snap)
            if os.path.exists(restore_path):
                ret=0
                for f in file_list:
                    f_old = os.path.join(restore_path,f.split('/'+dataset)[1][1:])
                    shutil.copy(f_old,f)
        return ret




    def get_snapshots(self, dataset):
        result = []
        out, err, rc = execute(['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name'])
        if rc == 0:
            for i in out.splitlines():
                if dataset == i.split('@')[0]:
                    result.append(i.split('@')[1])

        return result

    def set(self, dataset, property, value):
        out, err, rc = execute(['zfs', 'set', property + '=' + value, dataset])
        return rc

    def get(self, dataset=None, property='all'):
        if dataset:
            out, err, rc = execute(['zfs', 'get', '-H', '-p', property, dataset])
            if out.split('\t')[2] != '-':
                value = out.split('\t')[2]
            else:
                value = None

            return value
        else:
            list = {}
            out, err, rc = execute(['zfs', 'get', '-H', property])
            for i in out.splitlines():

                if i.split('\t')[2] != '-':
                    list[i.split('\t')[0]] = i.split('\t')[2]
                else:
                    list[i.split('\t')[0]] = None

            return list

    def get_all(self, dataset):
        out, err, rc = execute(['zfs', 'get', '-H', 'all', dataset])
        list = {}
        for i in out.splitlines():
            list[i.split('\t')[1]] = i.split('\t')[2]

        return list

    def list(self, dataset=None):
        list = []
        if dataset == None:
            out, err, rc = execute(['zfs', 'list', '-H'])
        else:
            out, err, rc = execute(['zfs', 'list', '-H', '-r', dataset])
        for i in out.splitlines():
            list.append(i.split('\t')[0])
        return list

    def list_volumes(self):
        list = []
        out,err,rc = execute(['zfs','list','-t','volume','-H'])
        for i in out.splitlines():
            list.append(i.split('\t')[0])
        return list

    def clone(self, dataset, clone, property=None):
        if '@' in dataset:
            com = ['zfs', 'clone']
            if property:
                for i in property.keys():
                    com.append('-o')
                    com.append(i + '=' + property[i])
            com.append(dataset)
            com.append(clone)
            out, err, rc = execute(com)
            return rc
        else:
            snap = clone.split('/')[-1]
            snap_out, snap_err, snap_rc = execute(['zfs', 'snapshot', dataset + '@' + snap])
            com = ['zfs', 'clone']
            if property:
                for i in property.keys():
                    com.append('-o')
                    com.append(i + '=' + property[i])
            com.append(dataset + '@' + snap)
            com.append(clone)
            clone_out, clone_err, clone_rc = execute(com)
            if snap_rc == 0 and clone_rc == 0:
                return 0
            else:
                return 1

    def create(self, dataset, property=None):
        com = ['zfs', 'create']
        if property:
            for i in property.keys():
                com.append('-o')
                com.append(i + '=' + property[i])
        com.append(dataset)
        out, err, rc = execute(com)
        return rc

    def rename(selfself, dataset, dataset1):
        com = ['zfs', 'rename', dataset, dataset1]
        out, err, rc = execute(com)
        return rc

    def zvol_create(self, dataset, size=0, compression='lz4',bytes=0,volblocksize='8K'):
        if size:
            out, err, rc = execute(
                ['zfs', 'create', '-o', 'compression=' + compression, '-b', volblocksize, '-s', '-V', str(size) + 'G', dataset])
        elif bytes:
            out, err, rc = execute(
                ['zfs', 'create', '-o', 'compression=' + compression, '-s', '-V', str(bytes), '-b', volblocksize,
                 dataset])
        else:
            rc = -1
        return rc


    def share(self, dataset):
        out, err, rc = execute(['zfs', 'share', 'dataset'])
        return rc

    def unshare(self, dataset):
        out, err, rc = execute(['zfs', 'unshare', 'dataset'])
        return rc

    def destroy(self, dataset, recurse=False):
        if recurse == False:
            out, err, rc = execute(['zfs', 'destroy', dataset])
        else:
            out, err, rc = execute(['zfs', 'destroy', '-R', dataset])

        #print out, err, rc
        return rc

    def promote(self, dataset, recurse=False):
        if recurse == False:
            out, err, rc = execute(['zfs', 'promote', dataset])
        else:
            zlist = self.list(dataset)
            for fs in zlist:
                out, err, rc = execute(['zfs', 'promote', fs])
        return rc

    def diff(self, snap1, snap2=None):
        if snap2 == None:
            out, err, rc = execute(['zfs', 'diff', '-HF', snap1])
        else:
            out, err, rc = execute(['zfs', 'diff', '-HF', snap1, snap2])
        new = []
        mod = []
        err = []
        ren = []
        if rc == 0:
            for line in out.splitlines():
                args = line.split('\t')
                if args[0] == '+':
                    new.append((args[2], args[1]))

                elif args[0] == '-':
                    err.append((args[2], args[1]))

                elif args[0] == 'M':

                    mod.append((args[2], args[1]))

                elif args[0] == 'R':
                    ren.append((args[2], args[3], args[1]))

                else:
                    pass

        return new, mod, err, ren

    def get_space(self, dataset):
        space = {}
        out, err, rc = execute(['zfs', 'list', '-H', '-p', '-o', 'space', dataset])
        elem = out.split('\t')

        space['name'] = elem[0]
        space['avail'] = int(elem[1])
        space['used'] = int(elem[2])
        space['usedsnap'] = int(elem[3])
        space['useddss'] = int(elem[4])
        space['usedrefreserv'] = int(elem[5])
        space['usedchild'] = int(elem[6].strip('\n'))

        return space

    def conv_space(self, space):
        if space[-1] == 'K':
            return int(float(space[:-1].replace(',', '.')) * 1024)

        if space[-1] == 'M':
            return int(float(space[:-1].replace(',', '.')) * 1024 * 1024)

        if space[-1] == 'G':
            return int(float(space[:-1].replace(',', '.')) * 1024 * 1024 * 1024)

        if space[-1] == 'T':
            return int(float(space[:-1].replace(',', '.')) * 1024 * 1024 * 1024 * 1024)

    def hold(self, dataset, snapshot, tag, recurse=False):
        if self.is_zfs(dataset):
            if recurse:
                out, err, rc = execute(['zfs', 'hold', '-r', tag, dataset + '@' + snapshot])
            else:
                out, err, rc = execute(['zfs', 'hold', tag, dataset + '@' + snapshot])

            return rc
        else:
            return -1

    def release(self, dataset, snapshot, tag, recurse=False):
        if self.is_zfs(dataset):
            if recurse:
                out, err, rc = execute(['zfs', 'release', '-r', tag, dataset + '@' + snapshot])
            else:
                out, err, rc = execute(['zfs', 'release', tag, dataset + '@' + snapshot])

            return rc
        else:
            return -1

    def holds(self, dataset, snapshot, recurse=False):
        holds = []
        if self.is_zfs(dataset):
            if recurse:
                out, err, rc = execute(['zfs', 'holds', '-H', '-r', dataset + '@' + snapshot])
            else:
                out, err, rc = execute(['zfs', 'holds', '-H', dataset + '@' + snapshot])

            if rc == 0 and out:

                for line in sorted(out.splitlines(),reverse=True):
                    holds.append(line.split('\t')[1])

        return holds

    def get_holds(self, dataset, recurse=False):
        snaps = self.get_snapshots(dataset)
        holds = OrderedDict()
        for s in snaps:
            hold = self.holds(dataset, s, recurse)
            if hold:
                holds[s] = hold

        return holds

    def send(self, dataset, snap, snap1=None, recurse=True):
        cmd = []
        if self.is_zfs(dataset):
            snaps = self.get_snapshots(dataset)

            if snap in snaps:
                cmd.append('zfs')
                cmd.append('send')
                if recurse:
                    cmd.append('-R')

                if not snap1:
                    cmd.append(dataset + '@' + snap)
                else:
                    if snap1 in snaps:
                        cmd.append('-I')
                        cmd.append(dataset + '@' + snap)
                        cmd.append(dataset + '@' + snap1)
                    else:
                        cmd = []
        if cmd:
            p = Popen(cmd, stdout=PIPE)
            return p
        else:
            return None

    def get_send_size(self, dataset, snap, snap1=None, recurse=True):

        cmd = []
        if self.is_zfs(dataset):
            snaps = self.get_snapshots(dataset)

            if snap in snaps:
                cmd.append('zfs')
                cmd.append('send')
                if recurse:
                    cmd.append('-R')
                cmd.append('-nv')
                if not snap1:
                    cmd.append(dataset + '@' + snap)
                else:
                    if snap1 in snaps:
                        cmd.append('-I')
                        cmd.append(dataset + '@' + snap)
                        cmd.append(dataset + '@' + snap1)
                    else:
                        cmd = []
        if cmd:
            out, err, rc = execute(cmd)
            if rc == 0:
                return self.conv_space(out.splitlines()[-1].split()[-1])
            else:
                return None
        else:
            return None


    def recv(self, dataset, force=True):
        cmd = []
        cmd.append('zfs')
        cmd.append('recv')
        cmd.append(dataset)
        if force:
            cmd.append('-F')
        if cmd:
            p = Popen(cmd, stdin=PIPE)
            return p
        else:
            return None

    def recv_pipe(self,fd,dataset,force=True):
        cmd = []
        cmd.append('zfs')
        cmd.append('recv')
        cmd.append(dataset)
        if force:
            cmd.append('-F')
        if cmd:
            p = Popen(cmd, stdin=fd)
            return p
        else:
            return None

    def engociate_inc_send(self, dataset, recv_snapshots=[]):
        init_snap = None
        last_snap = None
        snapshots = []
        if self.is_zfs(dataset):
            snapshots = self.get_snapshots(dataset)
            #print snapshots
            #print inc_snapshots
            if snapshots:
                for snap in recv_snapshots:
                    #print snap
                    if snap in snapshots:
                        init_snap = snap

        if init_snap:
            last_snap = snapshots[-1]

            # if init_snap == last_snap:
            #     init_snap = None
            #     last_snap = None

        return init_snap, last_snap

    def adaptive_send(self, dataset, snap, snap1=None, recurse=True, compression=None, verbose=False, limit=0,
                      time=0,out_fd=None):

        size = self.get_send_size(dataset,snap,snap1,recurse)
        send = self.send(dataset, snap, snap1, recurse)

        if send:


            if compression:
                piper = pv(send.stdout, verbose=verbose, limit=limit, size=size, time=time)
                if out_fd:
                    compr = compressor(piper.stdout, compression,out_fd=out_fd)
                else:
                    compr = compressor(piper.stdout, compression)

                return compr
            else:
                if out_fd:
                    piper = pv(send.stdout, verbose=verbose, limit=limit, size=size, time=time,out_fd=out_fd)
                else:
                    piper = pv(send.stdout, verbose=verbose, limit=limit, size=size, time=time)

                return piper


class zpool():
    def list(self):
        list = []
        out, err, rc = execute(['zpool', 'list', '-H', '-o', 'name'])
        for i in out.splitlines():
            list.append(i)

        return list

    def get(self, zpool, property='all'):
        if zpool:
            out, err, rc = execute(['zpool', 'get', '-H', '-p', property, zpool])
            value = out.split('\t')[2]
            return value
        else:
            list = {}
            out, err, rc = execute(['zpool', 'get', '-H', '-p', property])
            for i in out.splitlines():
                if i.split('\t')[2] != '-':
                    list[i.split('\t')[0]] = i.split('\t')[2]

            return list

    def get_all(self, zpool):
        out, err, rc = execute(['zpool', 'get', '-H', '-p', 'all', zpool])
        list = {}
        for i in out.splitlines():
            list[i.split('\t')[1]] = i.split('\t')[2]

        return list

    def set(self, zpool, property, value):

        out, err, rc = execute(['zpool', 'set', property + '=' + value, zpool])

        return rc

    def start_scrub(self, zpool):
        out, err, rc = execute(['zpool', 'scrub', zpool])
        return rc

    def stop_scrub(self, zpool):
        out, err, rc = execute(['zpool', 'scrub', '-s', zpool])
        return rc

    def clear(self, zpool, drive=None):
        if drive:
            out, err, rc = execute(['zpool', 'clear', zpool, drive])
        else:
            out, err, rc = execute(['zpool', 'clear', zpool])

        return rc

    def zimport(self, zpool=None, force=False, mount=False, persist='id'):

        com = ['zpool', 'import']
        if zpool:
            com.append(zpool)
        else:
            com.append('-a')

        if force:
            com.append('-f')

        if not mount:
            com.append('-N')

        if persist == 'path':
            com.append('-d')
            com.append('/dev/disk/by-path')

        elif persist == 'id':
            com.append('-d')
            com.append('/dev/disk/by-id')

        else:
            com.append('-d')
            com.append(persist)

        out, err, rc = execute(com)
        return rc


class zdb:
    def __init__(self):
        self.zpools = {}
        out, err, rc = execute(['zdb'])
        self.lines = out.splitlines()
        for line in self.lines:
            m = re.match('(\S+):', line)
            if m:
                name = m.group(1)
                self.zpools[name] = []
            else:
                self.zpools[name].append(line)

    def get_zpools(self):

        return self.zpools.keys()

    def get_drives(self, zpool):
        if zpool in self.zpools.keys():
            disks = []
            for line in self.zpools[zpool]:
                m = re.match("\s*path: '(\S+)'", line)
                if m:
                    disk = m.group(1)

                w = re.match("\s*whole_disk: (\d+)", line)
                if w:
                    if int(w.group(1)):
                        if 'part' in disk:
                            disk = disk[:-6]
                        else:
                            disk = disk[:-1]
                    else:

                        pass

                    disks.append(disk)
        else:
            disks = []

        return disks

    def get_guid(self, zpool, drive):
        if zpool in self.zpools.keys():
            for line in self.zpools[zpool]:
                m = re.match('\s*guid: (\d+)', line)
                if m:
                    guid = m.group(1)

                m = re.match("\s*path: '%s'" % drive, line)
                if m:
                    return guid
        else:
            return None

    def get_zpool_state(self, zpool):
        status, err, rc = execute(['zpool', 'status', zpool, '-v'])
        if rc == 0:
            for line in status.splitlines():
                m = re.match('\s*%s\s*(\S+)' % zpool, line)
                if m:
                    state = m.group(1)
                    return state

    def get_disk_state(self, zpool, drive):
        status, err, rc = execute(['zpool', 'status', zpool, '-v'])
        if rc == 0:
            for line in status.splitlines():
                m = re.match('\s*%s\s*(\S+)' % drive, line)
                if m:
                    state = m.group(1)
                    break
                else:
                    state = 'UNAVAIL'
            return state

    def is_missing(self, zpool):
        missing = False
        status, err, rc = execute(['zpool', 'status', zpool, '-v'])
        if rc == 0:
            for line in status.splitlines():
                if 'UNAVAIL' in line:
                    missing = True
            return missing

    def get_missing(self, zpool):
        guid = None
        status, err, rc = execute(['zpool', 'status', zpool, '-v'])
        if rc == 0:
            for line in status.splitlines():
                m = re.match('\s*(\d*)\s* UNAVAIL', line)
                if m:
                    guid = m.group(1)

        return guid

    def heal(self, zpool, old_disk, new_disk):

        pass

    def replace(self, zpool, old_disk, new_disk):
        out, err, rc = execute(['zpool', 'replace', zpool, old_disk, new_disk, '-f'])
        if rc != 0:
            if 'is part of active pool' in err:
                rc == 2
        return rc

    def is_resilvering(self, zpool):
        status, err, rc = execute(['zpool', 'status', zpool, '-v'])
        if rc == 0:
            line = 'action: Wait for the resilver to complete.'
            if line in status.splitlines():
                return True
            else:
                return False


if __name__ == '__main__':
    import time

    zf = zfs()
    zp = zpool()

    zf.restore_files('archive/video/20.106/190601','videoserver',['/archive/video/20.106/190601/02/0202-00122.mp4',])
