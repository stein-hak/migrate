from subprocess import Popen, PIPE
import os

def pv(fd,verbose=False, limit=0, size=0, time=0, shell=False,out_fd=None,file=None):

    if size and time:
        limit_calc = int(float(size)/float(time))
        limit = limit_calc

    if file:
        shell = True

    if shell == True:
        cmd = 'pv '
        if not verbose:
            cmd += '-q '
        if limit:
            cmd += '-L %s ' % str(limit)
        if size:
            cmd += '-s %s ' % str(size)
        if file:
            cmd += ' > %s' % file
    else:
        cmd = ['pv']
        if not verbose:
            cmd.append('-q')
        if limit:
            cmd.append('-L %s' % str(limit))
        if size:
            cmd.append('-s %s' % str(size))


    if cmd:
        if file:
            p = Popen(cmd, stdin=fd, shell=shell)
        else:
            if not out_fd:
                p = Popen(cmd, stdin=fd,stdout=PIPE,shell=shell)
            else:

                p = Popen(cmd,stdin=fd,stdout=out_fd,shell=shell)

        return p

    else:

        return None

def pv_file(file,verbose=False, limit=0, time=0):
    cmd = []
    if os.path.isfile(file):
        size = os.path.getsize(file)

        if time:
            limit_calc = int(float(size) / float(time))
            limit = limit_calc

        cmd = ['pv',file]

        if not verbose:
            cmd.append('-q')
        if limit:
            cmd.append('-L %s' % str(limit))
        if size:
            cmd.append('-s %s' % str(size))


    if cmd:
        p = Popen(cmd, bufsize=128*1024*1024,stdout=PIPE)

        return p

    else:

        return None


