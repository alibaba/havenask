import argparse
import os

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--out", help="out", action="store")
    args = parser.parse_args()

    outs = args.out
    output = os.popen('/usr/bin/realpath %s' % __file__).read()

    path = os.path.abspath(os.path.join(os.path.dirname(output), '../..'))
    cmd = 'GIT_DIR=%s/.git GIT_WORK_TREE=%s git log -10 > %s' % (path, path, outs)
    # with open('/home/jiaming.tjm/tmp.xxx', 'w') as f:
    #     f.write("\noutput " + output)
    #     f.write("\nout " + outs)
    #     f.write("\npath " + path)
    #     f.write("\ncmd " + cmd)
    os.system(cmd)
