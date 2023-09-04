import os
import sys
import traceback

def main():
    try:
        print(os.environ);
        from bs_tools.process import Process
        process = Process()

        indexRoot=sys.argv[1]
        cpIndexRoot=indexRoot+"/python_index"

        cmd = "fs_util rm %s" % cpIndexRoot
        process.run(cmd)
        print(cmd)        

        cmd = "fs_util ls %s" % indexRoot
        data, err, code = process.run(cmd)
        print(data)
        dirs=data.split("\n")

        for i in range(0, len(dirs)):
            dirPath=indexRoot+"/"+dirs[i]
            cmd = "fs_util cp -r %s %s" % (dirPath, cpIndexRoot)
            print(cmd)
            process.run(cmd)
        return True
    
    except Exception, e:
        print e
        print traceback.print_exc()
        return False

if __name__ == '__main__':
    if main():
        sys.exit(0)
    else:
        sys.exit(-1)
                    
