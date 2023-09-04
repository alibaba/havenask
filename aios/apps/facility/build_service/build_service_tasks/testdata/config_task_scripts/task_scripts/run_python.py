import os
import sys
import traceback

def main():
    try:
        print(os.environ);
        
        # test arguments
        args = []
        for i in range(1, len(sys.argv)) :
            args.append(sys.argv[i])
        print("test arguments: %s" % ' '.join(args))
        print("BS_TASK_PARTITION_ID = "+ os.getenv("BS_TASK_PARTITION_ID"))
        print("BS_TASK_INSTANCE_ID = "+ os.getenv("BS_TASK_INSTANCE_ID"))

        # test import module from _external
        #import swift_tools.swift_util
        #print("test import swift module ok")

        # test import module from script root
        import test_module.test_tool
        print("test import test module ok")

        # test run binary from _external/bin
        '''
        cmd = 'fs_util ls /'
        from bs_tools.process import Process
        process = Process()
        data, err, code = process.run(cmd)
        if (code == 0):
            #print(data)
            print("test binary: fs_util ok")
        '''

        import commands
        cmd = 'test_link_bin'
        code, data = commands.getstatusoutput(cmd)
        if (code == 0):
            #print(data)
            print("test third-part binary: test_link ok")
            
        return True

        '''
        # test run binary from script root
        from bs_tools.process import Process
        process = Process()
        cmd = 'test_link_bin'
        data, err, code = process.run(cmd)
        if (code == 0):
            #print(data)
            print("test third-part binary: test_link ok")
        
        return True
        '''
    except Exception, e:
        print e
        print traceback.print_exc()
        return False

if __name__ == '__main__':
    if main():
        sys.exit(0)
    else:
        sys.exit(-1)
