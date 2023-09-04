#!/bin/env python

import base_cmd
import swift_admin_delegate
import swift_common_define

class SwiftStopCmd(base_cmd.BaseCmd):
    '''
    swift {sp|stop}
       {-z zookeeper_address  | --zookeeper=zookeeper_address}

    options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address

Example:
    swift sp -z zfs://10.250.12.23:1234/root 
    '''

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate()
        return True
            
    def run(self):
        ret, adminLeader = self.getAdminLeaderAddr()
        if not ret:
            return "", "", 0

        ret, errorMsg = self.adminDelegate.stopSys(adminLeader)
        if not ret:
            print "ERROR: stop swift system failed!"
            print errorMsg
            return "", "", 1
        
        print "Stop swift system success!"
        print "You can use getstatus cmd to check whether swift system really be stopped."
        return "", "", 0

        
