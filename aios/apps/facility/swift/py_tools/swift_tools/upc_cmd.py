import base_cmd
import swift_admin_delegate


class UpdateBrokerConfigCmd(base_cmd.BaseCmd):
    '''
    swift {ubc|updateBrokerConfig}
    {-z zookeeper_address           | --zookeeper=zookeeper_address }
    {-v config_path                 | --configPath=config_path}

Examples:
    swift ubc -z zfs://10.250.12.23:1234/root -v zfs://10.250.12.23:1234/root/config/1111
    '''

    def addOptions(self):
        super(UpdateBrokerConfigCmd, self).addOptions()
        self.parser.add_option('-v', '--configVersionPath', action='store', dest='configVersionPath')

    def initMember(self, options):
        super(UpdateBrokerConfigCmd, self).initMember(options)
        self.configVersionPath = options.configVersionPath

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        if self.configVersionPath is None:
            self.configVersionPath = self.getLatestConfig(self.configPath)
        ret, response, errorMsg = self.adminDelegate.updateBrokerConfig(self.configVersionPath)
        if not ret:
            print "updateBrokerConfig failed"
            print errorMsg
            return ret, response, 1
        return ret, response, 0


class RollbackBrokerConfigCmd(base_cmd.BaseCmd):
    '''
    swift {rbc|rollbackBrokerConfig}
    {-z zookeeper_address           | --zookeeper=zookeeper_address }

Examples:
    swift rbc -z zfs://10.250.12.23:1234/root
    '''

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.rollbackBrokerConfig()
        if not ret:
            print "rollbackBrokerConfig failed"
            print errorMsg
            return ret, response, 1
        return ret, response, 0
