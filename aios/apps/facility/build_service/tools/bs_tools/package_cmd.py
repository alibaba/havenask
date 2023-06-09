import os
import base_cmd
import pm_run_delegate
import build_app_config

class PackageCmdBase(base_cmd.BaseCmd):
    def __init__(self):
        super(PackageCmdBase, self).__init__()

    def addOptions(self):
        self.parser.add_option('-c', '--config', action='store', dest='configPath')

    def initMember(self, options):
        super(PackageCmdBase, self).initMember(options)
        self.baseInitMember(options)

    def baseInitMember(self, options):
        lastConfig = self.getLatestConfig(options.configPath)
        self.buildAppConfig = build_app_config.BuildAppConfig(
            self.fsUtil, lastConfig)
        self.pm = pm_run_delegate.PmRun(self.toolsConf.pm_run_exe,
                                        self.buildAppConfig.nuwaAddress)

    def checkOptionsValidity(self, options):
        if options.configPath is None:
            raise Exception("ERROR: configPath[-c] must be specified.")

class AddPackageCmd(PackageCmdBase):
    """
    bs {addpackage|ap}
        {-c config_path      | --config=config_path}
        [-l local_package_path  | --localpackage=local_package_path]
        [-t package_type        | --packageType=package_type]
Option:
    -c config_path, --config=config_path   : required
    -l local_package, --localpackage=local_package : optional, if not specified,
                                                     will use local_package_path
                                                     configed in tools_conf.py
    -t package_type, --packageType=package_type    : optional, hadoop or apsara,
                                                     default will be apsara
    """
    def __init__(self):
        super(AddPackageCmd, self).__init__()

    def addOptions(self):
        super(AddPackageCmd, self).addOptions()
        self.parser.add_option('-l', '--localpackage', action='store',
                               dest='localPackage')
        self.parser.add_option('-t', '--packageType', type='choice', action='store',
                               dest='packageType', choices=['apsara', 'hadoop'],
                               default='apsara')

    def checkOptionsValidity(self, options):
        super(AddPackageCmd, self).checkOptionsValidity(options)
        if options.localPackage is None:
            raise Exception("localpackage[-l] must be specified")
        if not os.path.isfile(options.localPackage):
            raise Exception("localPackage[%s] not exist or is not file" \
                % options.localPackage)

    def initMember(self, options):
        super(AddPackageCmd, self).initMember(options)
        self.localPackage = options.localPackage
        self.packageType = options.packageType

    def run(self):
        if self.packageType == "apsara":
            return self.addApsaraPackage()
        else:
            return self.addHadoopPackage()

    def addApsaraPackage(self):
        packageName = self.buildAppConfig.apsaraPackageName
        self.pm.addPackage(packageName, self.localPackage)
        print "INFO: add apsara package success!"

    def addHadoopPackage(self):
        remotePackagePath = self.buildAppConfig.hadoopPackageName
        if not remotePackagePath:
            raise Exception("WARN: hadoop_package_name has not been configurated")
        if self.fsUtil.exists(remotePackagePath):
            self.fsUtil.remove(remotePackagePath)
        self.fsUtil.copy(self.localPackage, remotePackagePath)
        print "INFO: add hadoop package success!"

class RemovePackageCmd(PackageCmdBase):
    """
    bs {removepackage|rp}
        {-c config_path      | --config=config_path}
Option:
    -c config_path, --config=config_path   : required

    """
    def run(self):
        packageName = self.buildAppConfig.apsaraPackageName
        self.pm.removePackage(packageName)
        print "INFO: remove package success!"

class ListPackageCmd(PackageCmdBase):
    """
    bs {listpackage|lp}
        {-c config_path      | --config=config_path}
Option:
    -c config_path, --config=config_path   : required

    """
    def run(self):
        out, error, code = self.pm.listPackage()
        if code != 0:
            raise Exception("ERROR: failed to list package from nuwa[%s], error[%s], stdout[%s]" \
                % (self.buildAppConfig.nuwaAddress, error, out))
        print "INFO: get package from nuwa[%s]..." % self.buildAppConfig.nuwaAddress
        print out
