# -*- coding: utf-8 -*-

from base_cmd import BaseCmd
from build_service.proto.Admin_pb2 import *
from vulcan.protocol.Service_pb2 import *
from vulcan.protocol.VulcanCommon_pb2 import *
import arpc.python.rpc_impl_async as async_rpc_impl
import build_app_config
import admin_locator
import vulcan_msg_converter
import os

class AdminCmdBase(BaseCmd):
    def __init__(self):
        super(AdminCmdBase, self).__init__()

    def addOptions(self):
        self.parser.add_option('-c', '--config', action='store', dest='configPath')
        self.parser.add_option('-a', '--app_name', action='store', dest='appName', default='')
        #self.parser.add_option('-m', '--app_mode', action='store', dest='appMode', default='BS_APP')
        self.parser.add_option('', '--timeout', type='int', action='store',
                               dest='timeout', default=30)

    def checkOptionsValidity(self, options):
        if not options.configPath:
            raise Exception('-c config_path is needed.')

    def getAppMode(self):
        appModeStr = os.getenv('APP_MODE')
        if appModeStr == "BS_APP" or appModeStr == "VULCAN_APP":
            return appModeStr
        return "BS_APP"

    def initMember(self, options):
        self.configPath = options.configPath
        self.timeout = options.timeout
        self.appMode = self.getAppMode()
        if options.appName == '' and self.configPath:
            buildAppConfig = build_app_config.BuildAppConfig(
                self.fsUtil, self.getLatestConfig(self.configPath))
            self.appName = buildAppConfig.userName + '_' + buildAppConfig.serviceName
        else:
            self.appName = options.appName
        self.appName = self.appName.replace('.', '_')

    def getAdminAddress(self):
        if hasattr(self, 'zkRoot') and self.zkRoot:
            adminAddr = admin_locator.getAdminAddressFromZk(self.zkRoot, self.appMode)
        else:
            adminAddr = admin_locator.getAdminAddress(
                self.getLatestConfig(self.configPath), self.appMode,
                self.fsUtil, self.timeout)
        return adminAddr

    # always return a valid response
    # raise Exception when call failed
    def callWithTimeout(self, method, request, adminAddr, timeout):
        if self.appMode == "VULCAN_APP":
            return self.callVulcanApp(method, request, adminAddr, timeout)
        else:
            return self.callBSApp(method, request, adminAddr, timeout)

    def callVulcanApp(self, method, request, adminAddr, timeout):
        converter = vulcan_msg_converter.VulcanMsgConverter();
        vulcanMethod, vulcanRequest = converter.convertToVulcanRequest(method, request)
        stub = async_rpc_impl.ServiceStubWrapperFactory.createServiceStub(
            Service_Stub, adminAddr)
        reply = stub.asyncCall(vulcanMethod, vulcanRequest)
        try:
            reply.wait(timeout)
        except async_rpc_impl.ReplyTimeOutError:
            raise Exception('arpc call timeout, admin address[%s].' % adminAddr)

        response = reply.getResponse()
        if not response:
            raise Exception('admin not response.')
        bsResponse = converter.convertToBSResponse(request.buildId, adminAddr, method, response)
        return bsResponse

    def callBSApp(self, method, request, adminAddr, timeout):
        stub = async_rpc_impl.ServiceStubWrapperFactory.createServiceStub(
            AdminService_Stub, adminAddr)
        reply = stub.asyncCall(method, request)

        try:
            reply.wait(timeout)
        except async_rpc_impl.ReplyTimeOutError:
            raise Exception('arpc call timeout, admin address[%s].' % adminAddr)

        response = reply.getResponse()
        if not response:
            raise Exception('admin not response.')
        if response.errorMessage:
            raise Exception('%s failed error [%s]' % (method, response.errorMessage))

        return response

    # always return a valid response
    # raise Exception when call failed or admin return error
    def call(self, method):
        request = self.createRequest()
        assert(request)
        adminAddr = self.getAdminAddress()
        if not adminAddr:
            raise Exception('admin not started')
        response = self.callWithTimeout(method=method, request=request,
                                        adminAddr=adminAddr, timeout=self.timeout)
        if not response:
            raise Exception('admin not responce, maybe timeout')
        return response
