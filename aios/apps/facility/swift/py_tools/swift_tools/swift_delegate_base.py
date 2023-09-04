import swift_common_define
import swift.protocol.ErrCode_pb2 as swift_proto_errcode


class SwiftDelegateBase(object):
    # return (True, None) or (False, errorMsg)
    def __init__(self, username=None, passwd=None, accessId=None, accessKey=None):
        self.username = "" if username is None else username
        self.passwd = "" if passwd is None else passwd
        self.accessId = "" if accessId is None else accessId
        self.accessKey = "" if accessKey is None else accessKey

    def _checkErrorInfo(self, response, serverSpec):
        if not response:
            return False, "ERROR: response is Empty!"

        if not response.HasField(swift_common_define.PROTO_ERROR_INFO):
            errorMsg = "Response from server[%(server_spec)s] must have field[%(field)s]!" % \
                {"server_spec": serverSpec,
                 "field": swift_common_define.PROTO_ERROR_INFO,
                 }
            return False, errorMsg

        errorInfo = response.errorInfo
        if not errorInfo.HasField(swift_common_define.PROTO_ERROR_CODE):
            errorMsg = "Field[%(error_info)s] of response from server[%(server_spec)s] must have field[%(error_code)s]!" %\
                {"error_info": swift_common_define.PROTO_ERROR_INFO,
                 "server_spec": serverSpec,
                 "error_code": swift_common_define.PROTO_ERROR_CODE,
                 }
            return False, errorMsg

        if errorInfo.errCode != swift_proto_errcode.ERROR_NONE:
            errorMsg = "ErrorCode[%(err_code)d], ErrorMsg[%(err_msg)s]" % \
                {"err_code": errorInfo.errCode,
                 "err_msg": errorInfo.errMsg,
                 }
            return False, errorMsg

        return True, None
