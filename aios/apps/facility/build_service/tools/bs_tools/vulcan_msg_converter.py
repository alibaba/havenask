# -*- coding: utf-8 -*-

from build_service.proto.Admin_pb2 import *
from vulcan.protocol.VulcanCommon_pb2 import *
from include.protobuf_json import pb2json
from include.protobuf_json import json2pb
import json

class VulcanMsgConverter:
    def __init__(self):
        pass
    
    def getJobId(self, buildId):
        return buildId.appName + "." + buildId.dataTable + "." + str(buildId.generationId)
    
    def convertToVulcanRequest(self, requestMethod, request):
        if requestMethod == 'startBuild':
            return "create", self.convertStartBuildRequest(request)
        if requestMethod == 'stopBuild':
            return "update", self.convertStopBuildRequest(request)
        if requestMethod == 'updateConfig':
            return "update", self.convertUpdateConfigRequest(request)
        if requestMethod == 'getServiceInfo':
            return "read", self.convertGetServiceInfoRequest(request)
        return "update", self.convertOtherRequest(requestMethod, request)

    def convertToBSResponse(self, buildId, adminAddress, requestMethod, response):
        if requestMethod == "getServiceInfo":
            return self.convertGetServiceInfoResponse(adminAddress, response)
        if requestMethod == "startBuild":
            return self.convertStartBuildResponse(buildId, response)
        #other response ignore
        return response

    def convertStartBuildResponse(self, buildId, response):
        startBuildResponse = StartBuildResponse()
        if response.status == 202:
            startBuildResponse.generationId = buildId.generationId
        return startBuildResponse
            
        
    def convertGetServiceInfoResponse(self, adminAddress, vulcanResponse):
        jobInfos = json.loads(vulcanResponse.msg)
        response = ServiceInfoResponse()
        response.adminAddress = adminAddress
        for jobInfo in jobInfos:
            if (jobInfo["info"] == ""):
                continue
            generationInfo = json.loads(jobInfo["info"])
            json2pb(response.generationInfos.add(), generationInfo)
        return response
        
    def convertGetServiceInfoRequest(self, request):
        vulcanRequest = Request()
        vulcanRequest.path = "/jobs"
        vulcanRequest.property = json.dumps(pb2json(request))
        return vulcanRequest

    def convertOtherRequest(self, requestMethod, request):
        vulcanRequest = Request()
        vulcanRequest.path = "/jobs/" + self.getJobId(request.buildId) + "/cmd/" + requestMethod
        vulcanRequest.property = json.dumps(pb2json(request))
        return vulcanRequest

    def convertUpdateConfigRequest(self, request):
        vulcanRequest = Request()
        vulcanRequest.path = "/jobs/" + self.getJobId(request.buildId) + "/configs"
        configInfo = { "config_path": request.configPath }
        vulcanRequest.property = json.dumps(configInfo)
        return vulcanRequest

    def convertStopBuildRequest(self, request):
        vulcanRequest = Request()
        vulcanRequest.path = "/jobs/" + self.getJobId(request.buildId) + "/stop"
        return vulcanRequest

    def convertStartBuildRequest(self, request):
        vulcanRequest = Request()
        vulcanRequest.path = "/jobs"
        info = {}
        from include.protobuf_json import pb2json
        info["job_id"] = self.getJobId(request.buildId)
        info["config_path"] = request.configPath
        info["description"] = json.dumps(pb2json(request))
        vulcanRequest.property = json.dumps(info)
        return vulcanRequest
