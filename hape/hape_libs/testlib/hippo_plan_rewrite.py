
from hape_libs.config.hippo_config import CarbonHippoPlanParser

class HippoPlanRewrite:
    def __init__(self, hippo_plan):
        self._hippo_plan_parser = CarbonHippoPlanParser(hippo_plan)
    
    
    def rewrite_ports(self, ports):
        assert(len(ports) == 4)
        self._hippo_plan_parser.filter_args_with_value_prefix("arpc")["value"] = "arpc:" + str(ports[0])
        self._hippo_plan_parser.filter_args_with_value_prefix("http")["value"] = "http:" + str(ports[1])
        self._hippo_plan_parser.filter_args_with_value_prefix("httpPort")["value"] = "httpPort=" + str(ports[2])
        self._hippo_plan_parser.filter_args_with_value_prefix("gigGrpcPort")["value"] = "gigGrpcPort=" + str(ports[3])
        self._hippo_plan_parser.filter_args_with_value_prefix("port")["value"] = "port=" + str(ports[0])
        
        self._hippo_plan_parser.filter_health_port()["value"] = str(ports[2])
        