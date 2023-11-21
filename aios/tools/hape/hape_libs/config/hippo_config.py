from hape_libs.utils.logger import Logger

class CarbonHippoPlanParser:
    def __init__(self, carbon_hippo_plan):
        self._carbon_hippo_plan = carbon_hippo_plan
    
    def parse_args_env(self, key):
        args = self._carbon_hippo_plan["role_desc"]["processInfos"][0]["args"]
        for arg_dict in args:
            if arg_dict["key"] == "--env" and arg_dict["value"].startswith(key+"="):
                return arg_dict["value"].split("=")[1]
            
    def parse_count(self):
        return int(self._carbon_hippo_plan["role_desc"]["count"])   
    
    def filter_args_with_value_prefix(self, prefix):
        args = self._carbon_hippo_plan["role_desc"]["processInfos"][0]["args"]
        for arg_dict in args:
            if arg_dict["value"].startswith(prefix):
                return arg_dict
            
    def filter_health_port(self):
        args = self._carbon_hippo_plan["role_desc"]["healthCheckerConfig"]["args"]
        for arg_dict in args:
            if arg_dict["key"] == "PORT":
                return arg_dict
            
        
    