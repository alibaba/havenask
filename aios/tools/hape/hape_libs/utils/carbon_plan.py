import attr

@attr.s
class Resource(object):
    amount = attr.ib(default=attr.NOTHING)
    name = attr.ib(default=attr.NOTHING)
    type = attr.ib(default=attr.NOTHING)
    
@attr.s
class FullResource(object):
    resources  = attr.ib(converter=lambda rs: [Resource(**r) for r in rs]) #type: Resource
 
@attr.s
class PackageInfo(object):
    packageURI = attr.ib(default=attr.NOTHING)
    type = attr.ib(default=attr.NOTHING)

@attr.s
class KeyValuePair(object):
    key = attr.ib(default=attr.NOTHING)
    value = attr.ib(default=attr.NOTHING)

@attr.s
class Processes(object):
    args = attr.ib(converter=lambda arg_list: [KeyValuePair(**arg) for arg in arg_list]) #type: list[KeyValuePair]
    envs = attr.ib(converter=lambda env_list: [KeyValuePair(**env) for env in env_list]) #type: list[KeyValuePair]
    cmd = attr.ib(default=attr.NOTHING)
    isDaemon = attr.ib(default=attr.NOTHING)
    processName = attr.ib(default=attr.NOTHING)
    
    
@attr.s
class HealthCheckerConfig(object):
    args = attr.ib(converter=lambda arg_list: [KeyValuePair(**arg) for arg in arg_list]) #type: list[KeyValuePair]
    name = attr.ib()
    
@attr.s 
class RoleDesc(object):
    containerConfigs = attr.ib()
    minHealthCapacity = attr.ib()
    count = attr.ib()
    healthCheckerConfig = attr.ib(converter = lambda h: HealthCheckerConfig(**h)) #type: HealthCheckerConfig
    resources  = attr.ib(converter=lambda rs: [FullResource(**r) for r in rs]) #type: FullResource
    packageInfos = attr.ib(converter=lambda rs: [PackageInfo(**r) for r in rs]) #type: list[PackageInfo]
    processInfos = attr.ib(converter=lambda rs: [Processes(**r) for r in rs]) #type: list[Processes]
    
@attr.s
class CarbonPlan(object):
    role_desc = attr.ib(converter=lambda r: RoleDesc(**r)) #type: RoleDesc
    
    def rewrite_args(self, key, value):
        for arg in self.role_desc.processInfos[0].args:
            if arg.key == key:
                arg.value = value
                
                
    def rewrite_kkv_args(self, key, subkey, subkey_sep, value):
        for arg in self.role_desc.processInfos[0].args:
            if key == arg.key:
                if arg.value.split(subkey_sep)[0] == subkey:
                    arg.value = subkey + subkey_sep + value
                    break
                
    def rewrite_health_config(self, key, value):
        for arg in self.role_desc.healthCheckerConfig.args:
            if arg.key == key:
                arg.value = value
