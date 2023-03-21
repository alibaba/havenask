class Target:
    USER_TARGET_TYPE = 'user-target'
    FINAL_TARGET = 'final-target'
    HEARTBEAT = "heartbeat"
    def __init__(self, type, plan, domain_name, role_name, worker_name=None, command=None, host_init=None):
        self.plan = plan
        self.type = type
        self.domain_name = domain_name
        self.role_name = role_name
        self.worker_name = worker_name
        self.user_cmd = command
        self.host_init = host_init
    
    def to_dict(self):
        dict_obj = {
            "plan": self.plan,
            "type": self.type,
            "domain_name": self.domain_name,
            "role_name": self.role_name,
            "worker_name": self.worker_name,
            "user_cmd": self.user_cmd,
            "host_init": self.host_init
        }
        return dict_obj