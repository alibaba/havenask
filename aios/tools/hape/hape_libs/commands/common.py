import click 
import functools
import logging

from hape_libs.utils.logger import Logger
from hape_libs.config import DomainConfig
from hape_libs.domain import HavenaskDomain

def common_params(func):
    @click.option("-c", "--config", help="Path of hape config. Will use default if not specified", required=False)
    @click.option("-v", "--verbose", help="Enable debug level logging", default=False, is_flag=1, required=False)
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

def command_init(kwargs, logging_level = None): #type: (dict, int)->HavenaskDomain
    if kwargs['verbose'] == True:
        Logger.init_stdout_logger(logging.DEBUG)
    else:
        if logging_level == None:
            Logger.init_stdout_logger(logging.INFO)
        else:
            Logger.init_stdout_logger(logging_level)
        
    domain_config = DomainConfig(kwargs['config'])
    havenask_domain = HavenaskDomain(domain_config)
    return havenask_domain