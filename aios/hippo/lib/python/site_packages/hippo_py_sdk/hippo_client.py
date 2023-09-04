#!/bin/env python

import os
import time
import json

from arpc.python.rpc_impl import ANetRpcImpl
from arpc.python.rpc_impl_async import ServiceStubWrapper
from arpc.python.rpc_impl_async import ServiceStubWrapperFactory

from hippo_py_sdk import leader_locator
from hippo_py_sdk.include import message_codec
from hippo_py_sdk.pb_helper import *
from hippo_py_sdk.log import log_info,log_error

from hippo.proto.Slave_pb2 import *
from hippo.proto.Common_pb2 import *
from hippo.proto.Admin_pb2 import *
from hippo.proto.Client_pb2 import *
from hippo.proto.ErrorCode_pb2 import *
from hippo.proto.ApplicationMaster_pb2 import *
from hippo.proto.WorkerStatus_pb2 import *

import hippo_py_sdk.include.zk_connector as zk_connector

DEFAULT_CPU_RESOURCE_AMOUNT = 100
DEFAULT_MEM_RESOURCE_AMOUNT = 1024    # MB

class HippoClient(object):
    def __init__(self, master_zk_path, printError = True):
        self.master_zk_path = master_zk_path
        self.leader_elector_path = os.path.join(
            self.master_zk_path, 'hippo_election_path')
        self.printError = printError

    def get_leader(self):
        return leader_locator.get_leader(self.leader_elector_path,
                                         verbose = True)

    def get_leader_http(self):
        return leader_locator.get_leader_http(self.leader_elector_path,
                                              verbose = True)

    def get_master_versions(self):
        master_workers = self.get_master_workers()
        master_worker_versions = {}
        for master_worker in master_workers:
            version = self._get_master_worker_version(master_worker)
            if not version:
                return None
            master_worker_versions[master_worker] = version

        return master_worker_versions
        
    def get_master_workers(self, timeout = 30, verbose = False):
        zk_server, sub_path = zk_connector.parse_zk_path(
            self.leader_elector_path)
        my_zk_connector = zk_connector.getZkConnector(zk_server, timeout)
        if my_zk_connector is None:
            if verbose:
                log_error('connect to zookeeper[%s] failed.' % zk_server)
            return None
    
        elector_nodes = leader_locator.get_leader_elector_nodes(
            my_zk_connector, sub_path, timeout, verbose)
        if elector_nodes is None:
            my_zk_connector.close()
            return None
        if not elector_nodes:
            if verbose:
                log_error('Can not find any leader on %s.' % self.leader_elector_path)
            return None
        elector_nodes.sort()

        master_workers = []
        for node in elector_nodes:
            ret, worker_address = my_zk_connector.get(os.path.join(
                    sub_path, node))
            if not ret:
                if verbose:
                    log_error('read leader node failed.')
                my_zk_connector.close()
                return None

            master_workers.append(worker_address)
        
        my_zk_connector.close()
        return master_workers
    
    def allocate_resource(self, request):
        '''ret: bool(success/fail), resouce
        '''
        response = self._rpc_call('ApplicationMasterService', 'allocate', request)
        if not response:
            return False, 'ApplicationMasterService no response'
        else:
            return True, response

    def update_app_resource_preference(self, app_id, preference_desc):
        '''preference_desc = 'slaveaddress,type,leaseMs,tag;slaveaddress,type,leaseMs,tag'
        type = [any, prefer, betternot, prohibit]
        '''
        request = UpdateResourcePreferenceRequest()
        fullfill_update_preference_request(request, app_id, preference_desc)
        response = self._rpc_call('ClientService', 'updateAppResourcePreference', request)
        if not response:
            return False
        else:
            return True

    def get_app_resource_preference(self, app_id):
        request = ResourcePreferenceRequest()
        request.applicationId = app_id
        response = self._rpc_call('ClientService', 'getAppResourcePreference', request)
        if not response:
            return False, 'ClientService no response'
        return True, response
        
    def clear_app_resource_preference(self, app_name):
        ret, response = self.get_app_resource_preference(app_name)
        if not ret:
            return False
        pref_desc = []
        for desc in response.preferenceDesc:
            pref_desc.append("%s,any,0,%s" %
                             (desc.slaveAddress, desc.resourceTag))
        if not pref_desc:
            return True
        ret = self.update_app_resource_preference(app_name, ';'.join(pref_desc))
        if not ret:
            return False
        return True

    def update_queue(self, queue_name, schedule_mode, dynamic_queue_desc,
                     static_queue_desc, lease_ratio = None):
        if dynamic_queue_desc is not None:
            return self.update_dynamic_queue(queue_name, schedule_mode,
                                             dynamic_queue_desc, lease_ratio)
        elif static_queue_desc is not None:
            return self.update_static_queue(queue_name, schedule_mode,
                                            static_queue_desc, lease_ratio)
        else:
            # just update other options
            request = UpdateQueueRequest()
            request.queueDescription.name = queue_name
            if type(schedule_mode) == type(''):
                if schedule_mode.lower() == "spreadout":
                    request.queueDescription.scheduleMode = 0
                elif schedule_mode.lower() == "pileup":
                    request.queueDescription.scheduleMode = 1
            elif schedule_mode is not None:
                request.queueDescription.scheduleMode = schedule_mode

            if lease_ratio is not None:
                request.queueDescription.leaseRatio = int(lease_ratio)
            response = self._rpc_call('ClientService', 'updateQueue', request)
            return self._handle_response_with_errorinfo(response)
    
    def add_queue(self, queue_name, schedule_mode, dynamic_queue_desc,
                  static_queue_desc, lease_ratio = 100):
        if dynamic_queue_desc is not None:
            return self.add_dynamic_queue(queue_name, schedule_mode,
                                          dynamic_queue_desc, lease_ratio)
        else:
            return self.add_static_queue(queue_name, schedule_mode,
                                         static_queue_desc, lease_ratio)

    def del_queue(self, queue_name):
        request = DelQueueRequest()
        request.name = queue_name
        response = self._rpc_call('ClientService', 'delQueue', request)
        return self._handle_response_with_errorinfo(response)             

    def get_queue_status(self, queue_name):
        request = QueueStatusRequest()
        if queue_name is not None:
            request.name = queue_name
        response = self._rpc_call('ClientService', 'getQueueStatus', request)
        return response

    def merge_queue(self, queue_from, queue_to):
        request = MergeQueueRequest()
        request.queueFrom = queue_from
        request.queueTo = queue_to
        response = self._rpc_call('ClientService', 'mergeQueue', request)
        return response

    def list_merged_queues(self):
        request = ListMergedQueuesRequest()
        response = self._rpc_call('ClientService', 'listMergedQueues', request)
        return response
    
    def update_static_queue(self, queue_name, schedule_mode, static_queue_desc,
                            lease_ratio = 100):
        request = UpdateQueueRequest()
        request.queueDescription.name = queue_name
        request.queueDescription.queueType = QueueDescription.STATIC_RESOURCE_QUEUE
        if type(schedule_mode) == type(''):
            if schedule_mode.lower() == "spreadout":
                request.queueDescription.scheduleMode = 0
            elif schedule_mode.lower() == "pileup":
                request.queueDescription.scheduleMode = 1
        elif schedule_mode is not None:
            request.queueDescription.scheduleMode = schedule_mode
                
        if lease_ratio is not None:
            request.queueDescription.leaseRatio = int(lease_ratio)

        queueResource = request.queueDescription.queueResource.add()
        queueResource.minCount = int(static_queue_desc)
        queueResource.maxCount = int(-1)
        
        response = self._rpc_call('ClientService', 'updateQueue', request)
        return self._handle_response_with_errorinfo(response)
    
    def update_dynamic_queue(self, queue_name, schedule_mode, dynamic_queue_desc,
                             lease_ratio = 100):
        request = UpdateQueueRequest()
        request.queueDescription.name = queue_name
        request.queueDescription.queueType = QueueDescription.DYNAMIC_RESOURCE_QUEUE
        if type(schedule_mode) == type(''):
            if schedule_mode.lower() == "spreadout":
                request.queueDescription.scheduleMode = 0
            elif schedule_mode.lower() == "pileup":
                request.queueDescription.scheduleMode = 1
        elif schedule_mode is not None:
            request.queueDescription.scheduleMode = schedule_mode
                
        if lease_ratio is not None:
            request.queueDescription.leaseRatio = int(lease_ratio)
        
        resource_options = dynamic_queue_desc.split(";")
        for option in resource_options:
            queueResource = request.queueDescription.queueResource.add()
            r_tag, r_min, r_max = self.parse_resource_option(option)
            # support default resource group
            if r_tag != "machineType_" :
                resource = queueResource.resources.add()
                resource.type = Resource.TEXT
                resource.name = r_tag
            queueResource.minCount = int(r_min)
            queueResource.maxCount = int(r_max)
            
        response = self._rpc_call('ClientService', 'updateQueue', request)
        return self._handle_response_with_errorinfo(response)
    
        
    def add_static_queue(self, queue_name, schedule_mode, static_queue_desc,
                         lease_ratio = 100):
        request = AddQueueRequest()
        request.queueDescription.name = queue_name
        request.queueDescription.queueType = QueueDescription.STATIC_RESOURCE_QUEUE
        request.queueDescription.scheduleMode = schedule_mode
        request.queueDescription.leaseRatio = lease_ratio
        
        queueResource = request.queueDescription.queueResource.add()
        queueResource.minCount = int(static_queue_desc)
        queueResource.maxCount = int(-1)
            
        response = self._rpc_call('ClientService', 'addQueue', request)
        return self._handle_response_with_errorinfo(response)
    
    def add_dynamic_queue(self, queue_name, schedule_mode, dynamic_queue_desc,
                          lease_ratio = 100):
        request = AddQueueRequest()
        request.queueDescription.name = queue_name
        request.queueDescription.queueType = QueueDescription.DYNAMIC_RESOURCE_QUEUE
        request.queueDescription.scheduleMode = schedule_mode
        request.queueDescription.leaseRatio = lease_ratio
                
        resource_options = dynamic_queue_desc.split(";")
        for option in resource_options:
            queueResource = request.queueDescription.queueResource.add()
            r_tag, r_min, r_max = self.parse_resource_option(option)
            # support default resource group
            if r_tag != "machineType_" :
                resource = queueResource.resources.add()
                resource.type = Resource.TEXT
                resource.name = r_tag
            queueResource.minCount = int(r_min)
            queueResource.maxCount = int(r_max)
        response = self._rpc_call('ClientService', 'addQueue', request)
        return self._handle_response_with_errorinfo(response)        

    # resource_option_str:
    #       A7,10,100 or B2,20,-1 or A8,-1,-1
    def parse_resource_option(self, resource_option_str):
        items = resource_option_str.split(',')
        if (len(items) < 3 or len(items) > 3):
            log_error('invalid resource option:%s' % resource_option_str)
            return None
        r_tag = "machineType_" + items[0]
        r_min = items[1]
        r_max = items[2]
        return r_tag, r_min, r_max

    def fill_configs_request(self, configs, request):
        if configs is None:
            return False
        if type(configs) != dict:
            return False
        for k,v in configs.items():
            if type(k) != str or (type(v) != str and type(v) != int):
                return False
            config = request.configs.add()
            config.key = str(k)
            config.value = str(v)
        return True

    def extract_config_dict(self, obj, err):
        if 'configs' not in obj:
            err = "miss configs field"
            return None
        if type(obj['configs']) != type([]):
            err = "configs field is not list "
            return None
        config_dict = {}
        for oo in obj['configs']:
            if type(oo) != type({}) or oo.get('key', '') == '' or oo.get('value', '') == '':
                err = "invalid config"
                return None
            key = str(oo['key'])
            value = str(oo['value'])
            config_dict[key] = value
        return config_dict
    
    def update_configs_by_file(self, config_file, override = True):
        f = open(config_file)
        content = f.read()
        f.close()
        obj = {}
        try:
            obj = json.loads(content)
        except:
            print "ERROR, invalid json format file"
            return False
        err = ""
        config_dict = self.extract_config_dict(obj, err)
        if not config_dict:
            print "ERROR, extract config dict fail"
            return False
        request = UpdateConfigsRequest()
        if override:
            request.cover = True
        else:
            request.cover = False
        if not self.fill_configs_request(config_dict, request):
            print "ERROR, invalid json format file"
            return False
        response = self._rpc_call('ClientService', 'updateConfigs', request)
        return self._handle_response_with_errorinfo(response)

    def update_configs(self, configs):
        request = UpdateConfigsRequest()
        if not self.fill_configs_request(configs, request):
            print "ERROR, invalid option"
            return False
        response = self._rpc_call('ClientService', 'updateConfigs', request)
        return self._handle_response_with_errorinfo(response)

    def get_configs(self, keys):
        request = GetConfigsRequest()
        for key in keys:
            request.keys.append(key)
        response = self._rpc_call('ClientService', 'getConfigs', request)
        return response

    def del_configs(self, keys, del_all):
        request = DelConfigsRequest()
        if del_all:
            request.all = True
        else:
            for key in keys:
                request.keys.append(key)
        response = self._rpc_call('ClientService', 'delConfigs', request)
        return response

    def update_rules(self, rules_file):
        print "ERROR, this service will not work anymore, please use config update."
        return True

    def get_rules(self):
        print "ERROR, this service will not work anymore, please use config get."
        return True
        
    def update_request_rules(self, request_file):
        if not os.path.exists(request_file):
            log_error('rule request file not exist')
            return False
        request = UpdateMappingRuleRequest()
        json_obj = json.loads(open(request_file).read())
        json2pb(request, json_obj)
        response = self._rpc_call('ClientService', 'updateMappingRule', request)
        return self._handle_response_with_errorinfo(response)

    def update_request_rules_by_request(self, request):
        return self._rpc_call('ClientService', 'updateMappingRule', request)

    def update_request_rules_by_cmdline(self, app_name, tag_name, group,
                                        slot_resource, declare_resource, queue,
                                        cpusetMode, meta_tags, containerconfigs,
                                        priority):
        request = UpdateMappingRuleRequest()
        request.applicationId = app_name
        if tag_name != None:
            request.tag = tag_name

        if group != None:
            request.adjustment.groupId = group

        if queue != None:
            request.adjustment.queue = queue

        if cpusetMode != None:
            request.adjustment.cpusetMode = cpusetMode

        if slot_resource != None:
            new_option = request.adjustment.options.add()
            resources = slot_resource.strip().split(';')
            for resource in resources:
                option = new_option.resources.add()
                option.CopyFrom(self.parse_resource(resource))

        if declare_resource != None:
            resources = declare_resource.strip().split(';')
            for resource in resources:
                new_declare = request.adjustment.declarations.add()
                new_declare.CopyFrom(self.parse_resource(resource))
                
        if meta_tags != None:
            tags = meta_tags.strip().split(';')
            for tag in tags:
                kv = tag.strip().split('=')
                if len(kv) == 2:
                    metatag = request.adjustment.metaTags.add()
                    metatag.key = kv[0]
                    metatag.value = kv[1]

        if containerconfigs != None:
            configs = containerconfigs.strip().split(';')
            for config in configs:
                request.adjustment.containerConfigs.append(config)

        if priority != None:
            pri = priority.strip().split(',')
            if len(pri) >= 1:
                request.adjustment.priority.majorPriority = int(pri[0])
            if len(pri) >= 2:
                request.adjustment.priority.minorPriority = int(pri[1])

        response = self._rpc_call('ClientService', 'updateMappingRule', request)
        return self._handle_response_with_errorinfo(response)
        
    def get_requests_rules(self, app_name, tag_name):
        request = GetMappingRulesRequest()
        if app_name != None:
            request.applicationId = app_name
        if tag_name != None:
            request.tag = tag_name
        response = self._rpc_call('ClientService', 'getMappingRules', request)
        return response

    def del_requests_rules(self, app_name, tag_name):
        request = DelMappingRuleRequest()
        if app_name != None:
            request.applicationId = app_name
        if tag_name != None:
            request.tag = tag_name
        response = self._rpc_call('ClientService', 'delMappingRule', request)
        return self._handle_response_with_errorinfo(response)

    def get_slave_allocate_wmark(self, slave_list = []):
        request = SlaveAllocateWmarkRequest()
        request.slaves.extend(slave_list)
        response = self._rpc_call('ClientService', 'getSlaveAllocateWmark', request)
        return response

    def get_lacking_quota(self, group_id):
        request = QuotaSummaryRequest()
        request.groupId = group_id
        response = self._rpc_call('ClientService', 'getQuotaSummary', request)
        return response
        
    def submit_application_with_pb(self, request):
        self._adjust_app_desc(request.applicationDescription)
        response = self._rpc_call('ClientService', 'submitApplication', request)
        return self._handle_response_with_errorinfo(response)
        
    def submit_application_with_jsonstring(self, json_string):
        request = SubmitApplicationRequest()
        json_obj = json.loads(json_string)
        message_codec.json2pb(request.applicationDescription, json_obj)
        self._adjust_app_desc(request.applicationDescription)
        response = self._rpc_call('ClientService', 'submitApplication', request)
        return self._handle_response_with_errorinfo(response)
        
    def submit_application(self, app_desc_file):
        request = SubmitApplicationRequest()
        json_obj = json.loads(open(app_desc_file).read())
        message_codec.json2pb(request.applicationDescription, json_obj)
        self._adjust_app_desc(request.applicationDescription)
        response = self._rpc_call('ClientService', 'submitApplication', request)
        return self._handle_response_with_errorinfo(response)

    def update_application(self, app_desc_file, force=False):
        request = UpdateApplicationRequest()
        json_obj = json.loads(open(app_desc_file).read())
        message_codec.json2pb(request.applicationDescription, json_obj)
        self._adjust_app_desc(request.applicationDescription)
        request.force = force
        response = self._rpc_call('ClientService', 'updateApplication', request)
        return self._handle_response_with_errorinfo(response)

    def update_app_master(self, app_desc_file):
        request = UpdateAppMasterRequest()
        json_obj = json.loads(open(app_desc_file).read())
        message_codec.json2pb(request.applicationDescription, json_obj)
        self._adjust_app_desc(request.applicationDescription)
        response= self._rpc_call('ClientService', 'updateAppMaster', request)
        return self._handle_response_with_errorinfo(response)
    
    def update_application_with_jsonstring(self, json_string, force=False):
        request = UpdateApplicationRequest()
        json_obj = json.loads(json_string)
        message_codec.json2pb(request.applicationDescription, json_obj)
        self._adjust_app_desc(request.applicationDescription)
        request.force = force
        response = self._rpc_call('ClientService', 'updateApplication', request)
        return self._handle_response_with_errorinfo(response)
    
    def update_application_with_pb(self, request):
        self._adjust_app_desc(request.applicationDescription)
        response = self._rpc_call('ClientService', 'updateApplication', request)
        return self._handle_response_with_errorinfo(response)
    
    def update_app_master_with_jsonstring(self, json_string):
        request = UpdateAppMasterRequest()
        json_obj = json.loads(json_string)
        message_codec.json2pb(request.applicationDescription, json_obj)
        self._adjust_app_desc(request.applicationDescription)
        response= self._rpc_call('ClientService', 'updateAppMaster', request)
        return self._handle_response_with_errorinfo(response)
    
    def update_app_master_with_pb(self, request):
        self._adjust_app_desc(request.applicationDescription)
        response= self._rpc_call('ClientService', 'updateAppMaster', request)
        return self._handle_response_with_errorinfo(response)

    def get_app_description(self, app_name):
        request = GetAppDescriptionRequest()
        request.applicationId = app_name
        response= self._rpc_call('ClientService', 'getAppDescription', request)
        if not response:
            return False, 'ClientService no response'
        return True, response
    
    def stop_application(self, app_name, reserve_time = None):
        request = StopApplicationRequest()
        request.applicationId = app_name
        if reserve_time is not None:
            request.reserveTime = reserve_time
        response = self._rpc_call('ClientService', 'stopApplication', request)
        return self._handle_response_with_errorinfo(response)

    def freeze_application(self, app_name):
        request = FreezeApplicationRequest()
        request.applicationId = app_name
        response = self._rpc_call('ClientService', 'freezeApplication', request)
        return self._handle_response_with_errorinfo(response)

    def freeze_all_application(self):
        request = FreezeAllApplicationRequest()
        response = self._rpc_call('ClientService', 'freezeAllApplication', request)
        return self._handle_response_with_errorinfo(response)
    
    def unfreeze_application(self, app_name):
        request = UnFreezeApplicationRequest()
        request.applicationId = app_name
        response = self._rpc_call('ClientService', 'unfreezeApplication', request)
        return self._handle_response_with_errorinfo(response)

    def unfreeze_all_application(self):
        request = UnFreezeAllApplicationRequest()
        response = self._rpc_call('ClientService', 'unfreezeAllApplication', request)
        return self._handle_response_with_errorinfo(response)

    def freeze_master_schedule(self):
        request = UpdateMasterOptionsRequest()
        request.scheduleSwitch = False
        response = self._rpc_call('ClientService', 'updateMasterScheduleSwitch', request)
        return self._handle_response_with_errorinfo(response)

    def unfreeze_master_schedule(self):
        request = UpdateMasterOptionsRequest()
        request.scheduleSwitch = True
        response = self._rpc_call('ClientService', 'updateMasterScheduleSwitch', request)
        return self._handle_response_with_errorinfo(response)

    def update_resource_score_table(self, queue_name, resource_name, score_table):
        request = UpdateResourceScoreTableRequest()
        request.queueName = queue_name
        request.resourceName = resource_name
        for score in score_table:
            request.scoreTable.append(score)
        response = self._rpc_call('ClientService',
                                  'updateResourceScoreTable', request)
        return self._handle_response_with_errorinfo(response)

    def delete_queue_score_table(self, queue_name):
        request = DeleteResourceScoreTableRequest()
        request.queueName = queue_name
        response = self._rpc_call('ClientService',
                                  'deleteResourceScoreTable', request)

        if response is not None :
            response_json = {}
            message_codec.pb2json(response_json, response)
            log_info(json.dumps(response_json, indent = 4))
        return self._handle_response_with_errorinfo(response)

    def list_queue_score_table(self):
        request = ListResourceScoreTableRequest()
        response = self._rpc_call('ClientService',
                                  'listResourceScoreTable', request)
        return response

    def kill_application(self, app_name):
        request = ForceKillApplicationRequest()
        request.applicationId = app_name
        response = self._rpc_call('ClientService', 'forceKillApplication', request)
        return self._handle_response_with_errorinfo(response)

    def clear_yarn_appId(self, app_name):
        request = ClearYarnApplicationIdRequest()
        request.applicationId = app_name
        response = self._rpc_call('ClientService', 'clearYarnApplicationId', request)
        return self._handle_response_with_errorinfo(response)

    def get_slave_status(self, slave_list = [], detail = False, showAllRes = False):
        request = SlaveStatusRequest()
        for slave in slave_list:
            request.slaves.append(slave)
        request.withDetailInfo = detail
        request.showAllRes = showAllRes
        return self._rpc_call('ClientService', 'getSlaveStatus', request)

    def get_app_summary(self):
        request = AppSummaryRequest()
        return self._rpc_call('ClientService', 'getAppSummary', request)

    def get_app_status(self, app_name, only_request = False):
        request = AppStatusRequest()
        request.applicationId = app_name
        if only_request:
            request.onlyRequest = only_request
        return self._rpc_call('ClientService', 'getAppStatus', request)
    
    def get_app_raw_request(self, app_name):
        request = GetRawRequestsRequest()
        request.appId = app_name
        return self._rpc_call('ClientService', 'getLastRawRequests', request)

    def get_app_reserved_slots(self, app_name, tag):
        request = GetReserveSlotsRequest()
        if app_name:
            request.appId = app_name
        if tag:
            request.tag = tag
        return self._rpc_call('ClientService', 'getReserveSlots', request)

    def clear_app_reserved_slots(self, app_name, tag, slot_list):
        target_slot = None
        request = ClearReserveSlotsRequest()
        request.appId = app_name
        if tag:
            request.tag = tag
        if slot_list:
            target_slot = self._normalize_slot_list(
                slot_list, self.hippo_config.slave_port)
        if target_slot:
            for slot in target_slot:
                slot_id = request.slots.add()
                slot_id.slaveAddress = slot[0] + ":" + slot[1]
                slot_id.id = int(slot[2])
        response = self._rpc_call('ClientService', 'clearReserveSlots', request)
        return self._handle_response_with_errorinfo(response)

    def get_service_status(self):
        request = ServiceStatusRequest()
        return self._rpc_call('ClientService', 'getServiceStatus', request)

    def get_all_tags(self, app_name):
        app_status = self.get_app_status(app_name)
        tag_list = []
        for response in app_status.lastAllocateResponse:
            tag_list.append(response.resourceTag)

        return tag_list

    def get_all_apps(self):
        app_list = []
        app_summary = self.get_app_summary()
        for app in app_summary.appSummary:
            app_list.append(app.applicationId)
        return app_list
    
    def get_app_checksum(self, app_status):
        if app_status == None:
            return None
        if app_status.appSummary == None:
            return None
        if app_status.appSummary.appChecksum == None:
            return None
        return long(app_status.appSummary.appChecksum)

    #appChecksum in launch_request should be set
    def launch_process(self, slave_spec, launch_request, timeout = 5):
        slave_stub = ServiceStubWrapperFactory.createServiceStub(
            SlaveService_Stub, slave_spec)
        response = slave_stub.syncCall("startProcess", launch_request, timeout)
        return response

    def get_slave_slot_details_async(self, slave_spec):
        get_detail_request = GetSlotDetailsRequest()
        stub = ServiceStubWrapperFactory.createServiceStub(
            SlaveService_Stub, slave_spec)
        if stub == None:
            return None
        return (stub.asyncCall("GetSlotDetailsRequest", get_detail_request), stub)

    def rebind_slave_cpuset_async(self, slave_spec):
        rebind_request = RebindAllSlotCpusetRequest()
        stub = ServiceStubWrapperFactory.createServiceStub(
            SlaveService_Stub, slave_spec)
        if stub == None:
            return None
        return (stub.asyncCall("rebindAllSlotCpuset", rebind_request), stub)

    def stop_slave_async(self, slave_spec, cleanup = False):
        stop_request = StopRequest()
        stop_request.cleanUp = cleanup
        stub = ServiceStubWrapperFactory.createServiceStub(
            SlaveService_Stub, slave_spec)
        if stub == None:
            return None
        return (stub.asyncCall("stop", stop_request), stub)

    def _adjust_app_desc(self, applicationDescription):
        resources = applicationDescription.requiredResource.resources
        if len(resources) == 0:
            default_cpu_resource = resources.add()
            default_cpu_resource.type = Resource.SCALAR
            default_cpu_resource.name = 'cpu'
            default_cpu_resource.amount = DEFAULT_CPU_RESOURCE_AMOUNT

            default_mem_resource = resources.add()
            default_mem_resource.type = Resource.SCALAR
            default_mem_resource.name = 'mem'
            default_mem_resource.amount = DEFAULT_MEM_RESOURCE_AMOUNT
            
    def _handle_response_with_errorinfo(self, response):
        if self.printError:
            return self._handle_response_with_print_errorinfo(response)
        else:
            return self._handle_response_with_return_errorinfo(response)

    def _handle_response_with_print_errorinfo(self, response):        
        if not response:
            log_error('master did not response')
            return False
        if response.errorInfo.errorCode != ERROR_NONE:
            log_error('master response with errorcode: %d, errormsg: %s' % (
                response.errorInfo.errorCode, response.errorInfo.errorMsg))
            return False
        return True

    def _handle_response_with_return_errorinfo(self, response):
        error_msg = 'no err'
        if not response:
            error_msg = 'ERROR: master did not response'
            return False, error_msg
        if response.errorInfo.errorCode != ERROR_NONE:
            error_msg = 'ERROR: master response with errorcode: %d, errormsg: %s'%(
                response.errorInfo.errorCode, response.errorInfo.errorMsg)
            return False, error_msg
        return True, error_msg
    
    def _rpc_call(self, service, method, request):
        not_leader_retry_times = 3
        while not_leader_retry_times > 0:
            leader_address = self.get_leader()
            if leader_address is None:
                log_error('get leader failed')
                return None
            controller, response = self._do_rpc_call(leader_address, service,
                                                     method, request)
            if response == None or self._not_leader(controller.ErrorText()):
                not_leader_retry_times -= 1
                time.sleep(1)
            else:
                return response
        return None

    def _do_rpc_call(self, address, service, method, request):
        address = 'tcp:' + address
        channel = ANetRpcImpl.OpenChannel(address)
        if channel is None:
            log_error('open channel to %s failed, master may not exist' % address)
            return None, None
        controller = ANetRpcImpl.Controller()
        stub_class = globals()[service + '_Stub']
        stub_obj = stub_class(channel)
        stubWrapper = ServiceStubWrapper(stub_obj)
        response = stubWrapper.syncCall(method, request, 5) #sec
        return controller, response
        
    
    def _not_leader(self, error_text):
        return error_text.lower().startswith('leader is')

    def _get_master_worker_version(self, master_worker):
        request = GetWorkerVersionRequest()
        controller, response = self._do_rpc_call(master_worker,
                                                 "WorkerStatusService",
                                                 "getWorkerVersion",
                                                 request)
        if response == None:
            return None
        
        return response.version
    

    ## federation cmds
    def get_fed_cluster(self):
        request = GetClusterRequest()
        return self._rpc_call('AdminService', 'getCluster', request)
        
    def add_fed_cluster(self, cluster_id, endpoint, service_addr, driver_class,
                        specs = []):
        request = AddClusterRequest()
        request.clusterDesc.clusterId = cluster_id
        if endpoint:
            request.clusterDesc.endpoint = endpoint
        if service_addr:
            request.clusterDesc.serviceAddr = service_addr
        if driver_class:
            request.clusterDesc.driverClass = driver_class
        for i in specs:
            e = i.split("=")
            if len(e) != 2:
                log_error("invalid spec %s" % i)
                return False
            s = request.clusterDesc.specs.add()
            s.key = e[0]
            s.value = e[1]
        response = self._rpc_call('AdminService', 'addCluster', request)
        return self._handle_response_with_errorinfo(response)

    def stop_fed_cluster(self, cluster_id):
        request = StopClusterRequest()
        request.cluster = cluster_id
        response = self._rpc_call('AdminService', 'stopCluster', request)
        return self._handle_response_with_errorinfo(response)    
        
    def del_fed_cluster(self, cluster_id):
        request = DelClusterRequest()
        request.cluster = cluster_id
        response = self._rpc_call('AdminService', 'delCluster', request)
        return self._handle_response_with_errorinfo(response)
        
    def operate_fed_cluster(self, cluster_id, op = "online"):
        request = OperateClusterRequest()
        request.cluster = cluster_id
        request.state = op
        response = self._rpc_call('AdminService', 'setClusterState', request)
        return self._handle_response_with_errorinfo(response)

    def get_route_policy(self, policy_id):
        request = OperateRoutePolicyRequest()
        request.op = hippo.proto.Common_pb2.GET
        if policy_id:
            tmp = request.policies.add()
            tmp.policyId = policy_id
        return self._rpc_call('AdminService', 'operateRoutePolicy', request)

    def operate_route_policies(self, op = "get", policies = []):
        request = OperateRoutePolicyRequest()
        if op == "add":
            request.op = hippo.proto.Common_pb2.ADD
        elif op == "update":
            request.op = hippo.proto.Common_pb2.UPDATE
        elif op == "del":
            request.op = hippo.proto.Common_pb2.DEL
        else:
            request.op = hippo.proto.Common_pb2.GET
        for p in policies:
            tmp = request.policies.add()
            tmp.CopyFrom(p)
        print request
        response = self._rpc_call('AdminService', 'operateRoutePolicy', request)
        return self._handle_response_with_errorinfo(response)

    def get_policy_mapping(self, app_id, role):
        request = OperatePolicyMappingRequest()
        request.op = hippo.proto.Common_pb2.GET
        if app_id is not None:
            tmp = request.mappings.add()
            tmp.applicationId = app_id
            if role:
                tmp.rolePrefix = role
        return self._rpc_call('AdminService', 'operatePolicyMapping', request)
    
    def operate_policy_mappings(self, op = "get", mappings = []):
        request = OperatePolicyMappingRequest()
        if op == "add":
            request.op = hippo.proto.Common_pb2.ADD
        elif op == "update":
            request.op = hippo.proto.Common_pb2.UPDATE
        elif op == "del":
            request.op = hippo.proto.Common_pb2.DEL
        else:
            request.op = hippo.proto.Common_pb2.GET
        for m in mappings:
            tmp = request.mappings.add()
            tmp.CopyFrom(m)
        print request            
        response = self._rpc_call('AdminService', 'operatePolicyMapping', request)
        return self._handle_response_with_errorinfo(response)    
        
        
        
