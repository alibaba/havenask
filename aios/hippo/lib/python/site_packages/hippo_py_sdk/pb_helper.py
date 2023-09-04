#!/bin/env python

from hippo.proto.Common_pb2 import *
from hippo.proto.Admin_pb2 import *
from hippo.proto.Client_pb2 import *
from hippo.proto.ErrorCode_pb2 import *
from hippo.proto.ApplicationMaster_pb2 import *


def fullfill_update_preference_request(update_request, app_id, preference_desc):
    update_request.applicationId = app_id
    fullfill_resource_preference(update_request.preferenceDesc, preference_desc)

def fullfill_allocate_request(allocate_request, app_id, resource_request_descs = '', release_slotid_desc = '', preference_desc = '', slot_payload_desc = ''):
    '''
    input:
    allocate_request =  AllocateRequest

    resource_request_descs = 'resource_request_desc';'resource_request_desc'
    resource_request_desc = qrs,3,[scalar#cpu#100&text#a5#1|scalar#cpu#100&scalar#mem#32],Queueu_A
    
    release_slotid_desc =  'slot_id';'slot_id'
    slot_id='address','id'
    preference_desc = 'slaveaddress,type,leaseMs,tag;slaveaddress,type,leaseMs,tag'
    '''
    allocate_request.applicationId = app_id
    if resource_request_descs and resource_request_descs != '':
        resource_requests = resource_request_descs.split(';')
        for rd in resource_requests:
            r = allocate_request.require.add()
            fullfill_resource_request(r, rd)
    if release_slotid_desc and release_slotid_desc != '': 
        slots = release_slotid_desc.split(';')
        for sd in slots:
            #ReleaseSlotDescription
            rsd = allocate_request.release.add()
            sd_items = sd.split(',')
            address = sd_items[0]
            s_id = sd_items[1]
            rsd.slaveAddress = address
            rsd.id = int(s_id)
    fullfill_resource_preference(allocate_request.preferenceDesc, preference_desc)
    if slot_payload_desc and slot_payload_desc != '':
        fullfill_slot_payloads(allocate_request.slotPayloads, slot_payload_desc)

def fullfill_slot_payloads(slot_payloads, desc):
    raise Exception("full fill slot payloads not implemented")
    
def fullfill_resource_preference(ppd, preference_desc):
    if preference_desc and preference_desc != '':
        pds = preference_desc.split(';')
        for pd in pds:
            e = pd.split(',')
            slave = e[0]
            ptype = e[1]
            time = e[2]
            tag = e[3]
            workDirTag = ''
            if len(e) > 4:
                workDirTag = e[4]
            s_type = None
            if ptype == 'any':
                s_type = PreferenceDescription.PREF_ANY
            elif ptype == 'prefer':
                s_type = PreferenceDescription.PREF_PREFER
            elif ptype == 'betternot':
                s_type = PreferenceDescription.PREF_BETTERNOT
            elif ptype == 'prohibit':
                s_type = PreferenceDescription.PREF_PROHIBIT
            ppd_item = ppd.add()
            ppd_item.slaveAddress = slave
            ppd_item.type = s_type
            ppd_item.leaseMs = int(time)
            ppd_item.resourceTag = tag
            if workDirTag != '':
                ppd_item.workDirTag = workDirTag
            
def fullfill_slot_resource(slot_resource, desc, default_cpu=100, default_mem=4):
    resources_desc = desc.split('&')
    for r_desc in resources_desc:
        r_type, name, amount = r_desc.split('#')
        resource = slot_resource.resources.add()
        if r_type.lower() == 'scalar':
            resource.type = Resource.SCALAR
        elif r_type.lower() == 'scalar_cmp':
            resource.type = Resource.SCALAR_CMP
        elif r_type.lower() == 'text':
            resource.type = Resource.TEXT
        elif r_type.lower() == 'exclude_text':
            resource.type = Resource.EXCLUDE_TEXT
        elif r_type.lower() == 'queue_name':
            resource.type = Resource.QUEUE_NAME
        elif r_type.lower() == 'exclusive':
            resource.type = Resource.EXCLUSIVE
        elif r_type.lower() == 'prefer_text':
            resource.type = Resource.PREFER_TEXT
        elif r_type.lower() == 'prohibit_text':
            resource.type = Resource.PROHIBIT_TEXT
        else:
            print 'unrecognized resouce type: %s, please check' % r_type
            return False
        resource.name = name
        resource.amount = int(amount)
    return True

def fullfill_declarations(resource_request, desc):
    if desc == "":
        return True
    resources_desc = desc.split('&')
    for r_desc in resources_desc:
        r_type, name, amount = r_desc.split('#')
        resource = resource_request.declarations.add()
        if r_type.lower() == 'scalar':
            resource.type = Resource.SCALAR
        elif r_type.lower() == 'scalar_cmp':
            resource.type = Resource.SCALAR_CMP
        elif r_type.lower() == 'text':
            resource.type = Resource.TEXT
        elif r_type.lower() == 'exclude_text':
            resource.type = Resource.EXCLUDE_TEXT
        elif r_type.lower() == 'queue_name':
            resource.type = Resource.QUEUE_NAME
        elif r_type.lower() == 'exclusive':
            resource.type = Resource.EXCLUSIVE
        elif r_type.lower() == 'prefer_text':
            resource.type = Resource.PREFER_TEXT
        elif r_type.lower() == 'prohibit_text':
            resource.type = Resource.PROHIBIT_TEXT
        else:
            print 'unrecognized resouce type: %s, please check' % r_type
            return False
        resource.name = name
        resource.amount = int(amount)
    return True

def fullfill_resource_request(resource_request, desc, default_cpu=100, default_mem=4):
    '''
    input:
    resource_request = ResourceRequest()

    [] indicate it can be omitted.
    qrs,3,[scalar#cpu#100&text#a5#1|scalar#cpu#100&scalar#mem#32], queue_A, text#d1#1
    searcher,5,[scalar#cpu#200&text#b2#1|scalar#cpu#200&scalar#mem#64], queue_B, scalar#d2#10
    require:
    qrs: 3
          (scalar#cpu#100) and (text#a5#1)
       or (scalar#cpu#100) and (scalar#mem#32)
    searcher: 5
          (scalar#cpu#200) and (text#b2#1)
       or (scalar#cpu#200) and (text#b2#1) 
    '''
    descs = desc.split(',')
    resource_request.tag = descs[0]
    resource_request.count = int(descs[1])
    if len(descs) == 2:
        # if not resource, add default
        slot_resource = resource_request.options.add()
        default_resource = slot_resource.resources.add()
        default_resource.name = 'cpu'
        default_resource.type =  Resource.SCALAR
        default_resource.amount =  default_cpu
        default_resource = slot_resource.resources.add()
        default_resource.name = 'mem'
        default_resource.type =  Resource.SCALAR
        default_resource.amount =  default_mem
    if len(descs) >= 3:
        parseSlotResource(descs[2], resource_request, default_cpu, default_mem)  
    if len(descs) >= 4:
        resource_request.queue = descs[3]
    if len(descs) >= 5:
        fullfill_declarations(resource_request, descs[4])
    if len(descs) >= 6:
        if descs[5] != "":
            major, minor = descs[5].split(':')
            resource_request.priority.majorPriority = int(major)
            resource_request.priority.minorPriority = int(minor)
    if len(descs) >= 7:
        if descs[6] == "slot":
            resource_request.allocateMode = ResourceRequest.SLOT
        elif descs[6] == "machine":
            resource_request.allocateMode = ResourceRequest.MACHINE
        else:
            resource_request.allocateMode = ResourceRequest.AUTO
    if len(descs) >= 8:
        resource_request.groupId = descs[7]
    if len(descs) >= 9:
        paramStr = descs[8]
        for kvStr in paramStr.split(';'):
            k, v = kvStr.split(':')
            meta = resource_request.metaTags.add()
            meta.key = k
            meta.value = v

    return resource_request

def parseSlotResource(desc, resource_request, default_cpu, default_mem):
    resource_options = desc
    if resource_options == "None":
        return
    options = resource_options.split('|')
    for option in options:
        slot_resource = resource_request.options.add()
        fullfill_slot_resource(slot_resource, option,
                               default_cpu, default_mem)

def is_resource_satisfied(required_slot_resource, assigned_slot_resource):
    for required_resource in required_slot_resource.resources:
        resource_matched = False
        for assigned_resource in assigned_slot_resource.resources:
            if assigned_resource.name == required_resource.name and  \
                    assigned_resource.type == required_resource.type:
                if assigned_resource.type == Resource.SCALAR:
                    if assigned_resource.amount >= required_resource.amount:
                        resource_matched = True
                else:
                    resource_matched = True                    
                break
        if not resource_matched:
            return False
    return True
                    
                    
