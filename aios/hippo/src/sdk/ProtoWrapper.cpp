/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "hippo/ProtoWrapper.h"

using namespace std;

namespace hippo {

// convert proto message to hippo struct
void ProtoWrapper::convert(const proto::AssignedSlot &in,
                           hippo::SlotInfo *out)
{
    out->reclaiming = in.reclaim();
    out->launchSignature = in.launchsignature();
    out->packageChecksum = in.packagechecksum();
    out->preDeployPackageChecksum = in.predeploypackagechecksum();
    out->noLongerMatchQueue = in.nolongermatchqueue();
    out->noLongerMatchResourceRequirement =
        in.nolongermatchresourcerequirement();
    out->requirementId =
        in.requirementid();
    out->k8sNamespace = in.k8snamespace();
    out->k8sPodName = in.k8spodname();
    out->k8sPodUID = in.k8spoduid();
    out->ip = in.ip();
    convert(in.id(), &(out->slotId));
    convert(in.slavestatus(), &(out->slaveStatus));
    convert(in.slotresource(), &(out->slotResource));
    convert(in.processstatus(), &(out->processStatus));
    convert(in.datastatus(), &(out->dataStatus));
    convert(in.packagestatus(), &(out->packageStatus));
    convert(in.predeploypackagestatus(), &(out->preDeployPackageStatus));
    convert(in.slotpreference(), &(out->slotPreference));
}

void ProtoWrapper::convert(const proto::SlotPreference &in,
                           hippo::SlotPreference *out)
{
    switch(in.preftag()) {
    case proto::PREF_RELEASE:
        out->preftag = hippo::SlotPreference::PREF_RELEASE;
        break;
    default:
        out->preftag = hippo::SlotPreference::PREF_NORMAL;
    }
}

void ProtoWrapper::convert(const proto::SlotId &in,
                           hippo::SlotId *out)
{
    out->slaveAddress = in.slaveaddress();
    out->id = in.id();
    out->declareTime = in.declaretime();
    out->restfulHttpPort = in.restfulhttpport();
    out->k8sNamespace = in.k8snamespace();
    out->k8sPodName = in.k8spodname();
    out->k8sPodUID = in.k8spoduid();
}

void ProtoWrapper::convert(const proto::ProcessStatus &in,
                           hippo::ProcessStatus *out)
{
    out->isDaemon = in.isdaemon();
    out->status = (hippo::ProcessStatus::Status)(in.status());
    out->processName = in.processname();
    out->restartCount = in.restartcount();
    out->startTime = in.starttime();
    out->exitCode = in.exitcode();
    out->pid = in.pid();
    out->instanceId = in.instanceid();
    convert(in.otherinfos(), &out->otherInfos);
}

void ProtoWrapper::convert(const proto::SlaveStatus &in,
                           hippo::SlaveStatus *out)
{
    out->status = (hippo::SlaveStatus::Status)(in.status());
}

void ProtoWrapper::convert(const proto::DataStatus &in,
                           hippo::DataStatus *out)
{
    out->name = in.name();
    out->src = in.src();
    out->dst = in.dst();
    out->curVersion = in.curversion();
    out->targetVersion = in.targetversion();
    out->status = (hippo::DataStatus::Status)(in.deploystatus());
    out->retryCount = in.retrycount();
    out->attemptId = in.attemptid();
    if (in.lasterrorinfo().errorcode() == proto::ERROR_NONE) {
        out->lastErrorCode = hippo::DataStatus::ERROR_NONE;
    } else if (in.lasterrorinfo().errorcode() == proto::ERROR_DATA_SOURCE) {
        out->lastErrorCode = hippo::DataStatus::ERROR_SOURCE;
    } else if (in.lasterrorinfo().errorcode() == proto::ERROR_DATA_DEST) {
        out->lastErrorCode = hippo::DataStatus::ERROR_DEST;
    } else {
        out->lastErrorCode = hippo::DataStatus::ERROR_OTHER;
    }
    out->lastErrorMsg = in.lasterrorinfo().errormsg();
}

void ProtoWrapper::convert(const proto::PackageStatus &in,
                           hippo::PackageStatus *out)
{
    out->status = (hippo::PackageStatus::Status)(in.status());
}

void ProtoWrapper::convert(const proto::SlotResource &in,
                           hippo::SlotResource *out)
{
    convert(in.resources(), &out->resources);
}

void ProtoWrapper::convert(const proto::Resource &in,
                           hippo::Resource *out)
{
    out->type = (hippo::Resource::Type)(in.type());
    out->name = in.name();
    out->amount = in.amount();
}

void ProtoWrapper::convert(const proto::PackageInfo &in,
                           hippo::PackageInfo *out)
{
    out->URI = in.packageuri();
    out->type = (hippo::PackageInfo::PackageType)in.type();
}

void ProtoWrapper::convert(const proto::ProcessInfo &in,
                           hippo::ProcessInfo *out)
{
    out->isDaemon = in.isdaemon();
    out->name = in.processname();
    out->cmd = in.cmd();
    out->instanceId = in.instanceid();
    out->stopTimeout = in.stoptimeout();
    out->restartInterval = in.restartinterval();
    out->restartCountLimit = in.restartcountlimit();
    out->procStopSig = in.procstopsig();    
    convert(in.args(), &(out->args));
    convert(in.envs(), &(out->envs));
    convert(in.otherinfos(), &(out->otherInfos));
}

void ProtoWrapper::convert(const proto::DataInfo &in,
                           hippo::DataInfo *out)
{
    out->name = in.name();
    out->src = in.src();
    out->dst = in.dst();
    out->version = in.version();
    out->retryCountLimit = in.retrycountlimit();
    out->attemptId = in.attemptid();
    out->expireTime = in.expiretime();
    if (in.normalizetype() == proto::DataInfo::COLON_REPLACED) {
        out->normalizeType = DataInfo::COLON_REPLACED;
    } else {
        out->normalizeType = DataInfo::NONE;
    }
}

void ProtoWrapper::convert(const proto::Parameter &in,
                           hippo::PairType *out)
{
    out->first = in.key();
    out->second = in.value();
}

void ProtoWrapper::convert(const proto::Priority &in,
                           hippo::Priority *out)
{
    out->majorPriority = in.majorpriority();
    out->minorPriority = in.minorpriority();    
}

void ProtoWrapper::convert(const proto::ResourceRequest::AllocateMode &in,
                           hippo::ResourceAllocateMode *out)
{
    if (in == proto::ResourceRequest::MACHINE) {
        *out = hippo::MACHINE;
    } else if (in == proto::ResourceRequest::SLOT) {
        *out = hippo::SLOT;        
    } else {
        *out = hippo::AUTO;
    }
}


void ProtoWrapper::convert(const proto::ErrorInfo &in, hippo::ErrorInfo *out) {
    out->errorMsg = in.errormsg();
    switch(in.errorcode()) {
    case proto::ERROR_NONE:
        out->errorCode = hippo::ErrorInfo::ERROR_NONE;
        break;
    case proto::ERROR_MASTER_NOT_LEADER:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_NOT_LEADER;
        break;
    case proto::ERROR_MASTER_SERVICE_NOT_AVAILABLE:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_SERVICE_NOT_AVAILABLE;
        break;
    case proto::ERROR_MASTER_SERIALIZE_FAIL:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_SERIALIZE_FAIL;
        break;
    case proto::ERROR_MASTER_APP_ALREADY_EXIST:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_APP_ALREADY_EXIST;
        break;
    case proto::ERROR_MASTER_APP_NOT_EXIST:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_APP_NOT_EXIST;
        break;
    case proto::ERROR_MASTER_SLAVE_ALREADY_EXIST:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_SLAVE_ALREADY_EXIST;
        break;
    case proto::ERROR_MASTER_SLAVE_NOT_EXIST:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_SLAVE_NOT_EXIST;
        break;
    case proto::ERROR_MASTER_OFFLINE_SLAVE:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_OFFLINE_SLAVE;
        break;
    case proto::ERROR_MASTER_UPDATE_SLAVE_INFO:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_UPDATE_SLAVE_INFO;
        break;
    case proto::ERROR_MASTER_INVALID_REQUEST:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_INVALID_REQUEST;
        break;
    case proto::ERROR_MASTER_DESERIALIZE_FAIL:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_DESERIALIZE_FAIL;
        break;
    case proto::ERROR_MASTER_UPDATE_APP_FAIL:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_UPDATE_APP_FAIL;
        break;
    case proto::ERROR_MASTER_RESOURCE_CONFLICT:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_RESOURCE_CONFLICT;
        break;
    case proto::ERROR_MASTER_RESOURCE_NOT_EXIST:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_RESOURCE_NOT_EXIST;
        break;
    case proto::ERROR_MASTER_EXCLUDE_TEXT_CONFLICT:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_EXCLUDE_TEXT_CONFLICT;
        break;
    case proto::ERROR_MASTER_QUEUE_ALREADY_EXIST:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_QUEUE_ALREADY_EXIST;
        break;
    case proto::ERROR_MASTER_QUEUE_NOT_EXIST:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_QUEUE_NOT_EXIST;
        break;
    case proto::ERROR_MASTER_QUEUE_RESOURCE_DESC_ERROR:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_QUEUE_RESOURCE_DESC_ERROR;
        break;
    case proto::ERROR_MASTER_INNER_QUEUE_CANNT_DEL:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_INNER_QUEUE_CANNT_DEL;
        break;
    case proto::ERROR_MASTER_RULES_WRONG_FORMAT:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_RULES_WRONG_FORMAT;
        break;
    case proto::ERROR_MASTER_RULES_INVALID:
        out->errorCode = hippo::ErrorInfo::ERROR_MASTER_RULES_INVALID;
        break;
        // resource adjust
    case proto::ERROR_RESOURCE_AJUST_NO_NEED_AJUST:
        out->errorCode = hippo::ErrorInfo::ERROR_RESOURCE_AJUST_NO_NEED_AJUST;
        break;
    case proto::ERROR_RESOURCE_AJUST_FAIL_NOT_ENOUGH_RESOURCE:
        out->errorCode = hippo::ErrorInfo::ERROR_RESOURCE_AJUST_FAIL_NOT_ENOUGH_RESOURCE;
        break;
    case proto::ERROR_RESOURCE_AJUST_FAIL_RESOURCE_CONFLICT:
        out->errorCode = hippo::ErrorInfo::ERROR_RESOURCE_AJUST_FAIL_RESOURCE_CONFLICT;
        break;
        // slave error
    case proto::ERROR_SLAVE_CLEANUP_SLOT_PLAN:
        out->errorCode = hippo::ErrorInfo::ERROR_SLAVE_CLEANUP_SLOT_PLAN;
        break;
    case proto::ERROR_SLAVE_SLOTID_NOT_SPECIFIED:
        out->errorCode = hippo::ErrorInfo::ERROR_SLAVE_SLOTID_NOT_SPECIFIED;
        break;
    case proto::ERROR_SLAVE_SLOT_NOT_EXIST:
        out->errorCode = hippo::ErrorInfo::ERROR_SLAVE_SLOT_NOT_EXIST;
        break;
        // data error
    case proto::ERROR_DATA_SOURCE:
        out->errorCode = hippo::ErrorInfo::ERROR_DATA_SOURCE;
        break;
    case proto::ERROR_DATA_DEST:
        out->errorCode = hippo::ErrorInfo::ERROR_DATA_DEST;
        break;
    case proto::ERROR_DATA_OTHER:
        out->errorCode = hippo::ErrorInfo::ERROR_DATA_OTHER;
        break;
    default:
        out->errorCode = hippo::ErrorInfo::ERROR_NONE;
        break;
    }
}

// convert hippo struct to proto message
void ProtoWrapper::convert(const hippo::ProcessContext &in,
                           proto::ProcessLaunchContext *out)
{
    convert(in.pkgs, out->mutable_requiredpackages());
    convert(in.preDeployPkgs, out->mutable_predeploypackages());
    convert(in.processes, out->mutable_processes());
    convert(in.datas, out->mutable_requireddatas());
}

void ProtoWrapper::convert(const hippo::PackageInfo &in,
                           proto::PackageInfo *out)
{
    out->set_packageuri(in.URI);
    out->set_type((proto::PackageInfo::PackageType)in.type);
}

void ProtoWrapper::convert(const hippo::ProcessInfo &in,
                           proto::ProcessInfo *out)
{
    out->set_isdaemon(in.isDaemon);
    out->set_processname(in.name);
    out->set_cmd(in.cmd);
    out->set_instanceid(in.instanceId);
    out->set_stoptimeout(in.stopTimeout);
    out->set_restartinterval(in.restartInterval);
    out->set_restartcountlimit(in.restartCountLimit);
    out->set_procstopsig(in.procStopSig);    
    convert(in.args, out->mutable_args());
    convert(in.envs, out->mutable_envs());
    convert(in.otherInfos, out->mutable_otherinfos());
}

void ProtoWrapper::convert(const hippo::DataInfo &in,
                           proto::DataInfo *out)
{
    out->set_name(in.name);
    out->set_src(in.src);
    out->set_dst(in.dst);
    out->set_version(in.version);
    out->set_retrycountlimit(in.retryCountLimit);
    out->set_attemptid(in.attemptId);
    out->set_expiretime(in.expireTime);
    if (in.normalizeType == DataInfo::COLON_REPLACED) {
        out->set_normalizetype(proto::DataInfo::COLON_REPLACED);
    } else {
        out->set_normalizetype(proto::DataInfo::NONE);        
    }
}

void ProtoWrapper::convert(const hippo::PairType &in,
                           proto::Parameter *out)
{
    out->set_key(in.first);
    out->set_value(in.second);
}

void ProtoWrapper::convert(const vector<hippo::PackageInfo> &in,
                           vector<proto::PackageInfo> *out)
{
    out->clear();
    vector<hippo::PackageInfo>::const_iterator it = in.begin();
    for (; it != in.end(); ++it) {
        proto::PackageInfo packageInfo;
        convert(*it, &packageInfo);
        out->push_back(packageInfo);
    }
}

void ProtoWrapper::convert(const map<string, hippo::PackageInfo> &in,
                           vector<proto::PackageInfo> *out)
{
    out->clear();
    map<string, hippo::PackageInfo>::const_iterator it = in.begin();
    for (; it != in.end(); it++) {
        proto::PackageInfo packageInfo;
        convert(it->second, &packageInfo);
        out->push_back(packageInfo);
    }
}

void ProtoWrapper::convert(const hippo::SlotResource &in,
                           proto::SlotResource *out)
{
    convert(in.resources, out->mutable_resources());
}

void ProtoWrapper::convert(const hippo::Resource &in,
                           proto::Resource *out)
{
    out->set_type(proto::Resource::Type(in.type));
    out->set_name(in.name);
    out->set_amount(in.amount);
}

void ProtoWrapper::convert(const hippo::Priority &in,
                           proto::Priority *out)
{
    out->set_majorpriority(in.majorPriority);
    out->set_minorpriority(in.minorPriority);    
}

void ProtoWrapper::convert(const hippo::ResourceAllocateMode &in,
                           proto::ResourceRequest::AllocateMode *out)
{
    if (in == hippo::MACHINE) {
        *out = proto::ResourceRequest::MACHINE;
    } else if (in == hippo::SLOT) {
        *out = proto::ResourceRequest::SLOT;        
    } else {
        *out = proto::ResourceRequest::AUTO;
    }
}

void ProtoWrapper::convert(const hippo::ErrorInfo &in, proto::ErrorInfo *out) {
    out->set_errormsg(in.errorMsg);
    switch(in.errorCode) {
    case hippo::ErrorInfo::ERROR_NONE:
        out->set_errorcode(proto::ERROR_NONE);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_NOT_LEADER:
        out->set_errorcode(proto::ERROR_MASTER_NOT_LEADER);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_SERVICE_NOT_AVAILABLE:
        out->set_errorcode(proto::ERROR_MASTER_SERVICE_NOT_AVAILABLE);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_SERIALIZE_FAIL:
        out->set_errorcode(proto::ERROR_MASTER_SERIALIZE_FAIL);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_APP_ALREADY_EXIST:
        out->set_errorcode(proto::ERROR_MASTER_APP_ALREADY_EXIST);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_APP_NOT_EXIST:
        out->set_errorcode(proto::ERROR_MASTER_APP_NOT_EXIST);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_SLAVE_ALREADY_EXIST:
        out->set_errorcode(proto::ERROR_MASTER_SLAVE_ALREADY_EXIST);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_SLAVE_NOT_EXIST:
        out->set_errorcode(proto::ERROR_MASTER_SLAVE_NOT_EXIST);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_OFFLINE_SLAVE:
        out->set_errorcode(proto::ERROR_MASTER_OFFLINE_SLAVE);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_UPDATE_SLAVE_INFO:
        out->set_errorcode(proto::ERROR_MASTER_UPDATE_SLAVE_INFO);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_INVALID_REQUEST:
        out->set_errorcode(proto::ERROR_MASTER_INVALID_REQUEST);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_DESERIALIZE_FAIL:
        out->set_errorcode(proto::ERROR_MASTER_DESERIALIZE_FAIL);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_UPDATE_APP_FAIL:
        out->set_errorcode(proto::ERROR_MASTER_UPDATE_APP_FAIL);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_RESOURCE_CONFLICT:
        out->set_errorcode(proto::ERROR_MASTER_RESOURCE_CONFLICT);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_RESOURCE_NOT_EXIST:
        out->set_errorcode(proto::ERROR_MASTER_RESOURCE_NOT_EXIST);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_EXCLUDE_TEXT_CONFLICT:
        out->set_errorcode(proto::ERROR_MASTER_EXCLUDE_TEXT_CONFLICT);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_QUEUE_ALREADY_EXIST:
        out->set_errorcode(proto::ERROR_MASTER_QUEUE_ALREADY_EXIST);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_QUEUE_NOT_EXIST:
        out->set_errorcode(proto::ERROR_MASTER_QUEUE_NOT_EXIST);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_QUEUE_RESOURCE_DESC_ERROR:
        out->set_errorcode(proto::ERROR_MASTER_QUEUE_RESOURCE_DESC_ERROR);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_INNER_QUEUE_CANNT_DEL:
        out->set_errorcode(proto::ERROR_MASTER_INNER_QUEUE_CANNT_DEL);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_RULES_WRONG_FORMAT:
        out->set_errorcode(proto::ERROR_MASTER_RULES_WRONG_FORMAT);
        break;
    case hippo::ErrorInfo::ERROR_MASTER_RULES_INVALID:
        out->set_errorcode(proto::ERROR_MASTER_RULES_INVALID);
        break;
        // resource adjust
    case hippo::ErrorInfo::ERROR_RESOURCE_AJUST_NO_NEED_AJUST:
        out->set_errorcode(proto::ERROR_RESOURCE_AJUST_NO_NEED_AJUST);
        break;
    case hippo::ErrorInfo::ERROR_RESOURCE_AJUST_FAIL_NOT_ENOUGH_RESOURCE:
        out->set_errorcode(proto::ERROR_RESOURCE_AJUST_FAIL_NOT_ENOUGH_RESOURCE);
        break;
    case hippo::ErrorInfo::ERROR_RESOURCE_AJUST_FAIL_RESOURCE_CONFLICT:
        out->set_errorcode(proto::ERROR_RESOURCE_AJUST_FAIL_RESOURCE_CONFLICT);
        break;
        // slave error
    case hippo::ErrorInfo::ERROR_SLAVE_CLEANUP_SLOT_PLAN:
        out->set_errorcode(proto::ERROR_SLAVE_CLEANUP_SLOT_PLAN);
        break;
    case hippo::ErrorInfo::ERROR_SLAVE_SLOTID_NOT_SPECIFIED:
        out->set_errorcode(proto::ERROR_SLAVE_SLOTID_NOT_SPECIFIED);
        break;
    case hippo::ErrorInfo::ERROR_SLAVE_SLOT_NOT_EXIST:
        out->set_errorcode(proto::ERROR_SLAVE_SLOT_NOT_EXIST);
        break;
        // data error
    case hippo::ErrorInfo::ERROR_DATA_SOURCE:
        out->set_errorcode(proto::ERROR_DATA_SOURCE);
        break;
    case hippo::ErrorInfo::ERROR_DATA_DEST:
        out->set_errorcode(proto::ERROR_DATA_DEST);
        break;
    case hippo::ErrorInfo::ERROR_DATA_OTHER:
        out->set_errorcode(proto::ERROR_DATA_OTHER);
        break;
    default:
        out->set_errorcode(proto::ERROR_NONE);
        break;
    }
}

};
