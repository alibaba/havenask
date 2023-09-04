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
#include "common/Log.h"
#include "master_framework/SimpleMaster.h"
#include "simple_master/SimpleMasterServiceImpl.h"
#include "autil/StringUtil.h"
#include "fslib/fslib.h"

using namespace std;
using namespace hippo;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

USE_MASTER_FRAMEWORK_NAMESPACE(master_base);
BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

MASTER_FRAMEWORK_LOG_SETUP(simple_master, SimpleMaster);

#define SIMPLE_MASTER_APP_PLAN "simple_master_app_plan"

SimpleMaster::SimpleMaster() {
    _simpleMasterSchedulerCreatorPtr.reset(
            new SimpleMasterSchedulerCreator);
    _simpleMasterServiceImpl = new SimpleMasterServiceImpl(this);
    _leaderSerializer = NULL;
}

SimpleMaster::~SimpleMaster() {
    delete _simpleMasterServiceImpl;
    delete _leaderSerializer;
    _simpleMasterSchedulerPtr.reset();
}

bool SimpleMaster::doInit() {
    if (!MasterBase::doInit()) {
        return false;
    }

    if (!registerService(_simpleMasterServiceImpl)) {
        MF_LOG(ERROR, "simple master register service for simple master service failed");
        return false;
    }
    return initHTTPRPCServer();
}

bool SimpleMaster::doPrepareToRun() {
    if (!MasterBase::doPrepareToRun()) {
        return false;
    }
    
    if (!startSimpleMaster()) {
        shutdownLeaderElector();
        return false;
    }
    return true;
}

void SimpleMaster::doIdle() {
    if (!isLeader()) {
        MF_LOG(ERROR, "no longer be leader, it will stop ...");
        stop();
    }
    
    schedule();
}

bool SimpleMaster::startSimpleMaster() {
    ScopedLock lock(_lock);

    _simpleMasterSchedulerPtr = _simpleMasterSchedulerCreatorPtr->create();

    if (!_simpleMasterSchedulerPtr->init(getHippoZkPath(),
                    getLeaderElector(), getAppId()))
    {
        MF_LOG(ERROR, "init SimpleMasterScheduler failed! it will stop...");
        stop();
        return false;
    }

    if (!initLeaderSerializer()) {
        MF_LOG(ERROR, "init leader serializer failed! it will stop...");
        stop();        
        return false;
    }

    if (!recover()) {
        MF_LOG(ERROR, "recover failed! it will stop...");
        stop();
        return false;
    }

    if (!_simpleMasterSchedulerPtr->start()) {
        MF_LOG(ERROR, "start SimpleMasterScheduler failed! it will stop...");
        stop();
        return false;
    }
    return true;
}

bool SimpleMaster::initLeaderSerializer() {
    if (_leaderSerializer) {
        delete _leaderSerializer;
        _leaderSerializer = NULL;
    }

    string zkPath = getZkPath();
    if (zkPath.empty()) {
        MF_LOG(ERROR, "simple master zk path is empty!");
        return false;
    }
    
    _leaderSerializer = _simpleMasterSchedulerPtr->createLeaderSerializer(
            zkPath, SIMPLE_MASTER_APP_PLAN);
    if (_leaderSerializer == NULL) {
        return false;
    }

    if (*zkPath.rbegin() == '/') {
        _serializePath = zkPath + SIMPLE_MASTER_APP_PLAN;
    } else {
        _serializePath = zkPath + "/" + SIMPLE_MASTER_APP_PLAN;
    }

    return true;
}

bool SimpleMaster::recover() {
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(_serializePath);
    if (ec != fslib::EC_TRUE && ec != fslib::EC_FALSE) {
        MF_LOG(ERROR, "check simple app plan exist failed. error:%s",
               fslib::fs::FileSystem::getErrorString(ec).c_str());
        return false;
    }
    
    if (ec == fslib::EC_TRUE) {
        AppPlan appPlan;
        if (!doRecoverAppPlan(appPlan)) {
            MF_LOG(ERROR, "recover simple app plan failed!");
            return false;
        }
        
        _appPlan = appPlan;
    }

    return true;
}

bool SimpleMaster::doRecoverAppPlan(AppPlan &appPlan) {
    string content;
    assert(_leaderSerializer);
    if (!_leaderSerializer->read(content)) {
        MF_LOG(ERROR, "read app plan failed.");
        return false;
    }

    if (!appPlan.fromString(content)) {
        MF_LOG(ERROR, "parse json str[%s] failed.",
               content.c_str());
        return false;
    }

    return true;
}

void SimpleMaster::updateAppPlan(const proto::UpdateAppPlanRequest *request,
                                 proto::UpdateAppPlanResponse *response)
{
    MF_LOG(INFO, "update app plan, request:[%s].",
           request->ShortDebugString().c_str());
    AppPlan appPlan;
    if (!appPlan.fromString(request->appplan())) {
        MF_LOG(ERROR, "parse json str[%s] failed.",
               request->appplan().c_str());
        response->mutable_errorinfo()->set_errorcode(
                proto::ErrorInfo::ERROR_INVALID_APP_PLAN);
        response->mutable_errorinfo()->set_errormsg(
                "parse json str failed.");
        return;
    }

    ScopedLock lock(_lock);
        
    if (!doUpdateAppPlan(appPlan)) {
        response->mutable_errorinfo()->set_errorcode(
                proto::ErrorInfo::ERROR_SERIALIZE_APP_PLAN);
        response->mutable_errorinfo()->set_errormsg(
                "serialize app plan failed.");
    }
}

void SimpleMaster::updateRoleProperties(
        const proto::UpdateRolePropertiesRequest *request,
        proto::UpdateRolePropertiesResponse *response)
{
    proto::ErrorInfo *errorInfo = response->mutable_errorinfo();
    const string &op = request->operate();
    const string &jsonKvStr = request->jsonkvstr();
    const string &roleName = request->rolename();

    ScopedLock lock(_lock);
    
    AppPlan appPlan = _appPlan;
    map<string, RolePlan> &rolePlans = appPlan.rolePlans;
    if (rolePlans.find(roleName) == rolePlans.end()) {
        errorInfo->set_errorcode(
                proto::ErrorInfo::ERROR_UPDATE_ROLE_PROPERTIES);
        errorInfo->set_errormsg("update role properties failed, "
                                "can not find role.");
        return;
    }

    map<string, string> kvMap;
    try {
        autil::legacy::FromJsonString(kvMap, jsonKvStr);
    } catch (const autil::legacy::ExceptionBase &e) {
        MF_LOG(ERROR, "kv map from json string failed.");
        errorInfo->set_errorcode(
                proto::ErrorInfo::ERROR_UPDATE_ROLE_PROPERTIES);
        errorInfo->set_errormsg("kv map from json string failed.");
        return;
    }

    RolePlan &rolePlan = rolePlans[roleName];
    updateProperties(op, kvMap, rolePlan);

    if (!doUpdateAppPlan(appPlan)) {
        response->mutable_errorinfo()->set_errorcode(
                proto::ErrorInfo::ERROR_SERIALIZE_APP_PLAN);
        response->mutable_errorinfo()->set_errormsg(
                "serialize app plan failed.");
    }
}

std::vector<hippo::SlotInfo> SimpleMaster::getRoleSlots(const std::string &roleName) {
    return _simpleMasterSchedulerPtr->getRoleSlots(roleName);
}

void SimpleMaster::getAllRoleSlots(std::map<std::string, master_base::SlotInfos> &roleSlots) {
    return _simpleMasterSchedulerPtr->getAllRoleSlots(roleSlots);
}
    
void SimpleMaster::releaseSlots(const std::vector<hippo::SlotId> &slots,
                    const hippo::ReleasePreference &pref) {
    _simpleMasterSchedulerPtr->releaseSlots(slots, pref);
}

void SimpleMaster::updateProperties(const string &op,
                                    const map<string, string> &kvMap,
                                    RolePlan &rolePlan)
{
    if (op == "set") {
        setProperties(kvMap, rolePlan);
    } else if (op == "del") {
        delProperties(kvMap, rolePlan);
    }
}

void SimpleMaster::setProperties(const map<string, string> &kvMap,
                                 RolePlan &rolePlan)
{
    map<string, string> &properties = rolePlan.properties;
    for (map<string, string>::const_iterator it = kvMap.begin();
         it != kvMap.end(); it++)
    {
        properties[it->first] = it->second;
    }
}

void SimpleMaster::delProperties(const map<string, string> &kvMap,
                                 RolePlan &rolePlan)
{
    map<string, string> &properties = rolePlan.properties;
    for (map<string, string>::const_iterator it = kvMap.begin();
         it != kvMap.end(); it++)
    {
        properties.erase(it->first);
    }
}

bool SimpleMaster::doUpdateAppPlan(const AppPlan &appPlan) {
    if (appPlan == _appPlan) {
        return true;
    }

    MF_LOG(INFO, "app plan is modified. new app plan is:[%s].",
           ToJsonString(appPlan).c_str());
    
    string content = ToJsonString(appPlan);
    if (_leaderSerializer != NULL && !_leaderSerializer->write(content)) {
        MF_LOG(ERROR, "app plan[%s] serialize failed, will not update.",
               content.c_str());
        return false;
    }
    
    _appPlan = appPlan;
    return true;
}

void SimpleMaster::schedule() {
    assert(_simpleMasterSchedulerPtr != NULL);

    map<string, SlotInfos> roleSlotInfos;
    _simpleMasterSchedulerPtr->getAllRoleSlots(roleSlotInfos);

    AppPlan appPlan;
    {
        ScopedLock lock(_lock);
        appPlan = _appPlan;
        doSchedule(roleSlotInfos, appPlan);
        doUpdateAppPlan(appPlan);
    }

    _simpleMasterSchedulerPtr->setAppPlan(_appPlan);
}

string SimpleMaster::getIpFromSpec(const string &spec) {
    vector<string> ipAndPort = StringUtil::split(spec, ":");
    if (ipAndPort.size() == 0) {
        MF_LOG(ERROR, "hippo return an invalid slot.");
        return "";
    }
    return ipAndPort[0];
}

void SimpleMaster::getRoleIps(
        const map<string, SlotInfos> &roleSlotInfos,
        const string &roleName, vector<string> &ips)
{
    ips.clear();
    map<string, SlotInfos>::const_iterator it = roleSlotInfos.find(
            roleName);
    if (it == roleSlotInfos.end()) {
        MF_LOG(WARN, "%s not allocated.", roleName.c_str());
        return;
    }

    const SlotInfos &slotInfos = it->second;
    if (slotInfos.size() == 0) {
        MF_LOG(WARN, "%s not allocated.", roleName.c_str());
        return;
    }
    
    for (size_t i = 0; i < slotInfos.size(); i++) {
        const SlotInfo &slotInfo = slotInfos[i];
        string hippoSlaveSpec = slotInfo.slotId.slaveAddress;
        ips.push_back(getIpFromSpec(hippoSlaveSpec));
    }
}

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

