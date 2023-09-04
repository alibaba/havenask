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
#include "sdk/ProcessLauncherBase.h"

using namespace std;
using namespace autil;
using namespace hippo;
BEGIN_HIPPO_NAMESPACE(sdk);

HIPPO_LOG_SETUP(sdk, ProcessLauncherBase);

ProcessLauncherBase::ProcessLauncherBase() {
}

ProcessLauncherBase::~ProcessLauncherBase() {
}

void ProcessLauncherBase::setRoleProcessContext(const string &role,
        const ProcessContext &context,
        const string &scope)
{
    ScopedLock lock(_mutex);
    // set role basic context and clear custom context
    _roleProcessContextMap[role].basicContext[scope] = context;
    map<SlotId, ScopeProcessContextMap> &slotContextMap =
        _roleProcessContextMap[role].customContext;
    map<SlotId, ScopeProcessContextMap>::iterator it=
        slotContextMap.begin();
    for (; it != slotContextMap.end(); it++) {
        it->second.erase(scope);
    }
}

void ProcessLauncherBase::setSlotProcessContext(const vector<SlotInfo> &slotVec,
        const ProcessContext &context,
        const string &scope)
{
    ScopedLock lock(_mutex);
    // set slot vec custom context
    for (size_t i = 0; i < slotVec.size(); i++) {
        const string &role = slotVec[i].role;
        const SlotId &slotId = slotVec[i].slotId;
        doSetSlotProcessContext(role, slotId, context, scope);
    }
}

void ProcessLauncherBase::setSlotProcessContext(
        const vector<pair<vector<SlotInfo>, ProcessContext> > &slotLaunchPlans,
        const string &scope)
{
    ScopedLock lock(_mutex);
    // set slot vec custom context
    for (size_t i = 0; i < slotLaunchPlans.size(); ++i) {
        const vector<SlotInfo> &slotVec = slotLaunchPlans[i].first;
        const hippo::ProcessContext &context = slotLaunchPlans[i].second;
        for (size_t i = 0; i < slotVec.size(); i++) {
            const string &role = slotVec[i].role;
            const SlotId &slotId = slotVec[i].slotId;
            doSetSlotProcessContext(role, slotId, context, scope);
        }
    }
}

void ProcessLauncherBase::clearRoleProcessContext(const string &role) {
    ScopedLock lock(_mutex);
    _roleProcessContextMap.erase(role);
}

bool ProcessLauncherBase::restartProcess(const SlotInfo &slotInfo,
        const vector<string> &processNames)
{
    ScopedLock lock(_mutex);

    const string &role = slotInfo.role;
    const hippo::SlotId &slotId = slotInfo.slotId;
    hippo::ProcessContext slotProcessContext;
    if (!doGetSlotProcessContext(role, slotId, &slotProcessContext)) {
        HIPPO_LOG(WARN, "restart process fail, "
                  "get slot process context failed. "
                  "role:%s", role.c_str());
        return false;
    }
    for (size_t i = 0; i < processNames.size(); ++i) {
        const string &procName = processNames[i];
        hippo::ProcessInfo *procInfo = getProcessInfo(slotProcessContext, procName);
        if (!procInfo) {
            HIPPO_LOG(WARN, "restart process fail, can not find process[%s]",
                      procName.c_str());
            return false;
        }
        _procInstanceIds[slotId][procName] = TimeUtility::currentTime();
    }
    return true;
}

bool ProcessLauncherBase::isProcessReady(const SlotInfo &slotInfo,
        const vector<string> &processNames) const
{
    const string &role = slotInfo.role;
    const hippo::SlotId &slotId = slotInfo.slotId;
    hippo::ProcessContext slotProcessContext;
    if (!getSlotProcessContext(role, slotId, &slotProcessContext)) {
        HIPPO_LOG(WARN, "check is process ready fail, "
                  "get slot process context failed. "
                  "role:%s", role.c_str());
        return false;
    }

    for (size_t i = 0; i < processNames.size(); ++i) {
        const string &procName = processNames[i];
        hippo::ProcessInfo *procInfo = getProcessInfo(
                slotProcessContext, procName);
        if (!procInfo) {
            HIPPO_LOG(WARN, "check is process ready fail,"
                      " can not find process[%s]",
                      procName.c_str());
            return false;
        }

        int64_t reportedProcInstId = -1;
        for (size_t j = 0; j < slotInfo.processStatus.size(); j++) {
            if (slotInfo.processStatus[j].processName == procName) {
                reportedProcInstId = slotInfo.processStatus[j].instanceId;
                break;
            }
        }

        if (reportedProcInstId < getProcInstanceId(slotId, procName)) {
            return false;
        }
    }
    return true;
}

bool ProcessLauncherBase::updatePackages(const SlotInfo &slotInfo,
        const vector<PackageInfo> &packages)
{
    ScopedLock lock(_mutex);
    const string &role = slotInfo.role;
    const hippo::SlotId &slotId = slotInfo.slotId;
    hippo::ProcessContext slotProcessContext;
    if (!doGetSlotProcessContext(role, slotId, &slotProcessContext)) {
        HIPPO_LOG(WARN, "update package fail, get slot process context failed. "
                  "role:%s", role.c_str());
        return false;
    }
    HIPPO_LOG(INFO, "update slot%d@%s's package to %s",
              slotId.id, slotId.slaveAddress.c_str(),
              StringUtil::toString(packages, "\n").c_str());
    slotProcessContext.pkgs = packages;
    doSetSlotProcessContext(role, slotId, slotProcessContext);
    HIPPO_LOG(DEBUG, "pkg size:[%zd]", slotProcessContext.pkgs.size());
    return true;
}

bool ProcessLauncherBase::updatePreDeployPackages(const SlotInfo &slotInfo,
        const vector<PackageInfo> &packages)
{
    ScopedLock lock(_mutex);
    const string &role = slotInfo.role;
    const hippo::SlotId &slotId = slotInfo.slotId;
    hippo::ProcessContext slotProcessContext;
    if (!doGetSlotProcessContext(role, slotId, &slotProcessContext)) {
        HIPPO_LOG(WARN, "update package fail, get slot process context failed. "
                  "role:%s", role.c_str());
        return false;
    }
    HIPPO_LOG(INFO, "update slot%d@%s's pre-deploy package to %s",
              slotId.id, slotId.slaveAddress.c_str(),
              StringUtil::toString(packages, "\n").c_str());
    slotProcessContext.preDeployPkgs = packages;
    doSetSlotProcessContext(role, slotId, slotProcessContext);
    HIPPO_LOG(DEBUG, "pkg size:[%zd]", slotProcessContext.pkgs.size());
    return true;
}

bool ProcessLauncherBase::updateDatas(const SlotInfo &slotInfo,
                                  const vector<hippo::DataInfo> &datas,
                                  bool force)
{
    ScopedLock lock(_mutex);

    const string &role = slotInfo.role;
    const hippo::SlotId &slotId = slotInfo.slotId;
    hippo::ProcessContext slotProcessContext;
    if (!doGetSlotProcessContext(role, slotId, &slotProcessContext)) {
        HIPPO_LOG(WARN, "update datas fail, get slot process context failed. "
                  "role:%s", role.c_str());
        return false;
    }

    slotProcessContext.datas = datas;

    if (force) {
        int64_t currentTime = TimeUtility::currentTime();
        for (size_t i = 0; i < slotProcessContext.datas.size(); i++) {
            slotProcessContext.datas[i].attemptId = currentTime;
        }
    }
    doSetSlotProcessContext(role, slotId, slotProcessContext);
    return true;
}

void ProcessLauncherBase::setLaunchMetas(const map<SlotId, LaunchMeta> &launchMetas)
{
    _launchMetas = launchMetas;
}

void ProcessLauncherBase::setApplicationId(const string &appId) {
    _applicationId = appId;
}

void ProcessLauncherBase::setAppChecksum(int64_t checksum) {
    _appChecksum = checksum;
}

bool ProcessLauncherBase::getSlotProcessContext(const string &role,
        const SlotId &slotId,
        ProcessContext *context) const
{
    ScopedLock lock(_mutex);
    hippo::ProcessContext cxt;
    bool ret = doMergeSlotProcessContext(role, slotId, &cxt);
    if (ret) {
        *context = cxt;
    }
    setProcInstanceIds(slotId, context);

    return ret;
}

void ProcessLauncherBase::doSetSlotProcessContext(const string &role,
        const hippo::SlotId &slotId, const hippo::ProcessContext &context,
        const std::string &scope)
{
    _roleProcessContextMap[role].customContext[slotId][scope] = context;
}

bool ProcessLauncherBase::doGetSlotProcessContext(const string &role,
        const hippo::SlotId &slotId, hippo::ProcessContext *context,
        const std::string &scope) const
{
    RoleProcessContextMap::const_iterator it = _roleProcessContextMap.find(
            role);
    if (it == _roleProcessContextMap.end()) {
        return false;
    }
    const RoleProcessContext &roleContext = it->second;
    map<hippo::SlotId, ScopeProcessContextMap>::const_iterator cIt =
        roleContext.customContext.find(slotId);
    if (cIt != roleContext.customContext.end()) {
        if (!getProcessContext(cIt->second, scope, context)) {
            return false;
        }
    } else {
        if (!getProcessContext(roleContext.basicContext, scope, context)) {
            return false;
        }
    }
    return true;
}

bool ProcessLauncherBase::getProcessContext(
        const ScopeProcessContextMap &contextMap,
        const std::string &scope, hippo::ProcessContext *context) const
{
    ScopeProcessContextMap::const_iterator sIt = contextMap.find(scope);
    if (sIt != contextMap.end()) {
        (*context) = sIt->second;
        return true;
    }
    return false;
}

hippo::ProcessInfo* ProcessLauncherBase::getProcessInfo(
        hippo::ProcessContext &processContext,
        const string &procName) const
{
    vector<hippo::ProcessInfo> &processInfos = processContext.processes;
    for (size_t i = 0; i < processInfos.size(); ++i) {
        if (processInfos[i].name == procName) {
            return &processInfos[i];
        }
    }
    return NULL;
}

int64_t ProcessLauncherBase::getProcInstanceId(
        const hippo::SlotId &slotId, const string &procName) const
{
    map<hippo::SlotId, map<string, int64_t> >::const_iterator it =
        _procInstanceIds.find(slotId);
    if (it == _procInstanceIds.end()) {
        return 0;
    }
    map<string, int64_t>::const_iterator it2 = it->second.find(procName);
    if (it2 == it->second.end()) {
        return 0;
    }
    return it2->second;
}

void ProcessLauncherBase::setProcInstanceIds(
        const hippo::SlotId &slotId, hippo::ProcessContext *context) const
{
    map<hippo::SlotId, map<string, int64_t> >::const_iterator it =
        _procInstanceIds.find(slotId);
    if (it == _procInstanceIds.end()) {
        return ;
    }
    const map<string, int64_t> & procInstIds = it->second;
    for (size_t i = 0; i < context->processes.size(); i++) {
        hippo::ProcessInfo &procInfo = context->processes[i];
        map<string, int64_t>::const_iterator procIt = procInstIds.find(
                procInfo.name);
        if (procIt == procInstIds.end()) {
            continue;
        }
        procInfo.instanceId = procIt->second;
    }
}

bool ProcessLauncherBase::doMergeSlotProcessContext(const string &role,
        const hippo::SlotId &slotId, hippo::ProcessContext *context) const
{
    RoleProcessContextMap::const_iterator it = _roleProcessContextMap.find(
            role);
    if (it == _roleProcessContextMap.end()) {
        return false;
    }
    const RoleProcessContext &roleContext = it->second;
    ScopeProcessContextMap scopeSlotContext;
    ScopeProcessContextMap scopeRoleContext = roleContext.basicContext;
    map<hippo::SlotId, ScopeProcessContextMap>::const_iterator cIt =
        roleContext.customContext.find(slotId);
    if (cIt != roleContext.customContext.end()) {
        scopeSlotContext = cIt->second;
    }
    doMergeProcessContext(scopeSlotContext, scopeRoleContext, context);
    return true;
}

void ProcessLauncherBase::doMergeProcessContext(
        const ScopeProcessContextMap &slotContextMap,
        const ScopeProcessContextMap &roleContextMap,
        hippo::ProcessContext *context) const
{
    ScopeProcessContextMap scopeProcessContextMap = slotContextMap;
    ScopeProcessContextMap::const_iterator it = roleContextMap.begin();
    for (; it != roleContextMap.end(); it++) {
        if (slotContextMap.find(it->first) != slotContextMap.end()) {
            continue;
        }
        scopeProcessContextMap[it->first] = it->second;
    }

    HIPPO_LOG(DEBUG, "scope map size:[%zd].", scopeProcessContextMap.size());
    mergeScopeProcessContext(scopeProcessContextMap, context);
}

void ProcessLauncherBase::mergeScopeProcessContext(
        const ScopeProcessContextMap &slotContextMap,
        hippo::ProcessContext *context) const
{
    set<string> packageUris;
    set<string> processNames;
    set<string> dataNames;

    ScopeProcessContextMap::const_iterator it = slotContextMap.begin();
    for (; it != slotContextMap.end(); it++) {
        const ProcessContext &pcxt = it->second;
        for (size_t i = 0; i < pcxt.processes.size(); i++) {
            const ProcessInfo &pinfo = pcxt.processes[i];
            if (processNames.count(pinfo.name) > 0) {
                continue;
            }
            context->processes.push_back(pinfo);
            processNames.insert(pinfo.name);
        }

        for (size_t i = 0; i < pcxt.pkgs.size(); i++) {
            const PackageInfo &pkgInfo = pcxt.pkgs[i];
            if (packageUris.count(pkgInfo.URI) > 0) {
                continue;
            }
            HIPPO_LOG(DEBUG, "scope:%s, pkgInfo:[%s].",
                      it->first.c_str(), pkgInfo.URI.c_str());
            context->pkgs.push_back(pkgInfo);
            packageUris.insert(pkgInfo.URI);
        }

        for (size_t i = 0; i < pcxt.datas.size(); i++) {
            const DataInfo &dataInfo = pcxt.datas[i];
            if (dataNames.count(dataInfo.name) > 0) {
                continue;
            }
            context->datas.push_back(dataInfo);
            dataNames.insert(dataInfo.name);
        }
    }
    HIPPO_LOG(DEBUG, "context vector size:[%zd].", context->pkgs.size());
}

void ProcessLauncherBase::clearUselessContexts(
        const map<string, set<hippo::SlotId> > &slotIds)
{
    ScopedLock lock(_mutex);
    RoleProcessContextMap::iterator it = _roleProcessContextMap.begin();
    while (it != _roleProcessContextMap.end()) {
        const string &role = it->first;
        map<string, set<hippo::SlotId> >::const_iterator slotIt =
            slotIds.find(role);
        if (slotIt == slotIds.end()) {
            _roleProcessContextMap.erase(it++);
            continue;
        }
        const set<hippo::SlotId> &availableSlotIds = slotIt->second;
        map<hippo::SlotId, ScopeProcessContextMap> &customContext =
            it->second.customContext;
        map<hippo::SlotId, ScopeProcessContextMap>::iterator cIt =
            customContext.begin();
        while (cIt != customContext.end()) {
            const hippo::SlotId &slotId = cIt->first;
            if (availableSlotIds.count(slotId) == 0) {
                customContext.erase(cIt++);
            } else {
                cIt++;
            }
        }
        it++;
    }
}

END_HIPPO_NAMESPACE(sdk);
