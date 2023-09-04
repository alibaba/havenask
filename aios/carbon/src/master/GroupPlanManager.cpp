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
#include "master/GroupPlanManager.h"
#include "carbon/Log.h"
#include "autil/legacy/jsonizable.h"
#include "common/JsonUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

USE_CARBON_NAMESPACE(common);
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, GroupPlanManager);

GroupPlanManager::GroupPlanManager(const SerializerPtr &serializerPtr) {
    _serializerPtr = serializerPtr;
    _versionGeneratorPtr = GroupVersionGeneratorPtr(new GroupVersionGeneratorV1());
}

GroupPlanManager::~GroupPlanManager() {
}

version_t GroupPlanManager::addGroup(const GroupPlan &plan, ErrorInfo *errorInfo)
{
    ScopedLock lock(_mutex);
    groupid_t groupId = plan.groupId;
    VersionedGroupPlanMap tmpGroupPlans(_groupPlans);
    VersionedGroupPlanMap::const_iterator it = tmpGroupPlans.find(groupId);
    if (it != tmpGroupPlans.end()) {
        CARBON_LOG(WARN, "add group failed, groupId %s already exist",
                   groupId.c_str());
        errorInfo->errorCode = EC_GROUP_ALREADY_EXIST;
        errorInfo->errorMsg = "group already exist";
        return it->second.getGroupVersion();
    }

    version_t version = doAddGroup(groupId, plan, tmpGroupPlans);

    persist(tmpGroupPlans, errorInfo);
    return version;
}

version_t GroupPlanManager::doAddGroup(const groupid_t &groupId,
                                  const GroupPlan &plan,
                                  VersionedGroupPlanMap &tmpGroupPlans)
{
    version_t version = _versionGeneratorPtr->genGroupPlanVersion(plan);
    VersionedGroupPlan &versionedPlan = tmpGroupPlans[groupId];

    versionedPlan.plan = plan;
    versionedPlan.version = version;
    versionedPlan.checksum = version;

    return version;
}

version_t GroupPlanManager::updateGroup(const GroupPlan &plan,
                                   ErrorInfo *errorInfo)
{
    ScopedLock lock(_mutex);
    groupid_t groupId = plan.groupId;
    VersionedGroupPlanMap tmpGroupPlans(_groupPlans);
    VersionedGroupPlanMap::const_iterator it = tmpGroupPlans.find(groupId);
    if (it == tmpGroupPlans.end()) {
        CARBON_LOG(WARN, "update group failed, groupId %s not exist",
                   groupId.c_str());
        errorInfo->errorCode = EC_GROUP_NOT_EXIST;
        errorInfo->errorMsg = "group not exist";
        return _versionGeneratorPtr->genGroupPlanVersion(plan);
    }

    version_t version = doUpdateGroup(groupId, plan, tmpGroupPlans);

    persist(tmpGroupPlans, errorInfo);
    return version;
}

version_t GroupPlanManager::doUpdateGroup(const groupid_t &groupId,
                                     const GroupPlan &plan,
                                     VersionedGroupPlanMap &tmpGroupPlans)
{
    VersionedGroupPlan &versionedPlan = tmpGroupPlans[groupId];
    version_t newGroupPlanVersion = _versionGeneratorPtr->genGroupPlanVersion(plan);
    if (versionedPlan.checksum != newGroupPlanVersion) {
        versionedPlan.version = newGroupPlanVersion;
        versionedPlan.checksum = newGroupPlanVersion;
    }
    versionedPlan.plan = plan;
    return versionedPlan.version;
}    

void GroupPlanManager::delGroup(const groupid_t &groupId,
                                ErrorInfo *errorInfo)
{
    ScopedLock lock(_mutex);
    VersionedGroupPlanMap tmpGroupPlans(_groupPlans);
    tmpGroupPlans.erase(groupId);
    persist(tmpGroupPlans, errorInfo);
}

void GroupPlanManager::updateAllGroups(const GroupPlanMap &groupPlanMap,
                                       ErrorInfo *errorInfo)
{
    ScopedLock lock(_mutex);
    updateAllGroupsLocked(groupPlanMap, errorInfo);
}

void GroupPlanManager::updateAllGroupsLocked(
        const GroupPlanMap &groupPlanMap,
        ErrorInfo *errorInfo)
{
    VersionedGroupPlanMap tmpGroupPlans(_groupPlans);

    for(GroupPlanMap::const_iterator it = groupPlanMap.begin();
        it != groupPlanMap.end(); it++)
    {
        const groupid_t &groupId = it->first;
        const GroupPlan &plan = it->second;

        if (tmpGroupPlans.find(groupId) == tmpGroupPlans.end()) {
            doAddGroup(groupId, plan, tmpGroupPlans);
        } else {
            doUpdateGroup(groupId, plan, tmpGroupPlans);
        }
    }

    for(VersionedGroupPlanMap::iterator it = tmpGroupPlans.begin();
        it != tmpGroupPlans.end();)
    {
        if (groupPlanMap.find(it->first) == groupPlanMap.end()) {
            it = tmpGroupPlans.erase(it);
        } else {
            it++;
        }
    }    
    
    persist(tmpGroupPlans, errorInfo);
}

VersionedGroupPlanMap GroupPlanManager::getGroupPlans() const {
    ScopedLock lock(_mutex);
    return _groupPlans;
}

GroupPlanMap GroupPlanManager::getGroupPlansLocked() const {
    GroupPlanMap groupPlans;
    for (auto planPair : _groupPlans) {
        groupPlans[planPair.first] = planPair.second.plan;
    }
    return groupPlans;
}

void GroupPlanManager::persist(const VersionedGroupPlanMap &groupPlans,
                               ErrorInfo *errorInfo)
{
    string jsonString;
    try {
        jsonString = autil::legacy::ToJsonString(groupPlans, true);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "convert groupPlans to jsonstring failed: %s",
                   e.what());
        errorInfo->errorCode = EC_SERIALIZE_FAILED;
        errorInfo->errorMsg = "groupPlans ToJsonString failed";
        return;
    }

    if (!_serializerPtr->write(jsonString)) {
        CARBON_LOG(ERROR, "persist groupPlans failed");
        errorInfo->errorCode = EC_SERIALIZE_FAILED;
        errorInfo->errorMsg = "persist groupPlans failed";
        return;
    }

    _groupPlans = groupPlans;
}

bool GroupPlanManager::recover() {
    bool exist = false;
    bool succ = _serializerPtr->checkExist(exist);
    if (!succ) {
        CARBON_LOG(ERROR, "check GroupPlanManager serialize file failed.");
        return false;
    }

    if (!exist) {
        CARBON_LOG(INFO, "GroupPlanManager serialize file is not exist.");
        return true;
    }

    string str;
    succ = _serializerPtr->read(str);
    if (!succ) {
        CARBON_LOG(ERROR, "read GroupPlanManager serialied file failed.");
        return false;
    }

    try {
        autil::legacy::FromJsonString(_groupPlans, str);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "from json failed. error:[%s].", e.what());
        return false;
    }

    /*
      if the version of recovered GroupPlan is empty, it means that the
      version of GroupPlan was generated with GroupVersionGeneratorV0, the checksum
      is always generated with GroupVersionGeneratorV1.
     */
    GroupVersionGeneratorV0 versionGenerator;
    for (auto &planPair : _groupPlans) {
        VersionedGroupPlan &versionedPlan = planPair.second;
        if (versionedPlan.version == "") {
            versionedPlan.version = versionGenerator.genGroupPlanVersion(
                    versionedPlan.plan);
        }
        versionedPlan.checksum = _versionGeneratorPtr->genGroupPlanVersion(
                versionedPlan.plan);

    }
    
    return true;
}

void GroupPlanManager::createPlan(const string &path, const string &value,
                                  ErrorInfo *errorInfo)
{
    ScopedLock lock(_mutex);
    GroupPlanMap groupPlans = getGroupPlansLocked();

    Any a = ToJson(groupPlans);
    json::JsonMap *jsonMap = AnyCast<json::JsonMap>(&a);
    try {
        Any v;
        FromJsonString(v, value);
        bool ret = JsonUtil::create(jsonMap, path, v, errorInfo);
        if (!ret) {
            return;
        }
    } catch (const ExceptionBase &e) {
        CARBON_LOG(ERROR, "create plan node failed, error: %s", e.what());
        errorInfo->errorCode = EC_JSON_INVALID;
        errorInfo->errorMsg = "invalid json, error:" + string(e.what());
        return;
    }

    groupPlans.clear();
    FromJson(groupPlans, *jsonMap);

    updateAllGroupsLocked(groupPlans, errorInfo);
}

void GroupPlanManager::updatePlan(const string &path, const string &value,
                                 ErrorInfo *errorInfo)
{
    ScopedLock lock(_mutex);
    GroupPlanMap groupPlans = getGroupPlansLocked();
    Any a = ToJson(groupPlans);
    json::JsonMap *jsonMap = AnyCast<json::JsonMap>(&a);
    try {
        Any v;
        FromJsonString(v, value);
        bool ret = JsonUtil::update(jsonMap, path, v, errorInfo);
        if (!ret) {
            return;
        }
    } catch (const ExceptionBase &e) {
        CARBON_LOG(ERROR, "update plan node failed, error: %s", e.what());
        errorInfo->errorCode = EC_JSON_INVALID;
        errorInfo->errorMsg = "invalid json, error:" + string(e.what());
        return;
    }

    groupPlans.clear();
    FromJson(groupPlans, *jsonMap);
    updateAllGroupsLocked(groupPlans, errorInfo);
}

void GroupPlanManager::readPlan(const string &path, string *value,
                                ErrorInfo *errorInfo)
{
    ScopedLock lock(_mutex);

    GroupPlanMap groupPlans = getGroupPlansLocked();
    Any a = ToJson(groupPlans);
    json::JsonMap *jsonMap = AnyCast<json::JsonMap>(&a);
    Any v;
    bool ret = JsonUtil::read(*jsonMap, path, &v, errorInfo);
    if (!ret) {
        return;
    }

    *value = ToJsonString(v);
}

void GroupPlanManager::delPlan(const string &path, ErrorInfo *errorInfo) {
    ScopedLock lock(_mutex);
    GroupPlanMap groupPlans = getGroupPlansLocked();
    Any a = ToJson(groupPlans);
    json::JsonMap *jsonMap = AnyCast<json::JsonMap>(&a);
    JsonUtil::del(jsonMap, path, errorInfo);

    groupPlans.clear();
    FromJson(groupPlans, *jsonMap);
    updateAllGroupsLocked(groupPlans, errorInfo);
}

END_CARBON_NAMESPACE(master);

