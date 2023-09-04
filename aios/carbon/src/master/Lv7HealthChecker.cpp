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
#include "master/Lv7HealthChecker.h"
#include "carbon/Log.h"
#include "master/WorkerNode.h"
#include "master/SlotUtil.h"
#include "monitor/MonitorUtil.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, Lv7HealthChecker);

Lv7HealthChecker::Lv7HealthChecker(const string &id) :
    HealthChecker(id)
{
    _httpClientPtr.reset(new HttpClient());
}

Lv7HealthChecker::~Lv7HealthChecker() {
}

bool Lv7HealthChecker::init(const map<string, string> &options) {
    string portStr;
    if (!getValue(options, KEY_PORT, portStr)) {
        return false;
    }
    if (!StringUtil::strToInt32(portStr.c_str(), _port)) {
        CARBON_LOG(ERROR, "invalid port %s.", portStr.c_str());
        return false;
    }
    
    if (!getValue(options, KEY_CHECK_PATH, _checkPath)) {
        return false;
    }

    if (_checkPath.empty()) {
        return false;
    }

    if (_checkPath[0] != '/') {
        _checkPath = "/" + _checkPath;
    }

    string lostTimeoutStr;
    if (getValue(options, KEY_LOST_TIMEOUT, lostTimeoutStr)) {
        if (!StringUtil::strToInt64(lostTimeoutStr.c_str(), _lostTimeout)) {
            CARBON_LOG(ERROR, "invalid lostTime %s.", lostTimeoutStr.c_str()); 
            return false;
        }
        _lostTimeout *= 1000000; // s to us
    } else {
        _lostTimeout = DEFAULT_LOST_TIMEOUT;
    }
    
    _healthStatusTransferPtr.reset(new HealthStatusTransfer(_lostTimeout));
            
    return true;
}

void Lv7HealthChecker::doUpdate(const vector<WorkerNodePtr> &workerNodes) {
    vector<CheckNode> checkNodeVect;
    for (size_t i = 0; i < workerNodes.size(); i++) {
        const WorkerNodePtr &workerNodePtr = workerNodes[i];
        if (workerNodePtr->isUnAssignedSlot()) {
            CARBON_LOG(INFO, "worker node has not assigned slots. do not check. worker node:[%s].", workerNodePtr->getId().c_str());
            continue;
        }
        if (workerNodePtr->hasEmptySlotInfo()) {
            CARBON_LOG(WARN, "worker node has empty slot info, maybe be released, do not check health. worker node:[%s].", workerNodePtr->getId().c_str());
            continue;
        }
        nodeid_t nodeUID = workerNodePtr->getId();
        CheckNode checkNode;
        checkNode.nodeId = nodeUID;
        checkNode.version = workerNodePtr->getCurVersion();
        checkNode.slotInfo = workerNodePtr->getSlotInfo();
        fillNodeMetas(workerNodePtr, &checkNode.metas);
        checkNodeVect.push_back(checkNode);
    }

    ScopedLock lock(_lock);
    _checkNodeVect.swap(checkNodeVect);
}

bool Lv7HealthChecker::needHealthCheck(const SlotInfo &slotInfo) const {
    return SlotUtil::isPackageInstalled(slotInfo) &&
        SlotUtil::isAllProcessAlive(slotInfo);
}

void Lv7HealthChecker::getNodesNeedCheck(
        const vector<CheckNode> &allCheckNodeVect,
        vector<CheckNode> &needCheckNodes,
        set<nodeid_t> &allNodeIdSet)
{
    for (size_t i = 0; i < allCheckNodeVect.size(); i++) {
        const nodeid_t &nodeId = allCheckNodeVect[i].nodeId;
        const SlotInfo &slotInfo = allCheckNodeVect[i].slotInfo;
        version_t version = allCheckNodeVect[i].version;
        allNodeIdSet.insert(nodeId);
        if (SlotUtil::isSlotUnRecoverable(slotInfo)) {
            CARBON_LOG(WARN, "slot is non-recoverable, nodeId:[%s].",
                       nodeId.c_str());
            CheckResult &checkResult = _checkResultMap[nodeId];
            checkResult.nodeId = nodeId;
            checkResult.healthInfo.version = version;
            checkResult.healthInfo.healthStatus = HT_DEAD;
            checkResult.healthInfo.workerStatus = WT_UNKNOWN;
            checkResult.lastLostTime = 0;
        } else if (!needHealthCheck(slotInfo)) {
            CheckResult &checkResult = _checkResultMap[nodeId];
            checkResult.nodeId = nodeId;
            checkResult.healthInfo.version = version;
            checkResult.healthInfo.healthStatus = HT_UNKNOWN;
            checkResult.healthInfo.workerStatus = WT_UNKNOWN;
            checkResult.lastLostTime = 0;            
        } else {
            needCheckNodes.push_back(allCheckNodeVect[i]);
        }
    }
}

void Lv7HealthChecker::doCheck() {
    REPORT_USED_TIME(_id, METRIC_HEALTH_CHECK_TIME);
    vector<CheckNode> checkNodeVect;
    {
        ScopedLock lock(_lock);
        checkNodeVect = _checkNodeVect;
    }

    vector<CheckNode> needCheckNodes;
    set<nodeid_t> nodeIdSet;
    getNodesNeedCheck(checkNodeVect, needCheckNodes, nodeIdSet);

    vector<QueryResult> queryResultVect;
    query(needCheckNodes, &queryResultVect);
    updateCheckResult(needCheckNodes, queryResultVect, &_checkResultMap);

    HealthInfoMap healthInfoMap;
    for (map<nodeid_t, CheckResult>::iterator it = _checkResultMap.begin();
         it != _checkResultMap.end();)
    {
        const nodeid_t &nodeId = it->first;
        if (nodeIdSet.find(nodeId) == nodeIdSet.end()) {
            _checkResultMap.erase(it++);
        } else {
            healthInfoMap[nodeId] = it->second.healthInfo;
            it++;
        }
    }

    ScopedLock lock(_lock);
    _healthInfoMap.swap(healthInfoMap);
    CARBON_LOG(DEBUG, "check health infos done, healthInfos:[%s].",
               ToJsonString(_healthInfoMap).c_str());
}

void Lv7HealthChecker::query(const vector<CheckNode> &checkNodeVect,
                             vector<QueryResult> *queryResultVect)
{
    if (checkNodeVect.size() == 0) {
        return;
    }
    
    vector<string> urls;
    getQueryUrls(checkNodeVect, &urls);
    vector<string> queryDatas;
    getQueryDatas(checkNodeVect, &queryDatas);
    
    _httpClientPtr->query(urls, queryDatas, queryResultVect);
}

void Lv7HealthChecker::getQueryUrls(
        const vector<CheckNode> &checkNodeVect,
        vector<string> *urls) const
{
    urls->resize(checkNodeVect.size());

    for (size_t i = 0; i < checkNodeVect.size(); i++) {
        const CheckNode &checkNode = checkNodeVect[i];
        string ip = SlotUtil::getIp(checkNode.slotInfo);
        (*urls)[i] = "http://" + ip + ":" + StringUtil::toString(_port) + _checkPath;
    }
}

void Lv7HealthChecker::getQueryDatas(const vector<CheckNode> &checkNodeVect,
                                vector<string> *datas)
{
    return;
}

void Lv7HealthChecker::updateCheckResult(
        const vector<CheckNode> &checkNodeVect,
        const vector<QueryResult> &queryResultVect,
        map<nodeid_t, CheckResult> *checkResultMap)
{
    CARBON_LOG(DEBUG, "check node size:%zd, query result size:%zd.",
               checkNodeVect.size(), queryResultVect.size());
    assert(checkResultMap);
    assert(checkNodeVect.size() == queryResultVect.size());

    int64_t curTime = TimeUtility::currentTime();
    set<nodeid_t> checkNodeIdSet;
    for (size_t i = 0; i < checkNodeVect.size(); i++) {
        const QueryResult &queryResult = queryResultVect[i];
        const CheckNode &checkNode = checkNodeVect[i];
        const nodeid_t nodeId = checkNode.nodeId;
        version_t version = checkNode.version;
        checkNodeIdSet.insert(nodeId);
        CheckResult &checkResult = (*checkResultMap)[nodeId];
        checkResult.nodeId = nodeId;
        bool needUpdateHealthInfo = _healthStatusTransferPtr->transfer(
                queryResult.statusCode == 200, curTime, &checkResult);
        if (needUpdateHealthInfo) {
            KVMap resultMetas;
            parseHealthMetas(queryResult.content, &resultMetas);
            checkResult.healthInfo.metas = resultMetas;
            checkResult.healthInfo.version = version;
            checkResult.healthInfo.workerStatus =
                checkWorkerReady(checkNode, resultMetas);
        }
    }
}

void Lv7HealthChecker::fillNodeMetas(const WorkerNodePtr &workerNodePtr,
                                     KVMap *metas)
{
    
}

bool Lv7HealthChecker::parseHealthMetas(const string &content, KVMap *resultMetas) {
    return true;
}

WorkerType Lv7HealthChecker::checkWorkerReady(const CheckNode &checkNode,
                                              const KVMap &metas)
{
    return WT_READY;
}

bool Lv7HealthChecker::getValue(const KVMap &kvMap, const string &key,
                                string &value)
{
    KVMap::const_iterator it = kvMap.find(key);
    if (it == kvMap.end()) {
        return false;
    }

    value = it->second;
    return true;
}


END_CARBON_NAMESPACE(master);

