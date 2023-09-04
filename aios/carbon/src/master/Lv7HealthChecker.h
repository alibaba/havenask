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
#ifndef CARBON_LV7HEALTHCHECKER_H
#define CARBON_LV7HEALTHCHECKER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/HealthChecker.h"
#include "master/HttpClient.h"
#include "master/HealthStatusTransfer.h"

BEGIN_CARBON_NAMESPACE(master);

struct CheckResult {
    CheckResult() {
        lastLostTime = 0;
        lostCount = 0;
    }

    nodeid_t nodeId;
    HealthInfo healthInfo;
    int64_t lastLostTime;
    uint32_t lostCount;
};
    
class Lv7HealthChecker : public HealthChecker
{
public:
    Lv7HealthChecker(const std::string &id);
    ~Lv7HealthChecker();
private:
    Lv7HealthChecker(const Lv7HealthChecker &);
    Lv7HealthChecker& operator=(const Lv7HealthChecker &);
public:
    /* override */
    bool init(const std::map<std::string, std::string> &options);
    
    /* override */
    void doUpdate(const std::vector<WorkerNodePtr> &workerNodes);

    /* override */
    void doCheck();
    
public: /* for test */
    void setHttpClient(const HttpClientPtr &httpClientPtr) {
        _httpClientPtr = httpClientPtr;
    }

    int32_t getPort() const { return _port; }

    std::string getCheckPath() const { return _checkPath; }

    int64_t getLostTimeout() const { return _lostTimeout; }

protected:
    virtual void getQueryDatas(const std::vector<CheckNode> &checkNodeVect,
                               std::vector<std::string> *datas);

    virtual void fillNodeMetas(const WorkerNodePtr &workerNodePtr,
                               KVMap *metas);

    virtual bool parseHealthMetas(const std::string &content, KVMap *resultMetas);

    virtual WorkerType checkWorkerReady(const CheckNode &checkNode,
                                const KVMap &metas);
    
private:
    bool needHealthCheck(const SlotInfo &slotInfo) const;
    void getQueryUrls(const std::vector<CheckNode> &checkNodeVect,
                      std::vector<std::string> *urls) const;

    void updateCheckResult(const std::vector<CheckNode> &checkNodeVect,
                           const std::vector<QueryResult> &queryResult,
                           std::map<nodeid_t, CheckResult> *checkResultMap);
    
    void query(const std::vector<CheckNode> &needCheckNodes,
               std::vector<QueryResult> *queryResultVect);

    bool getValue(const KVMap &kvMap, const std::string &key,
                  std::string &value);

    void getNodesNeedCheck(const std::vector<CheckNode> &allCheckNodeVect,
                           std::vector<CheckNode> &needCheckNodes,
                           std::set<nodeid_t> &allNodeIdSet);

protected:
    int32_t _port;
    std::string _checkPath;
    int64_t _lostTimeout;
    std::vector<CheckNode> _checkNodeVect;
    std::map<nodeid_t, CheckResult> _checkResultMap;
    HttpClientPtr _httpClientPtr;
    HealthStatusTransferPtr _healthStatusTransferPtr;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(Lv7HealthChecker);

END_CARBON_NAMESPACE(master);

#endif //CARBON_LV7HEALTHCHECKER_H
