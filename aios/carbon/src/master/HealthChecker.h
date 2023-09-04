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
#ifndef CARBON_HEALTHCHECKER_H
#define CARBON_HEALTHCHECKER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/CommonDefine.h"
#include "carbon/Status.h"
#include "autil/Lock.h"

BEGIN_CARBON_NAMESPACE(master);
class WorkerNode;
CARBON_TYPEDEF_PTR(WorkerNode);

typedef std::map<nodeid_t, HealthInfo> HealthInfoMap;

struct CheckNode {
    CheckNode() {
        lastSlaveDeadTime = 0;
    }
    
    nodeid_t nodeId;
    version_t version;
    SlotInfo slotInfo;
    KVMap metas;
    int64_t lastSlaveDeadTime;
};

class HealthChecker
{
public:
    HealthChecker(const std::string &id);
    virtual ~HealthChecker();
private:
    HealthChecker(const HealthChecker &);
    HealthChecker& operator=(const HealthChecker &);
public:
    virtual bool init(const std::map<std::string, std::string> &options) = 0;
                      
    void update(const std::vector<WorkerNodePtr> &workerNodes);

    void check();
    
    virtual HealthInfoMap getHealthInfos() const;

    std::string getId() const {return _id;}

    bool isWorking() {return _isWorking;}

protected:
    virtual void doUpdate(const std::vector<WorkerNodePtr> &workerNodes) = 0;

    virtual void doCheck() = 0;
    
protected:
    std::string _id;
    HealthInfoMap _healthInfoMap;
    bool _isUpdated;
    bool _isWorking;
    mutable autil::ThreadMutex _lock;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(HealthChecker);

END_CARBON_NAMESPACE(master);

#endif //CARBON_HEALTHCHECKER_H
