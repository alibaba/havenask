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
#ifndef CARBON_ADVANCEDLV7HEALTHCHECKER_H
#define CARBON_ADVANCEDLV7HEALTHCHECKER_H

#include "carbon/CommonDefine.h"
#include "common/common.h"
#include "carbon/Log.h"
#include "master/Lv7HealthChecker.h"

BEGIN_CARBON_NAMESPACE(master);

class AdvancedLv7HealthChecker : public Lv7HealthChecker
{
public:
    AdvancedLv7HealthChecker(const std::string &id);
    ~AdvancedLv7HealthChecker();
private:
    AdvancedLv7HealthChecker(const AdvancedLv7HealthChecker &);
    AdvancedLv7HealthChecker& operator=(const AdvancedLv7HealthChecker &);
    
protected:
    /* override */
    void getQueryDatas(const std::vector<CheckNode> &checkNodeVect,
                       std::vector<std::string> *datas);

    /* override */
    void fillNodeMetas(const WorkerNodePtr &workerNodePtr, KVMap *metas);
    
    /* override */
    bool parseHealthMetas(const std::string &content, KVMap *metas);

    /* override */
    WorkerType checkWorkerReady(const CheckNode &checkNode, const KVMap &metas);
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(AdvancedLv7HealthChecker);

END_CARBON_NAMESPACE(master);

#endif //CARBON_ADVANCEDLV7HEALTHCHECKER_H
