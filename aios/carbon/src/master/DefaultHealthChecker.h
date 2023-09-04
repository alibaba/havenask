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
#ifndef CARBON_DEFAULTHEALTHCHECKER_H
#define CARBON_DEFAULTHEALTHCHECKER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/HealthChecker.h"

BEGIN_CARBON_NAMESPACE(master);

class DefaultHealthChecker : public HealthChecker
{
public:
    DefaultHealthChecker(const std::string& id);
    ~DefaultHealthChecker();
private:
    DefaultHealthChecker(const DefaultHealthChecker &);
    DefaultHealthChecker& operator=(const DefaultHealthChecker &);
    
public:
    /* override */
    virtual bool init(const KVMap &options);

protected:
    /* override */
    virtual void doUpdate(const std::vector<WorkerNodePtr> &workerNodes);

    /* override */
    virtual void doCheck();

protected:
    HealthType checkSlotInfo(CheckNode &checkNode);

    std::string getOption(const KVMap &options, const std::string &k);
private:
    std::map<nodeid_t, CheckNode> _checkNodes;
    bool _ignoreProcessStatus;
    int64_t _slaveDeadTimeout;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(DefaultHealthChecker);

END_CARBON_NAMESPACE(master);

#endif //CARBON_DEFAULTHEALTHCHECKER_H
