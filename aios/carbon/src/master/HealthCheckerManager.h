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
#ifndef CARBON_HEALTHCHECKERMANAGER_H
#define CARBON_HEALTHCHECKERMANAGER_H

#include "common/common.h"
#include "common/MultiThreadRunner.h"
#include "carbon/Log.h"
#include "master/HealthCheckerCreator.h"
#include "autil/LoopThread.h"

BEGIN_CARBON_NAMESPACE(master);

class HealthCheckerManager
{
public:
    HealthCheckerManager();
    virtual ~HealthCheckerManager();
private:
    HealthCheckerManager(const HealthCheckerManager &);
    HealthCheckerManager& operator=(const HealthCheckerManager &);
public:
    bool start();

    virtual HealthCheckerPtr getHealthChecker(const std::string &id,
            const HealthCheckerConfig &config);

public:
    /* only for test */
    void setHealthChecker(const std::string &id, const HealthCheckerPtr &healthCheckerPtr)
    {
        _healthCheckerMap[id] = healthCheckerPtr;
    }

    /* public for test */
    void check();

private:
    HealthCheckerCreatorPtr _healthCheckerCreatorPtr;
    std::map<std::string, HealthCheckerPtr> _healthCheckerMap;
    autil::ThreadMutex _lock;

    common::MultiThreadRunner _runner;
    autil::LoopThreadPtr _workLoopThreadPtr;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(HealthCheckerManager);

END_CARBON_NAMESPACE(master);

#endif //CARBON_HEALTHCHECKERMANAGER_H
