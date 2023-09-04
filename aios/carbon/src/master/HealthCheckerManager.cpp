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
#include "master/HealthCheckerManager.h"
#include "carbon/Log.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, HealthCheckerManager);

HealthCheckerManager::HealthCheckerManager() {
    _healthCheckerCreatorPtr.reset(new HealthCheckerCreator());
}

HealthCheckerManager::~HealthCheckerManager() {
    _workLoopThreadPtr.reset();
}

bool HealthCheckerManager::start() {
    _workLoopThreadPtr = LoopThread::createLoopThread(
            std::bind(&HealthCheckerManager::check, this),
            HEALTH_CHECK_INTERVAL);
    if (_workLoopThreadPtr == NULL) {
        CARBON_LOG(ERROR, "create health check work loop thread failed.");
        return false;
    }
    if (!_runner.start()) {
        CARBON_LOG(ERROR, "start runner for healthCheckerManager.");
        return false;
    }

    CARBON_LOG(INFO, "start HealthCheckerManager success.");
    return true;
}

HealthCheckerPtr HealthCheckerManager::getHealthChecker(
        const string &id, const HealthCheckerConfig &config)
{
    ScopedLock lock(_lock);
    map<string, HealthCheckerPtr>::iterator it = _healthCheckerMap.find(id);
    if (it != _healthCheckerMap.end()) {
        return it->second;
    }

    CARBON_LOG(INFO, "create health checker, id:[%s].", id.c_str());
    HealthCheckerPtr healthCheckerPtr =
        _healthCheckerCreatorPtr->create(id, config);
    if (healthCheckerPtr != NULL) {
        _healthCheckerMap[id] = healthCheckerPtr;
    }

    CARBON_LOG(DEBUG, "health checker count:%zd", _healthCheckerMap.size());
    return healthCheckerPtr;
}

void HealthCheckerManager::check() {
    map<string, HealthCheckerPtr> healthCheckerMap;
    {
        ScopedLock lock(_lock);
        map<string, HealthCheckerPtr>::const_iterator it = _healthCheckerMap.begin();
        for (; it != _healthCheckerMap.end(); it++) {
            if (!it->second.unique()) {
                healthCheckerMap[it->first] = it->second;
            }
        }
        _healthCheckerMap = healthCheckerMap;
    }

    CARBON_LOG(DEBUG, "check health checker, size:%zd.", healthCheckerMap.size());
    vector<std::function<void()>> funcs;
    for (const auto& healthCheck : healthCheckerMap) {
        funcs.push_back([healthCheck](){healthCheck.second->check();});
    }
    _runner.Run(funcs);
}

END_CARBON_NAMESPACE(master);
