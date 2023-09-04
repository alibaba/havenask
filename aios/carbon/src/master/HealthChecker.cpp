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
#include "master/HealthChecker.h"
#include "carbon/Log.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, HealthChecker);

HealthChecker::HealthChecker(const string &id) {
    _id = id;
    _isUpdated = false;
    _isWorking = false;
}

HealthChecker::~HealthChecker() {
}

void HealthChecker::update(const vector<WorkerNodePtr> &workerNodes) {
    doUpdate(workerNodes);
    _isUpdated = true;
}

void HealthChecker::check() {
    if (!_isUpdated) {
        return;
    }

    doCheck();
    _isWorking = true;
}

HealthInfoMap HealthChecker::getHealthInfos() const {
    ScopedLock lock(_lock);
    return _healthInfoMap;
}

END_CARBON_NAMESPACE(master);

