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
#include "master/HealthCheckerCreator.h"
#include "master/Lv7HealthChecker.h"
#include "master/DefaultHealthChecker.h"
#include "master/AdvancedLv7HealthChecker.h"
#include "carbon/Log.h"

using namespace std;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, HealthCheckerCreator);

HealthCheckerCreator::HealthCheckerCreator() {
}

HealthCheckerCreator::~HealthCheckerCreator() {
}

HealthCheckerPtr HealthCheckerCreator::create(
        const string &id, const HealthCheckerConfig &config)
{
    const string &name = config.name;
    const KVMap &args = config.args;
    
    CARBON_LOG(INFO, "create health checker, name:[%s], id:[%s].",
               name.c_str(), id.c_str());
    
    HealthCheckerPtr healthCheckerPtr;
    if (name == "" || name == DEFAULT_HEALTH_CHECKER) {
        healthCheckerPtr.reset(new DefaultHealthChecker(id));
    } else if (name == LV7_HEALTH_CHECKER) {
        healthCheckerPtr.reset(new Lv7HealthChecker(id));
    } else if (name == ADVANCED_LV7_HEALTH_CHECKER) {
        healthCheckerPtr.reset(new AdvancedLv7HealthChecker(id));
    }
    
    if (healthCheckerPtr != NULL && healthCheckerPtr->init(args)) {
        return healthCheckerPtr;
    }
    
    CARBON_LOG(ERROR, "create health checker failed, name:[%s]", name.c_str());
    return HealthCheckerPtr();
}

END_CARBON_NAMESPACE(master);

