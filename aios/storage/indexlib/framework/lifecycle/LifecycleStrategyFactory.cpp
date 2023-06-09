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
#include "indexlib/framework/lifecycle/LifecycleStrategyFactory.h"

#include "indexlib/framework/lifecycle/DynamicLifecycleStrategy.h"
#include "indexlib/framework/lifecycle/StaticLifecycleStrategy.h"

using namespace std;

namespace indexlib::framework {
AUTIL_LOG_SETUP(indexlib.framework, LifecycleStrategyFactory);

const string LifecycleStrategyFactory::DYNAMIC_STRATEGY = "dynamic";
const string LifecycleStrategyFactory::STATIC_STRATEGY = "static";

unique_ptr<LifecycleStrategy>
LifecycleStrategyFactory::CreateStrategy(const indexlib::file_system::LifecycleConfig& lifecycleConfig,
                                         const map<string, string>& parameters)
{
    const auto& strategy = lifecycleConfig.GetStrategy();
    if (strategy == DYNAMIC_STRATEGY) {
        indexlib::file_system::LifecycleConfig newConfig = lifecycleConfig;
        if (!newConfig.InitOffsetBase(parameters)) {
            AUTIL_LOG(ERROR, "set lifecycle config template parameter failed");
            return nullptr;
        }
        unique_ptr<LifecycleStrategy> strategy(new DynamicLifecycleStrategy(newConfig));
        return strategy;
    }
    if (strategy == STATIC_STRATEGY) {
        unique_ptr<LifecycleStrategy> strategy(new StaticLifecycleStrategy(lifecycleConfig));
        return strategy;
    }
    AUTIL_LOG(ERROR, "Invalid LifyecycleStrategy type [%s]", strategy.c_str());
    return nullptr;
}

} // namespace indexlib::framework
