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
#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "autil/Log.h" // IWYU pragma: keep
#include "build_service/plugin/PlugInManager.h"
#include "ha3/search/Optimizer.h"
#include "ha3/search/OptimizerChain.h"
#include "ha3/config/ResourceReader.h"

namespace isearch {
namespace common {
class Request;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace search {

class OptimizerChainManager : public build_service::plugin::PlugInManager
{
public:
    OptimizerChainManager(config::ResourceReaderPtr &resourceReaderPtr,
                          const std::string &moduleSuffix);
    ~OptimizerChainManager();
private:
    OptimizerChainManager(const OptimizerChainManager &);
    OptimizerChainManager& operator=(const OptimizerChainManager &);
public:
    OptimizerChainPtr createOptimizerChain(const common::Request *request) const;
    bool addOptimizer(const OptimizerPtr &optimizerPtr);
private:
    std::unordered_map<std::string, OptimizerPtr> _optimizers;
private:
    AUTIL_LOG_DECLARE();
    friend class OptimizerChainManagerCreatorTest;
    friend class OptimizerChainManagerTest;    
};

typedef std::shared_ptr<OptimizerChainManager> OptimizerChainManagerPtr;

} // namespace search
} // namespace isearch

