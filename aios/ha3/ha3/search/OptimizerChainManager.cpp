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
#include "ha3/search/OptimizerChainManager.h"

#include <assert.h>
#include <stdint.h>
#include <iosfwd>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "build_service/plugin/PlugInManager.h"

#include "ha3/common/OptimizerClause.h"
#include "ha3/common/Request.h"
#include "ha3/search/OptimizerChain.h"
#include "autil/Log.h"

using namespace std;
using namespace isearch::common;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, OptimizerChainManager);

OptimizerChainManager::OptimizerChainManager(
        config::ResourceReaderPtr &resourceReaderPtr, const string &moduleSuffix)
    : build_service::plugin::PlugInManager(resourceReaderPtr, moduleSuffix)
{
}

OptimizerChainManager::~OptimizerChainManager() {
    _optimizers.clear();
}

bool OptimizerChainManager::addOptimizer(const OptimizerPtr &optimizerPtr) {
    assert(optimizerPtr);
    const string &optimizerName = optimizerPtr->getName();
    unordered_map<std::string, OptimizerPtr>::const_iterator iter = _optimizers.find(optimizerName);
    if (iter != _optimizers.end()) {
        AUTIL_LOG(ERROR, "duplicate optimizer name[%s]", optimizerName.c_str());
        return false;
    }
    _optimizers[optimizerName] = optimizerPtr;
    return true;
}

OptimizerChainPtr OptimizerChainManager::createOptimizerChain(const common::Request *request) const
{
    common::OptimizerClause *optimizerClause = request->getOptimizerClause();
    assert(optimizerClause);

    OptimizerChainPtr optimizerChainPtr(new OptimizerChain);
    uint32_t optimizeCount = optimizerClause->getOptimizeCount();
    if (optimizeCount != optimizerClause->getOptimizeOptionCount()) {
        AUTIL_LOG(WARN, "optimizer[%s] lacks options", optimizerClause->toString().c_str());
        return OptimizerChainPtr();
    }
    for (uint32_t i = 0; i < optimizeCount; ++i) {
        const string &optimizeName = optimizerClause->getOptimizeName(i);
        unordered_map<string, OptimizerPtr>::const_iterator it = _optimizers.find(optimizeName);
        if (it == _optimizers.end()) {
            AUTIL_LOG(WARN, "Can not find optimizer[%s]", optimizeName.c_str());
            return OptimizerChainPtr();
        }
        const string &option = optimizerClause->getOptimizeOption(i);
        OptimizeInitParam param(option, request);
        OptimizerPtr optimizer = it->second->clone();
        if (!optimizer->init(&param)) {
            AUTIL_LOG(WARN, "Init optimizer[%s] failed", optimizeName.c_str());
            return OptimizerChainPtr();
        }
        optimizerChainPtr->addOptimizer(optimizer);
    }
    return optimizerChainPtr;
}

} // namespace search
} // namespace isearch

