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
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/Optimizer.h"

namespace isearch {
namespace search {

class OptimizerChain
{
public:
    OptimizerChain();
    ~OptimizerChain();
private:
    OptimizerChain(const OptimizerChain &);
    OptimizerChain& operator=(const OptimizerChain &);
public:
    void addOptimizer(const OptimizerPtr &optimizer);
    bool optimize(OptimizeParam *param);
private:
    std::vector<OptimizerPtr> _optimizers;
private:
    AUTIL_LOG_DECLARE();
private:
    friend class OptimizerChainManagerCreatorTest;
    friend class OptimizerChainManagerTest;
};

typedef std::shared_ptr<OptimizerChain> OptimizerChainPtr;

} // namespace search
} // namespace isearch

