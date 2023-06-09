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
#include "ha3/search/OptimizerChain.h"

#include <algorithm>
#include <memory>

#include "autil/Log.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
#include "ha3/search/Optimizer.h"

using namespace isearch::common;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, OptimizerChain);

OptimizerChain::OptimizerChain() {
}

OptimizerChain::~OptimizerChain() {
}

void OptimizerChain::addOptimizer(const OptimizerPtr &optimizer) {
    _optimizers.push_back(optimizer);
}

bool OptimizerChain::optimize(OptimizeParam *param)
{
    const Request *request = param->request;
    bool useTruncate = request->getConfigClause()->useTruncateOptimizer();
    for (std::vector<OptimizerPtr>::const_iterator it = _optimizers.begin();
         it != _optimizers.end(); ++it)
    {
        const OptimizerPtr &optimzier = *it;
        if (!useTruncate) {
            optimzier->disableTruncate();
        }
        if (!optimzier->optimize(param)) {
            return false;
        }
    }
    return true;
}

} // namespace search
} // namespace isearch

