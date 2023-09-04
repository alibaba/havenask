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
#include "suez/turing/expression/framework/RankAttributeExprCreator.h"

#include <algorithm>
#include <cstddef>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "suez/turing/expression/framework/RankAttributeExpression.h"
#include "suez/turing/expression/plugin/RankManager.h"
#include "suez/turing/expression/plugin/Scorer.h"

namespace matchdoc {
template <typename T>
class Reference;
} // namespace matchdoc

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(expression, RankAttributeExprCreator);

namespace suez {
namespace turing {

RankAttributeExprCreator::RankAttributeExprCreator(autil::mem_pool::Pool *pool, RankManager *rankManager)
    : _pool(pool), _rankManager(rankManager) {}

RankAttributeExprCreator::~RankAttributeExprCreator() {
    for (auto expr : _rankExprs) {
        POOL_DELETE_CLASS(expr);
    }
    _rankExprs.clear();
}

RankAttributeExpression *RankAttributeExprCreator::createRankExpr(const std::string &name,
                                                                  matchdoc::Reference<score_t> *scoreRef) {
    Scorer *scorer = NULL;
    if (_rankManager) {
        scorer = _rankManager->getScorer(name);
    }
    if (!scorer) {
        AUTIL_LOG(WARN, "create scorer: %s failed", name.c_str());
        return NULL;
    }
    auto expr = POOL_NEW_CLASS(_pool, RankAttributeExpression, scorer->clone(), scoreRef);
    _rankExprs.push_back(expr);
    return expr;
}

} // namespace turing
} // namespace suez
