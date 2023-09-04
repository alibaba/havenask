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

#include <stddef.h>
#include <string>
#include <vector>

#include "suez/turing/expression/common.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace matchdoc {
template <typename T>
class Reference;
} // namespace matchdoc

namespace suez {
namespace turing {
class RankAttributeExpression;
class RankManager;

class RankAttributeExprCreator {
public:
    RankAttributeExprCreator(autil::mem_pool::Pool *pool, RankManager *rankManager);
    ~RankAttributeExprCreator();

private:
    RankAttributeExprCreator(const RankAttributeExprCreator &);
    RankAttributeExprCreator &operator=(const RankAttributeExprCreator &);

public:
    RankAttributeExpression *createRankExpr(const std::string &name, matchdoc::Reference<score_t> *scoreRef = NULL);

private:
    autil::mem_pool::Pool *_pool;
    RankManager *_rankManager;
    std::vector<RankAttributeExpression *> _rankExprs;
};

} // namespace turing
} // namespace suez
