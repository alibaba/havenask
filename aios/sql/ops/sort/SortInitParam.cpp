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
#include "sql/ops/sort/SortInitParam.h"

#include <algorithm>
#include <cstddef>
#include <engine/NaviConfigContext.h>

#include "navi/engine/KernelConfigContext.h"
#include "sql/common/Log.h"
#include "sql/ops/util/KernelUtil.h"

using namespace std;

namespace sql {

AUTIL_LOG_SETUP(sql, SortInitParam);

SortInitParam::SortInitParam()
    : limit(0)
    , offset(0)
    , topk(0) {}

bool SortInitParam::initFromJson(navi::KernelConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "order_fields", keys);
    KernelUtil::stripName(keys);
    vector<string> orderStrVec;
    NAVI_JSONIZE(ctx, "directions", orderStrVec);
    if (keys.size() != orderStrVec.size()) {
        SQL_LOG(ERROR,
                "sort key & order size mismatch, "
                "key size [%lu], order size [%lu]",
                keys.size(),
                orderStrVec.size());
        return false;
    }
    if (keys.empty()) {
        SQL_LOG(ERROR, "sort key empty");
        return false;
    }
    for (const string &order : orderStrVec) {
        orders.emplace_back(order == "DESC");
    }
    NAVI_JSONIZE(ctx, "limit", limit);
    NAVI_JSONIZE(ctx, "offset", offset);
    topk = offset + limit;
    return true;
}

} // namespace sql
