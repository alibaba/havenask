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
#include "ha3/sql/ops/tvf/builtin/RankBaseTvfFunc.h"

#include <ext/alloc_traits.h>
#include <algorithm>
#include <cstddef>
#include <memory>
#include <set>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/tvf/SqlTvfProfileInfo.h"
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "matchdoc/MatchDoc.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Resource.h"
#include "table/ComboComparator.h"
#include "table/ComparatorCreator.h"
#include "table/Row.h"
#include "table/TableUtil.h"

using namespace std;
using namespace autil;
using namespace table;

namespace isearch {
namespace sql {
RankBaseTvfFunc::RankBaseTvfFunc() {
}

RankBaseTvfFunc::~RankBaseTvfFunc() {}

bool RankBaseTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 3) {
        SQL_LOG(WARN, "param size [%ld] not equal 3", context.params.size());
        return false;
    }
    _partitionStr = context.params[0];
    _partitionFields = StringUtil::split(context.params[0], ",");
    _sortStr = context.params[1];
    _sortFields = StringUtil::split(context.params[1], ",");
    if (_sortFields.size() == 0) {
        SQL_LOG(WARN, "parse order [%s] fail.", context.params[1].c_str());
        return false;
    }
    for (size_t i = 0; i < _sortFields.size(); i++) {
        const string &field = _sortFields[i];
        if (field.size() == 0) {
            SQL_LOG(WARN, "parse order [%s] fail.", context.params[1].c_str());
            return false;
        }
        if (field[0] != '+' && field[0] != '-') {
            _sortFlags.push_back(false);
        } else if (field[0] == '+') {
            _sortFlags.push_back(false);
            _sortFields[i] = field.substr(1);
        } else {
            _sortFlags.push_back(true);
            _sortFields[i] = field.substr(1);
        }
    }
    if (!StringUtil::fromString(context.params[2], _count)) {
        SQL_LOG(WARN, "parse param [%s] fail.", context.params[2].c_str());
        return false;
    }
    if (_count < 1) {
        SQL_LOG(WARN, "parse param [%s] fail. count < 1", context.params[2].c_str());
        return false;
    }
    _queryPool = context.queryPool;
    return true;
}
} // namespace sql
} // namespace isearch
