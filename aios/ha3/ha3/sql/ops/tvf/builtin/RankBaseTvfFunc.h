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
#include <stdint.h>
#include <string>
#include <vector>

#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorR.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "table/Table.h"

namespace isearch {
namespace sql {
class SqlTvfProfileInfo;
} // namespace sql
} // namespace isearch

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace isearch {
namespace sql {

class RankBaseTvfFunc : public OnePassTvfFunc {
public:
    RankBaseTvfFunc();
    ~RankBaseTvfFunc();
    RankBaseTvfFunc(const RankBaseTvfFunc &) = delete;
    RankBaseTvfFunc &operator=(const RankBaseTvfFunc &) = delete;

protected:
    bool init(TvfFuncInitContext &context) override;
    virtual bool doCompute(const table::TablePtr &input, table::TablePtr &output) override = 0;

protected:
    std::vector<std::string> _partitionFields;
    std::vector<std::string> _sortFields;
    std::vector<bool> _sortFlags;
    std::string _sortStr;
    std::string _partitionStr;
    int32_t _count;
    autil::mem_pool::Pool *_queryPool;
};
} // namespace sql
} // namespace isearch
