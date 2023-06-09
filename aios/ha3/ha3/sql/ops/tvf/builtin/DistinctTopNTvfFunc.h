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

class DistinctTopNTvfFunc : public OnePassTvfFunc {
public:
    DistinctTopNTvfFunc();
    ~DistinctTopNTvfFunc();
    DistinctTopNTvfFunc(const DistinctTopNTvfFunc &) = delete;
    DistinctTopNTvfFunc &operator=(const DistinctTopNTvfFunc &) = delete;

private:
    bool init(TvfFuncInitContext &context) override;
    bool doCompute(const table::TablePtr &input, table::TablePtr &output) override;

private:
    bool computeTable(const table::TablePtr &input);

private:
    std::vector<std::string> _groupFields;
    std::vector<std::string> _sortFields;
    std::vector<bool> _sortFlags;
    std::string _sortStr;
    std::string _groupStr;
    int64_t _totalLimit;
    int64_t _perGroupLimit;
    int64_t _partTotalLimit;
    int64_t _partPerGroupLimit;
    autil::mem_pool::Pool *_queryPool;
};

typedef std::shared_ptr<DistinctTopNTvfFunc> DistinctTopNTvfFuncPtr;

class DistinctTopNTvfFuncCreator : public TvfFuncCreatorR {
public:
    DistinctTopNTvfFuncCreator();
    ~DistinctTopNTvfFuncCreator();

public:
    void def(navi::ResourceDefBuilder &builder) const override {
        builder.name("DISTINCT_TOPN_TVF_FUNC");
    }

private:
    DistinctTopNTvfFuncCreator(const DistinctTopNTvfFuncCreator &) = delete;
    DistinctTopNTvfFuncCreator &operator=(const DistinctTopNTvfFuncCreator &) = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};

typedef std::shared_ptr<DistinctTopNTvfFuncCreator> DistinctTopNTvfFuncCreatorPtr;
} // namespace sql
} // namespace isearch
