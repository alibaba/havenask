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
#include "table/ComboComparator.h"
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

class SortTvfFunc : public TvfFunc {
public:
    SortTvfFunc(bool ordered = true);
    virtual ~SortTvfFunc();
    SortTvfFunc(const SortTvfFunc &) = delete;
    SortTvfFunc &operator=(const SortTvfFunc &) = delete;

public:
    bool init(TvfFuncInitContext &context) override;
    bool compute(const table::TablePtr &input, bool eof, table::TablePtr &output) override;

private:
    bool doCompute(const table::TablePtr &input);

protected:
    std::vector<std::string> _sortFields;
    std::vector<bool> _sortFlags;
    table::ComboComparatorPtr _comparator;
    table::TablePtr _table;
    std::string _sortStr;
    int32_t _count;
    autil::mem_pool::Pool *_queryPool;
    bool _ordered;
};

typedef std::shared_ptr<SortTvfFunc> SortTvfFuncPtr;

class SortTvfFuncCreator : public TvfFuncCreatorR {
public:
    SortTvfFuncCreator();
    ~SortTvfFuncCreator();

public:
    void def(navi::ResourceDefBuilder &builder) const override {
        builder.name("SORT_TVF_FUNC");
    }

private:
    SortTvfFuncCreator(const SortTvfFuncCreator &) = delete;
    SortTvfFuncCreator &operator=(const SortTvfFuncCreator &) = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};

typedef std::shared_ptr<SortTvfFuncCreator> SortTvfFuncCreatorPtr;
} // namespace sql
} // namespace isearch
