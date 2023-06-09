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
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorR.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "table/Row.h"
#include "table/Table.h"

namespace isearch {
namespace sql {
class SqlTvfProfileInfo;
} // namespace sql
} // namespace isearch
namespace table {
class Column;
} // namespace table

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace isearch {
namespace sql {

class UnPackMultiValueTvfFunc : public TvfFunc {
public:
    UnPackMultiValueTvfFunc();
    ~UnPackMultiValueTvfFunc();

private:
    UnPackMultiValueTvfFunc(const UnPackMultiValueTvfFunc &) = delete;
    UnPackMultiValueTvfFunc &operator=(const UnPackMultiValueTvfFunc &) = delete;

public:
    bool init(TvfFuncInitContext &context) override;
    bool compute(const table::TablePtr &input, bool eof, table::TablePtr &output) override;

private:
    bool unpackMultiValue(table::Column *column, table::TablePtr table);

    bool unpackTable(const std::vector<std::pair<int32_t, int32_t>> &unpackVec,
                     const std::string &unpackColName,
                     table::TablePtr &unpackTable);

    bool genColumnUnpackOffsetVec(table::Column *column,
                                  std::vector<std::pair<int32_t, int32_t>> &toMergeVec);

    bool copyNormalCol(size_t rawRowCount,
                       const std::vector<table::Row> &allRow,
                       const std::vector<std::pair<int32_t, int32_t>> &unpackVec,
                       table::Column *column);
    bool copyUnpackCol(size_t rawRowCount,
                       const std::vector<table::Row> &allRow,
                       const std::vector<std::pair<int32_t, int32_t>> &unpackVec,
                       table::Column *column);

private:
    std::vector<std::string> _unpackFields;
    autil::mem_pool::Pool *_queryPool;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<UnPackMultiValueTvfFunc> UnPackMultiValueTvfFuncPtr;

class UnPackMultiValueTvfFuncCreator : public TvfFuncCreatorR {
public:
    UnPackMultiValueTvfFuncCreator();
    ~UnPackMultiValueTvfFuncCreator();

public:
    void def(navi::ResourceDefBuilder &builder) const override {
        builder.name("UNPACK_MULTIVALUE_TVF_FUNC");
    }

private:
    UnPackMultiValueTvfFuncCreator(const UnPackMultiValueTvfFuncCreator &) = delete;
    UnPackMultiValueTvfFuncCreator &operator=(const UnPackMultiValueTvfFuncCreator &) = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<UnPackMultiValueTvfFuncCreator> UnPackMultiValueTvfFuncCreatorPtr;
} // namespace sql
} // namespace isearch
