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

#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/ops/tvf/TvfFunc.h"
#include "sql/ops/tvf/TvfFuncCreatorR.h"
#include "table/Table.h"

namespace sql {
class SqlTvfProfileInfo;
} // namespace sql

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace sql {

class PartialDistinctTopNTvfFunc : public OnePassTvfFunc {
public:
    PartialDistinctTopNTvfFunc();
    ~PartialDistinctTopNTvfFunc();
    PartialDistinctTopNTvfFunc(const PartialDistinctTopNTvfFunc &) = delete;
    PartialDistinctTopNTvfFunc &operator=(const PartialDistinctTopNTvfFunc &) = delete;

protected:
    bool init(TvfFuncInitContext &context) override;
    bool doCompute(const table::TablePtr &input, table::TablePtr &output) override;

private:
    bool computeTable(const table::TablePtr &input);

protected:
    std::vector<std::string> _groupFields;
    std::vector<std::string> _sortFields;
    std::vector<bool> _sortFlags;
    std::string _sortStr;
    std::string _groupStr;
    int64_t _totalLimit;
    int64_t _perGroupLimit;
    autil::mem_pool::Pool *_queryPool;
};

typedef std::shared_ptr<PartialDistinctTopNTvfFunc> PartialDistinctTopNTvfFuncPtr;

class PartialDistinctTopNTvfFuncCreator : public TvfFuncCreatorR {
public:
    PartialDistinctTopNTvfFuncCreator();
    ~PartialDistinctTopNTvfFuncCreator();

public:
    std::string getResourceName() const override {
        return "partialDistinctTopNTvf";
    }

private:
    PartialDistinctTopNTvfFuncCreator(const PartialDistinctTopNTvfFuncCreator &) = delete;
    PartialDistinctTopNTvfFuncCreator &operator=(const PartialDistinctTopNTvfFuncCreator &)
        = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};

class DistinctTopNTvfFunc : public PartialDistinctTopNTvfFunc {
public:
    DistinctTopNTvfFunc();
    ~DistinctTopNTvfFunc();
    DistinctTopNTvfFunc(const DistinctTopNTvfFunc &) = delete;
    DistinctTopNTvfFunc &operator=(const DistinctTopNTvfFunc &) = delete;

private:
    bool init(TvfFuncInitContext &context) override;
};

typedef std::shared_ptr<DistinctTopNTvfFunc> DistinctTopNTvfFuncPtr;

class DistinctTopNTvfFuncCreator : public TvfFuncCreatorR {
public:
    DistinctTopNTvfFuncCreator();
    ~DistinctTopNTvfFuncCreator();

public:
    std::string getResourceName() const override {
        return "distinctTopNTvf";
    }

private:
    DistinctTopNTvfFuncCreator(const DistinctTopNTvfFuncCreator &) = delete;
    DistinctTopNTvfFuncCreator &operator=(const DistinctTopNTvfFuncCreator &) = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};

typedef std::shared_ptr<DistinctTopNTvfFuncCreator> DistinctTopNTvfFuncCreatorPtr;

} // namespace sql
