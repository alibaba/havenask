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
#include <string>

#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/ops/tvf/TvfFuncCreatorR.h"
#include "sql/ops/tvf/builtin/SortTvfFunc.h"
#include "table/Table.h"

namespace sql {
class SqlTvfProfileInfo;
} // namespace sql

namespace sql {
class TvfFunc;

class TopKMarkTvfFuncCreator : public TvfFuncCreatorR {
public:
    TopKMarkTvfFuncCreator();
    ~TopKMarkTvfFuncCreator();

public:
    std::string getResourceName() const override {
        return "topKMarkTvf";
    }

private:
    TopKMarkTvfFuncCreator(const TopKMarkTvfFuncCreator &) = delete;
    TopKMarkTvfFuncCreator &operator=(const TopKMarkTvfFuncCreator &) = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};

class TopKMarkTvfFunc : public SortTvfFunc {
public:
    TopKMarkTvfFunc()
        : SortTvfFunc(false)
        , _markColumn("topKMark") {}

public:
    bool compute(const table::TablePtr &input, bool eof, table::TablePtr &output) override;
    bool doCompute(const table::TablePtr &input);

private:
    std::string _markColumn;
};

typedef std::shared_ptr<TopKMarkTvfFuncCreator> TopKMarkTvfFuncCreatorPtr;
} // namespace sql
