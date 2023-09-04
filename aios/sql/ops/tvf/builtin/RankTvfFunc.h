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
#include "sql/ops/tvf/builtin/RankBaseTvfFunc.h"
#include "table/Table.h"

namespace sql {
class SqlTvfProfileInfo;
} // namespace sql

namespace sql {
class TvfFunc;

class RankTvfFunc : public RankBaseTvfFunc {
public:
    RankTvfFunc();
    ~RankTvfFunc();
    RankTvfFunc(const RankTvfFunc &) = delete;
    RankTvfFunc &operator=(const RankTvfFunc &) = delete;

protected:
    bool doCompute(const table::TablePtr &input, table::TablePtr &output) override;
};

typedef std::shared_ptr<RankTvfFunc> RankTvfFuncPtr;

class RankTvfFuncCreator : public TvfFuncCreatorR {
public:
    RankTvfFuncCreator();
    ~RankTvfFuncCreator();

public:
    std::string getResourceName() const override {
        return "rankTvf";
    }

private:
    RankTvfFuncCreator(const RankTvfFuncCreator &) = delete;
    RankTvfFuncCreator &operator=(const RankTvfFuncCreator &) = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};

typedef std::shared_ptr<RankTvfFuncCreator> RankTvfFuncCreatorPtr;
} // namespace sql
