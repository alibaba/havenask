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

namespace isearch {
namespace sql {

class PrintTableTvfFunc : public TvfFunc {
public:
    PrintTableTvfFunc();
    ~PrintTableTvfFunc();
    PrintTableTvfFunc(const PrintTableTvfFunc &) = delete;
    PrintTableTvfFunc &operator=(const PrintTableTvfFunc &) = delete;

private:
    bool init(TvfFuncInitContext &context) override;
    bool compute(const table::TablePtr &input, bool eof, table::TablePtr &output) override;

private:
    uint32_t _count;
};

typedef std::shared_ptr<PrintTableTvfFunc> PrintTableTvfFuncPtr;

class PrintTableTvfFuncCreator : public TvfFuncCreatorR {
public:
    PrintTableTvfFuncCreator();
    ~PrintTableTvfFuncCreator();

public:
    void def(navi::ResourceDefBuilder &builder) const override {
        builder.name("PRINT_TABLE_TVF_FUNC");
    }

private:
    PrintTableTvfFuncCreator(const PrintTableTvfFuncCreator &) = delete;
    PrintTableTvfFuncCreator &operator=(const PrintTableTvfFuncCreator &) = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};

typedef std::shared_ptr<PrintTableTvfFuncCreator> PrintTableTvfFuncCreatorPtr;
} // namespace sql
} // namespace isearch
