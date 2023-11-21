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

#include <string>
#include <vector>

#include "sql/common/Log.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "sql/ops/tvf/TvfFuncCreatorR.h"
#include "table/Table.h"

namespace sql {
class SqlTvfProfileInfo;
}

namespace sql {

class TransRowsTvfFunc : public TvfFunc {
public:
    TransRowsTvfFunc();
    virtual ~TransRowsTvfFunc();
    TransRowsTvfFunc(const TransRowsTvfFunc &) = delete;
    TransRowsTvfFunc &operator=(const TransRowsTvfFunc &) = delete;

public:
    bool init(TvfFuncInitContext &context) override;
    bool compute(const table::TablePtr &input, bool eof, table::TablePtr &output) override;

protected:
    std::string _multiValueSep = ":";
    std::string _transField;
    autil::mem_pool::Pool *_dataPool = nullptr;
};

typedef std::shared_ptr<TransRowsTvfFunc> TransRowsTvfFuncPtr;

class TransRowsTvfFuncCreator : public TvfFuncCreatorR {
public:
    TransRowsTvfFuncCreator();
    ~TransRowsTvfFuncCreator();

public:
    std::string getResourceName() const override {
        return "transRowsTvf";
    }

private:
    TransRowsTvfFuncCreator(const TransRowsTvfFuncCreator &) = delete;
    TransRowsTvfFuncCreator &operator=(const TransRowsTvfFuncCreator &) = delete;

    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};

typedef std::shared_ptr<TransRowsTvfFuncCreator> TransRowsTvfFuncCreatorPtr;

} // namespace sql
