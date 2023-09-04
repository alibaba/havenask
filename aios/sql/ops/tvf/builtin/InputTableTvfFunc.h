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
#include <vector>

#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/ops/tvf/TvfFunc.h"
#include "sql/ops/tvf/TvfFuncCreatorR.h"
#include "table/Table.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace sql {
class SqlTvfProfileInfo;
} // namespace sql

namespace sql {

class InputTableTvfFunc : public OnePassTvfFunc {
public:
    InputTableTvfFunc();
    ~InputTableTvfFunc();
    InputTableTvfFunc(const InputTableTvfFunc &) = delete;
    InputTableTvfFunc &operator=(const InputTableTvfFunc &) = delete;

private:
    bool init(TvfFuncInitContext &context) override;
    bool doCompute(const table::TablePtr &input, table::TablePtr &output) override;
    bool fillInputTable(std::vector<std::vector<std::vector<std::string>>> &input,
                        table::TablePtr &output);
    bool prepareInputTableInfo(const std::string &input,
                               std::vector<std::vector<std::vector<std::string>>> &output);

private:
    autil::mem_pool::Pool *_queryPool {nullptr};
    std::vector<std::vector<std::vector<std::string>>> _inputTableInfo;
    char _rowSep {0};
    char _colSep {0};
    char _multiValueSep {0};
};

typedef std::shared_ptr<InputTableTvfFunc> InputTableTvfFuncPtr;

class InputTableTvfFuncCreator : public TvfFuncCreatorR {
public:
    InputTableTvfFuncCreator();
    ~InputTableTvfFuncCreator();

public:
    std::string getResourceName() const override {
        return "inputTableTvf";
    }

private:
    InputTableTvfFuncCreator(const InputTableTvfFuncCreator &) = delete;
    InputTableTvfFuncCreator &operator=(const InputTableTvfFuncCreator &) = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};
typedef std::shared_ptr<InputTableTvfFuncCreator> InputTableTvfFuncCreatorPtr;

class inputTableWithSepTvfFuncCreator : public TvfFuncCreatorR {
public:
    inputTableWithSepTvfFuncCreator();
    ~inputTableWithSepTvfFuncCreator();

public:
    std::string getResourceName() const override {
        return "inputTableWithSepTvf";
    }

private:
    inputTableWithSepTvfFuncCreator(const inputTableWithSepTvfFuncCreator &) = delete;
    inputTableWithSepTvfFuncCreator &operator=(const inputTableWithSepTvfFuncCreator &) = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};
typedef std::shared_ptr<inputTableWithSepTvfFuncCreator> inputTableWithSepTvfFuncCreatorPtr;

} // namespace sql
