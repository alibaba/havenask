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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "sql/ops/agg/Accumulator.h"
#include "sql/ops/agg/AggFunc.h"
#include "sql/ops/agg/AggFuncCreatorR.h"
#include "sql/ops/agg/AggFuncMode.h"
#include "table/Row.h"
#include "table/Table.h"

namespace table {
template <typename T>
class ColumnData;
} // namespace table

namespace sql {

class CountAccumulator : public Accumulator {
public:
    CountAccumulator()
        : value(0) {}
    ~CountAccumulator() = default;

public:
    int64_t value;
};

class CountAggFunc : public AggFunc {
public:
    CountAggFunc(const std::vector<std::string> &inputs,
                 const std::vector<std::string> &outputs,
                 AggFuncMode mode)
        : AggFunc(inputs, outputs, mode)
        , _inputColumn(NULL)
        , _countColumn(NULL) {}
    ~CountAggFunc() {}

private:
    CountAggFunc(const CountAggFunc &);
    CountAggFunc &operator=(const CountAggFunc &);

public:
    DEF_CREATE_ACCUMULATOR_FUNC(CountAccumulator)
    bool needDependInputTablePools() const override {
        return false;
    }

private:
    // local
    bool initCollectInput(const table::TablePtr &inputTable) override;
    bool initAccumulatorOutput(const table::TablePtr &outputTable) override;
    bool collect(table::Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, table::Row outputRow) const override;
    // global
    bool initMergeInput(const table::TablePtr &inputTable) override;
    bool initResultOutput(const table::TablePtr &outputTable) override;
    bool merge(table::Row inputRow, Accumulator *acc) override;
    bool outputResult(Accumulator *acc, table::Row outputRow) const override;

private:
    table::ColumnData<int64_t> *_inputColumn;
    table::ColumnData<int64_t> *_countColumn;
};

class CountAggFuncCreator : public AggFuncCreatorR {
public:
    std::string getResourceName() const override {
        return "SQL_AGG_FUNC_COUNT";
    }
    std::string getAggFuncName() const override {
        return "COUNT";
    }

private:
    AggFunc *createLocalFunction(const std::vector<table::ValueType> &inputTypes,
                                 const std::vector<std::string> &inputFields,
                                 const std::vector<std::string> &outputFields) override {
        return createFirstStageFunction(
            inputTypes, inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_LOCAL);
    }
    AggFunc *createNormalFunction(const std::vector<table::ValueType> &inputTypes,
                                  const std::vector<std::string> &inputFields,
                                  const std::vector<std::string> &outputFields) override {
        return createFirstStageFunction(
            inputTypes, inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_NORMAL);
    }
    AggFunc *createGlobalFunction(const std::vector<table::ValueType> &inputTypes,
                                  const std::vector<std::string> &inputFields,
                                  const std::vector<std::string> &outputFields) override;

private:
    AggFunc *createFirstStageFunction(const std::vector<table::ValueType> &inputTypes,
                                      const std::vector<std::string> &inputFields,
                                      const std::vector<std::string> &outputFields,
                                      AggFuncMode mode);
};

} // namespace sql
