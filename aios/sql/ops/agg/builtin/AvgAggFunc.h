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

#include <assert.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "sql/ops/agg/Accumulator.h"
#include "sql/ops/agg/AggFunc.h"
#include "sql/ops/agg/AggFuncCreatorR.h"
#include "sql/ops/agg/AggFuncMode.h"
#include "table/ColumnData.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace sql {

template <typename T>
class AvgAccumulator : public Accumulator {
public:
    AvgAccumulator()
        : count(0)
        , sum(0) {}
    ~AvgAccumulator() = default;

public:
    uint64_t count;
    T sum;
};

template <typename InputType, typename AccumulatorType>
class AvgAggFunc : public AggFunc {
public:
    AvgAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               AggFuncMode mode)
        : AggFunc(inputs, outputs, mode)
        , _inputColumn(NULL)
        , _countColumn(NULL)
        , _sumColumn(NULL)
        , _avgColumn(NULL) {}
    ~AvgAggFunc() {}

private:
    AvgAggFunc(const AvgAggFunc &);
    AvgAggFunc &operator=(const AvgAggFunc &);

public:
    DEF_CREATE_ACCUMULATOR_FUNC(AvgAccumulator<AccumulatorType>)

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
    table::ColumnData<InputType> *_inputColumn;
    table::ColumnData<uint64_t> *_countColumn;
    table::ColumnData<AccumulatorType> *_sumColumn;
    table::ColumnData<double> *_avgColumn;
};

// local
template <typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::initCollectInput(const table::TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = table::TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template <typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::initAccumulatorOutput(
    const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 2);
    _countColumn
        = table::TableUtil::declareAndGetColumnData<uint64_t>(outputTable, _outputFields[0]);
    if (_countColumn == nullptr) {
        return false;
    }
    _sumColumn
        = table::TableUtil::declareAndGetColumnData<AccumulatorType>(outputTable, _outputFields[1]);
    if (_sumColumn == nullptr) {
        return false;
    }
    return true;
}

template <typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::collect(table::Row inputRow, Accumulator *acc) {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    avgAcc->count++;
    avgAcc->sum += _inputColumn->get(inputRow);
    return true;
}

template <typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::outputAccumulator(Accumulator *acc,
                                                               table::Row outputRow) const {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    _countColumn->set(outputRow, avgAcc->count);
    _sumColumn->set(outputRow, avgAcc->sum);
    return true;
}

// global
template <typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::initMergeInput(const table::TablePtr &inputTable) {
    assert(_inputFields.size() == 2);
    _countColumn = table::TableUtil::getColumnData<uint64_t>(inputTable, _inputFields[0]);
    if (_countColumn == nullptr) {
        return false;
    }
    _sumColumn = table::TableUtil::getColumnData<AccumulatorType>(inputTable, _inputFields[1]);
    if (_sumColumn == nullptr) {
        return false;
    }
    return true;
}

template <typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::initResultOutput(const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _avgColumn = table::TableUtil::declareAndGetColumnData<double>(outputTable, _outputFields[0]);
    if (_avgColumn == nullptr) {
        return false;
    }
    return true;
}

template <typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::merge(table::Row inputRow, Accumulator *acc) {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    avgAcc->count += _countColumn->get(inputRow);
    avgAcc->sum += _sumColumn->get(inputRow);
    return true;
}

template <typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::outputResult(Accumulator *acc,
                                                          table::Row outputRow) const {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    assert(avgAcc->count > 0);
    double avg = (double)avgAcc->sum / avgAcc->count;
    _avgColumn->set(outputRow, avg);
    return true;
}

class AvgAggFuncCreator : public AggFuncCreatorR {
public:
    std::string getResourceName() const override {
        return "SQL_AGG_FUNC_AVG";
    }
    std::string getAggFuncName() const override {
        return "AVG";
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
