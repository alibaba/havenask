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
#include <string>
#include <vector>

#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/Span.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolVector.h"
#include "navi/common.h"
#include "sql/ops/agg/Accumulator.h"
#include "sql/ops/agg/AggFunc.h"
#include "sql/ops/agg/AggFuncCreatorR.h"
#include "sql/ops/agg/AggFuncMode.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace navi {
class ResourceInitContext;
} // namespace navi
namespace table {
template <typename T>
class ColumnData;
} // namespace table

namespace sql {

template <typename T>
class MultiGatherAccumulator : public Accumulator {
public:
    MultiGatherAccumulator(autil::mem_pool::Pool *pool)
        : poolVector(pool) {}
    ~MultiGatherAccumulator() = default;

public:
    autil::mem_pool::PoolVector<T> poolVector;
};

template <>
class MultiGatherAccumulator<autil::MultiChar> : public Accumulator {
public:
    MultiGatherAccumulator(autil::mem_pool::Pool *pool)
        : poolVector(pool) {}
    ~MultiGatherAccumulator() = default;

public:
    autil::mem_pool::PoolVector<autil::StringView> poolVector;
};

template <typename InputType>
class MultiGatherAggFunc : public AggFunc {
public:
    MultiGatherAggFunc(const std::vector<std::string> &inputs,
                       const std::vector<std::string> &outputs,
                       AggFuncMode mode)
        : AggFunc(inputs, outputs, mode)
        , _inputColumn(NULL)
        , _gatheredColumn(NULL)
        , _outputColumn(NULL) {}
    ~MultiGatherAggFunc() {}

private:
    MultiGatherAggFunc(const MultiGatherAggFunc &);
    MultiGatherAggFunc &operator=(const MultiGatherAggFunc &);

public:
    DEF_CREATE_ACCUMULATOR_FUNC(MultiGatherAccumulator<InputType>, _funcPool)

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
    table::ColumnData<autil::MultiValueType<InputType>> *_inputColumn;
    table::ColumnData<autil::MultiValueType<InputType>> *_gatheredColumn;
    table::ColumnData<autil::MultiValueType<InputType>> *_outputColumn;
};

// local
template <typename InputType>
bool MultiGatherAggFunc<InputType>::initCollectInput(const table::TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = table::TableUtil::getColumnData<autil::MultiValueType<InputType>>(
        inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template <typename InputType>
bool MultiGatherAggFunc<InputType>::initAccumulatorOutput(const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _gatheredColumn = table::TableUtil::declareAndGetColumnData<autil::MultiValueType<InputType>>(
        outputTable, _outputFields[0]);
    if (_gatheredColumn == nullptr) {
        return false;
    }
    return true;
}

template <typename InputType>
bool MultiGatherAggFunc<InputType>::collect(table::Row inputRow, Accumulator *acc) {
    MultiGatherAccumulator<InputType> *gatherAcc
        = static_cast<MultiGatherAccumulator<InputType> *>(acc);
    const autil::MultiValueType<InputType> &inputVals = _inputColumn->get(inputRow);
    for (size_t i = 0; i < inputVals.size(); ++i) {
        gatherAcc->poolVector.push_back(inputVals[i]);
    }
    return true;
}

template <typename InputType>
bool MultiGatherAggFunc<InputType>::outputAccumulator(Accumulator *acc,
                                                      table::Row outputRow) const {
    MultiGatherAccumulator<InputType> *gatherAcc
        = static_cast<MultiGatherAccumulator<InputType> *>(acc);
    _gatheredColumn->set(outputRow, gatherAcc->poolVector.data(), gatherAcc->poolVector.size());
    return true;
}

template <>
bool MultiGatherAggFunc<autil::MultiChar>::collect(table::Row inputRow, Accumulator *acc);

template <>
bool MultiGatherAggFunc<autil::MultiChar>::merge(table::Row inputRow, Accumulator *acc);

// global
template <typename InputType>
bool MultiGatherAggFunc<InputType>::initMergeInput(const table::TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _gatheredColumn = table::TableUtil::getColumnData<autil::MultiValueType<InputType>>(
        inputTable, _inputFields[0]);
    if (_gatheredColumn == nullptr) {
        return false;
    }
    return true;
}

template <typename InputType>
bool MultiGatherAggFunc<InputType>::initResultOutput(const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _outputColumn = table::TableUtil::declareAndGetColumnData<autil::MultiValueType<InputType>>(
        outputTable, _outputFields[0]);
    if (_outputColumn == nullptr) {
        return false;
    }
    return true;
}

template <typename InputType>
bool MultiGatherAggFunc<InputType>::merge(table::Row inputRow, Accumulator *acc) {
    MultiGatherAccumulator<InputType> *gatherAcc
        = static_cast<MultiGatherAccumulator<InputType> *>(acc);
    const autil::MultiValueType<InputType> &values = _gatheredColumn->get(inputRow);
    for (size_t i = 0; i < values.size(); i++) {
        gatherAcc->poolVector.push_back(values[i]);
    }
    return true;
}

template <typename InputType>
bool MultiGatherAggFunc<InputType>::outputResult(Accumulator *acc, table::Row outputRow) const {
    MultiGatherAccumulator<InputType> *gatherAcc
        = static_cast<MultiGatherAccumulator<InputType> *>(acc);
    _outputColumn->set(outputRow, gatherAcc->poolVector.data(), gatherAcc->poolVector.size());
    return true;
}

class MultiGatherAggFuncCreator : public AggFuncCreatorR {
public:
    std::string getResourceName() const override {
        return "SQL_AGG_FUNC_MULTIGATHER";
    }
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
    std::string getAggFuncName() const override {
        return "MULTIGATHER";
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
