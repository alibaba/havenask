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
#include <limits>
#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "navi/common.h"
#include "sql/common/Log.h"
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

template <typename T1, typename T2>
class MaxLabelAccumulator : public Accumulator {
public:
    MaxLabelAccumulator()
        : max(std::numeric_limits<T2>::lowest()) {}
    ~MaxLabelAccumulator() = default;

public:
    T1 label;
    T2 max;
};

template <typename InputType1, typename InputType2>
class MaxLabelAggFunc : public AggFunc {
public:
    using AccumulatorType = MaxLabelAccumulator<InputType1, InputType2>;

public:
    MaxLabelAggFunc(const std::vector<std::string> &inputs,
                    const std::vector<std::string> &outputs,
                    AggFuncMode mode)
        : AggFunc(inputs, outputs, mode)
        , _inputlabelColumn(NULL)
        , _inputValueColumn(NULL)
        , _labelColumn(NULL)
        , _maxColumn(NULL) {}
    ~MaxLabelAggFunc() {}

private:
    MaxLabelAggFunc(const MaxLabelAggFunc &);
    MaxLabelAggFunc &operator=(const MaxLabelAggFunc &);

public:
    DEF_CREATE_ACCUMULATOR_FUNC(AccumulatorType)
    bool needDependInputTablePools() const override;

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
    table::ColumnData<InputType1> *_inputlabelColumn;
    table::ColumnData<InputType2> *_inputValueColumn;
    table::ColumnData<InputType1> *_labelColumn;
    table::ColumnData<InputType2> *_maxColumn;
};

template <typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::needDependInputTablePools() const {
    return autil::IsMultiType<InputType1>::type::value
           || autil::IsMultiType<InputType2>::type::value;
}

// local
template <typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::initCollectInput(const table::TablePtr &inputTable) {
    assert(_inputFields.size() == 2);
    _inputlabelColumn = table::TableUtil::getColumnData<InputType1>(inputTable, _inputFields[0]);
    if (_inputlabelColumn == nullptr) {
        return false;
    }

    _inputValueColumn = table::TableUtil::getColumnData<InputType2>(inputTable, _inputFields[1]);
    if (_inputValueColumn == nullptr) {
        return false;
    }
    return true;
};

template <typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::initAccumulatorOutput(
    const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 2);
    _labelColumn = table::TableUtil::declareAndGetColumnData<InputType1>(
        outputTable, _outputFields[0], false);
    if (_labelColumn == nullptr) {
        return false;
    }

    _maxColumn = table::TableUtil::declareAndGetColumnData<InputType2>(
        outputTable, _outputFields[1], false);
    if (_maxColumn == nullptr) {
        return false;
    }

    outputTable->endGroup();
    return true;
}

template <typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::collect(table::Row inputRow, Accumulator *acc) {
    AccumulatorType *MaxLabelAcc = static_cast<AccumulatorType *>(acc);
    auto temp = MaxLabelAcc->max;
    MaxLabelAcc->max = std::max(MaxLabelAcc->max, _inputValueColumn->get(inputRow));
    if (temp != MaxLabelAcc->max) {
        MaxLabelAcc->label = _inputlabelColumn->get(inputRow);
    }
    return true;
}

template <typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::outputAccumulator(Accumulator *acc,
                                                                table::Row outputRow) const {
    AccumulatorType *MaxLabelAcc = static_cast<AccumulatorType *>(acc);
    _labelColumn->set(outputRow, MaxLabelAcc->label);
    _maxColumn->set(outputRow, MaxLabelAcc->max);
    return true;
}

// global
template <typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::initMergeInput(const table::TablePtr &inputTable) {
    return initCollectInput(inputTable);
}

template <typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::initResultOutput(const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _labelColumn = table::TableUtil::declareAndGetColumnData<InputType1>(
        outputTable, _outputFields[0], false);
    if (_labelColumn == nullptr) {
        return false;
    }
    return true;
}

template <typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::merge(table::Row inputRow, Accumulator *acc) {
    return collect(inputRow, acc);
}

template <typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::outputResult(Accumulator *acc,
                                                           table::Row outputRow) const {
    AccumulatorType *MaxLabelAcc = static_cast<AccumulatorType *>(acc);
    _labelColumn->set(outputRow, MaxLabelAcc->label);
    return true;
}
class MaxLabelAggFuncCreator : public AggFuncCreatorR {
public:
    std::string getResourceName() const override {
        return "SQL_AGG_FUNC_MAXLABEL";
    }
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
    std::string getAggFuncName() const override {
        return "MAXLABEL";
    }

private:
    AggFunc *createLocalFunction(const std::vector<table::ValueType> &inputTypes,
                                 const std::vector<std::string> &inputFields,
                                 const std::vector<std::string> &outputFields) override {
        if (outputFields.size() != 2) {
            SQL_LOG(
                ERROR, "impossible local output length, expect 2, actual %lu", outputFields.size());
            return nullptr;
        }
        return createFirstStageFunction(
            inputTypes, inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_LOCAL);
    }
    AggFunc *createNormalFunction(const std::vector<table::ValueType> &inputTypes,
                                  const std::vector<std::string> &inputFields,
                                  const std::vector<std::string> &outputFields) override {
        if (outputFields.size() != 1) {
            SQL_LOG(ERROR,
                    "impossible normal output length, expect 1, actual %lu",
                    outputFields.size());
            return nullptr;
        }
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
    AggFunc *listLabelType(const std::vector<table::ValueType> &inputTypes,
                           const std::vector<std::string> &inputFields,
                           const std::vector<std::string> &outputFields,
                           AggFuncMode mode);
    template <typename T>
    AggFunc *listValueType(const std::vector<table::ValueType> &inputTypes,
                           const std::vector<std::string> &inputFields,
                           const std::vector<std::string> &outputFields,
                           AggFuncMode mode);
};

} // namespace sql
