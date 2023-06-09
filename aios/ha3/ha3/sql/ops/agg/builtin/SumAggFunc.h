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
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/ops/agg/Accumulator.h"
#include "ha3/sql/ops/agg/AggFunc.h"
#include "ha3/sql/ops/agg/AggFuncCreatorR.h"
#include "ha3/sql/ops/agg/AggFuncMode.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "table/DataCommon.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace table {
template <typename T>
class ColumnData;
} // namespace table

namespace isearch {
namespace sql {

template <typename T>
class SumAccumulator : public Accumulator {
public:
    SumAccumulator()
        : value(0) {}
    ~SumAccumulator() = default;

public:
    T value;
};

template <typename InputType, typename AccumulatorType>
class SumAggFunc : public AggFunc {
public:
    SumAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               AggFuncMode mode)
        : AggFunc(inputs, outputs, mode)
        , _inputColumn(NULL)
        , _sumColumn(NULL) {}
    ~SumAggFunc() {}

private:
    SumAggFunc(const SumAggFunc &);
    SumAggFunc &operator=(const SumAggFunc &);

public:
    DEF_CREATE_ACCUMULATOR_FUNC(SumAccumulator<AccumulatorType>)
    bool needDependInputTablePools() const override {
        return false;
    }

private:
    // local
    bool initCollectInput(const table::TablePtr &inputTable) override;
    bool initAccumulatorOutput(const table::TablePtr &outputTable) override;
    bool collect(table::Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, table::Row outputRow) const override;

private:
    table::ColumnData<InputType> *_inputColumn;
    table::ColumnData<AccumulatorType> *_sumColumn;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE_2(sql, SumAggFunc, InputType, AccumulatorType);

// local
template <typename InputType, typename AccumulatorType>
bool SumAggFunc<InputType, AccumulatorType>::initCollectInput(const table::TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = table::TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template <typename InputType, typename AccumulatorType>
bool SumAggFunc<InputType, AccumulatorType>::initAccumulatorOutput(
    const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _sumColumn = table::TableUtil::declareAndGetColumnData<AccumulatorType>(
        outputTable, _outputFields[0], false);
    if (_sumColumn == nullptr) {
        return false;
    }
    return true;
}

template <typename InputType, typename AccumulatorType>
bool SumAggFunc<InputType, AccumulatorType>::collect(table::Row inputRow, Accumulator *acc) {
    SumAccumulator<AccumulatorType> *sumAcc = static_cast<SumAccumulator<AccumulatorType> *>(acc);
    sumAcc->value += _inputColumn->get(inputRow);
    return true;
}

template <typename InputType, typename AccumulatorType>
bool SumAggFunc<InputType, AccumulatorType>::outputAccumulator(Accumulator *acc,
                                                               table::Row outputRow) const {
    SumAccumulator<AccumulatorType> *sumAcc = static_cast<SumAccumulator<AccumulatorType> *>(acc);
    _sumColumn->set(outputRow, sumAcc->value);
    return true;
}

class SumAggFuncCreator : public AggFuncCreatorR {
public:
    void def(navi::ResourceDefBuilder &builder) const override {
        builder.name("SQL_AGG_FUNC_SUM");
    }
    std::string getAggFuncName() const override {
        return "SUM";
    }

private:
    AggFunc *createLocalFunction(const std::vector<table::ValueType> &inputTypes,
                                 const std::vector<std::string> &inputFields,
                                 const std::vector<std::string> &outputFields) override;
    AggFunc *createNormalFunction(const std::vector<table::ValueType> &inputTypes,
                                  const std::vector<std::string> &inputFields,
                                  const std::vector<std::string> &outputFields) override {
        return createLocalFunction(inputTypes, inputFields, outputFields);
    }
    AggFunc *createGlobalFunction(const std::vector<table::ValueType> &inputTypes,
                                  const std::vector<std::string> &inputFields,
                                  const std::vector<std::string> &outputFields) override {
        return createLocalFunction(inputTypes, inputFields, outputFields);
    }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
} // namespace isearch
