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

#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "sql/ops/agg/Accumulator.h"
#include "sql/ops/agg/AggFunc.h"
#include "sql/ops/agg/AggFuncCreatorR.h"
#include "sql/ops/agg/AggFuncMode.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace table {
template <typename T>
class ColumnData;
} // namespace table

namespace sql {

template <typename T>
class MinAccumulator : public Accumulator {
public:
    MinAccumulator()
        : isFirstAggregate(true) {}
    ~MinAccumulator() = default;

public:
    T value;
    bool isFirstAggregate;
};

template <typename InputType>
class MinAggFunc : public AggFunc {
public:
    MinAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               AggFuncMode mode)
        : AggFunc(inputs, outputs, mode)
        , _inputColumn(NULL)
        , _minColumn(NULL) {}
    ~MinAggFunc() {}

private:
    MinAggFunc(const MinAggFunc &);
    MinAggFunc &operator=(const MinAggFunc &);

public:
    DEF_CREATE_ACCUMULATOR_FUNC(MinAccumulator<InputType>)
    bool needDependInputTablePools() const override;

private:
    // local
    bool initCollectInput(const table::TablePtr &inputTable) override;
    bool initAccumulatorOutput(const table::TablePtr &outputTable) override;
    bool collect(table::Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, table::Row outputRow) const override;

private:
    table::ColumnData<InputType> *_inputColumn;
    table::ColumnData<InputType> *_minColumn;
};

template <typename InputType>
bool MinAggFunc<InputType>::needDependInputTablePools() const {
    return autil::IsMultiType<InputType>::type::value;
}

// local
template <typename InputType>
bool MinAggFunc<InputType>::initCollectInput(const table::TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = table::TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template <typename InputType>
bool MinAggFunc<InputType>::initAccumulatorOutput(const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _minColumn = table::TableUtil::declareAndGetColumnData<InputType>(
        outputTable, _outputFields[0], false);
    if (_minColumn == nullptr) {
        return false;
    }
    return true;
}

template <typename InputType>
bool MinAggFunc<InputType>::collect(table::Row inputRow, Accumulator *acc) {
    MinAccumulator<InputType> *minAcc = static_cast<MinAccumulator<InputType> *>(acc);
    if (minAcc->isFirstAggregate) {
        minAcc->value = _inputColumn->get(inputRow);
        minAcc->isFirstAggregate = false;
    } else {
        minAcc->value = std::min(minAcc->value, _inputColumn->get(inputRow));
    }
    return true;
}

template <typename InputType>
bool MinAggFunc<InputType>::outputAccumulator(Accumulator *acc, table::Row outputRow) const {
    MinAccumulator<InputType> *minAcc = static_cast<MinAccumulator<InputType> *>(acc);
    _minColumn->set(outputRow, minAcc->value);
    return true;
}

class MinAggFuncCreator : public AggFuncCreatorR {
public:
    std::string getResourceName() const override {
        return "SQL_AGG_FUNC_MIN";
    }
    std::string getAggFuncName() const override {
        return "MIN";
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
};

} // namespace sql
