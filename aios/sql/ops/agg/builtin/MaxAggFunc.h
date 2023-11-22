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
#include "sql/common/Log.h"
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
class MaxAccumulator : public Accumulator {
public:
    MaxAccumulator()
        : isFirstAggregate(true) {}
    ~MaxAccumulator() = default;

public:
    T value;
    bool isFirstAggregate;
};

template <typename InputType>
class MaxAggFunc : public AggFunc {
public:
    MaxAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               AggFuncMode mode)
        : AggFunc(inputs, outputs, mode)
        , _inputColumn(NULL)
        , _maxColumn(NULL) {}
    ~MaxAggFunc() {}

private:
    MaxAggFunc(const MaxAggFunc &);
    MaxAggFunc &operator=(const MaxAggFunc &);

public:
    DEF_CREATE_ACCUMULATOR_FUNC(MaxAccumulator<InputType>);

private:
    // local
    bool initCollectInput(const table::TablePtr &inputTable) override;
    bool initAccumulatorOutput(const table::TablePtr &outputTable) override;
    bool collect(table::Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, table::Row outputRow) const override;

private:
    table::ColumnData<InputType> *_inputColumn;
    table::ColumnData<InputType> *_maxColumn;
};

// local
template <typename InputType>
bool MaxAggFunc<InputType>::initCollectInput(const table::TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = table::TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template <typename InputType>
bool MaxAggFunc<InputType>::initAccumulatorOutput(const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _maxColumn
        = table::TableUtil::declareAndGetColumnData<InputType>(outputTable, _outputFields[0]);
    if (_maxColumn == nullptr) {
        SQL_LOG(ERROR, "declare column failed [%s]", _outputFields[0].c_str());
        return false;
    }
    return true;
}

template <typename InputType>
bool MaxAggFunc<InputType>::collect(table::Row inputRow, Accumulator *acc) {
    MaxAccumulator<InputType> *maxAcc = static_cast<MaxAccumulator<InputType> *>(acc);
    if (maxAcc->isFirstAggregate) {
        maxAcc->value = _inputColumn->get(inputRow);
        maxAcc->isFirstAggregate = false;
    } else {
        maxAcc->value = std::max(maxAcc->value, _inputColumn->get(inputRow));
    }
    return true;
}

template <typename InputType>
bool MaxAggFunc<InputType>::outputAccumulator(Accumulator *acc, table::Row outputRow) const {
    MaxAccumulator<InputType> *maxAcc = static_cast<MaxAccumulator<InputType> *>(acc);
    _maxColumn->set(outputRow, maxAcc->value);
    return true;
}

class MaxAggFuncCreator : public AggFuncCreatorR {
public:
    std::string getResourceName() const override {
        return "SQL_AGG_FUNC_MAX";
    }
    std::string getAggFuncName() const override {
        return "MAX";
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
