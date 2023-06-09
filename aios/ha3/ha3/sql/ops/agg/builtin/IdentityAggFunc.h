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
#include "navi/common.h"
#include "table/DataCommon.h"
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

namespace isearch {
namespace sql {

template <typename T>
class IdentityAccumulator : public Accumulator {
public:
    IdentityAccumulator()
        : isFirstAggregate(true) {}
    ~IdentityAccumulator() = default;

public:
    T value;
    bool isFirstAggregate;
};

template <typename InputType>
class IdentityAggFunc : public AggFunc {
public:
    IdentityAggFunc(const std::vector<std::string> &inputs,
                    const std::vector<std::string> &outputs,
                    AggFuncMode mode)
        : AggFunc(inputs, outputs, mode)
        , _inputColumn(NULL)
        , _identityColumn(NULL) {}
    ~IdentityAggFunc() {}

private:
    IdentityAggFunc(const IdentityAggFunc &);
    IdentityAggFunc &operator=(const IdentityAggFunc &);

public:
    DEF_CREATE_ACCUMULATOR_FUNC(IdentityAccumulator<InputType>)
    bool needDependInputTablePools() const override;

private:
    // local
    bool initCollectInput(const table::TablePtr &inputTable) override;
    bool initAccumulatorOutput(const table::TablePtr &outputTable) override;
    bool collect(table::Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, table::Row outputRow) const override;

private:
    table::ColumnData<InputType> *_inputColumn;
    table::ColumnData<InputType> *_identityColumn;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(sql, IdentityAggFunc, InputType);

template <typename InputType>
bool IdentityAggFunc<InputType>::needDependInputTablePools() const {
    return autil::IsMultiType<InputType>::type::value;
}

// local
template <typename InputType>
bool IdentityAggFunc<InputType>::initCollectInput(const table::TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = table::TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template <typename InputType>
bool IdentityAggFunc<InputType>::initAccumulatorOutput(const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _identityColumn = table::TableUtil::declareAndGetColumnData<InputType>(
        outputTable, _outputFields[0], false);
    if (_identityColumn == nullptr) {
        return false;
    }
    return true;
}

template <typename InputType>
bool IdentityAggFunc<InputType>::collect(table::Row inputRow, Accumulator *acc) {
    IdentityAccumulator<InputType> *identityAcc
        = static_cast<IdentityAccumulator<InputType> *>(acc);
    if (identityAcc->isFirstAggregate) {
        identityAcc->value = _inputColumn->get(inputRow);
        identityAcc->isFirstAggregate = false;
    }
    return true;
}

template <typename InputType>
bool IdentityAggFunc<InputType>::outputAccumulator(Accumulator *acc, table::Row outputRow) const {
    IdentityAccumulator<InputType> *identityAcc
        = static_cast<IdentityAccumulator<InputType> *>(acc);
    _identityColumn->set(outputRow, identityAcc->value);
    return true;
}

class IdentityAggFuncCreator : public AggFuncCreatorR {
public:
    void def(navi::ResourceDefBuilder &builder) const override {
        builder.name("SQL_AGG_FUNC_IDENTITY");
    }
    std::string getAggFuncName() const override {
        return "IDENTITY";
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

class ArbitraryAggFuncCreator : public IdentityAggFuncCreator {
public:
    void def(navi::ResourceDefBuilder &builder) const override {
        builder.name("SQL_AGG_FUNC_ARBITRARY");
    }
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
    std::string getAggFuncName() const override {
        return "ARBITRARY";
    }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
} // namespace isearch
