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

#include "autil/Hyperloglog.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "sql/common/Log.h"
#include "sql/ops/agg/Accumulator.h"
#include "sql/ops/agg/AggFunc.h"
#include "sql/ops/agg/AggFuncCreatorR.h"
#include "sql/ops/agg/AggFuncMode.h"
#include "table/ColumnData.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace sql {

class ApproxCountDistinctAccumulator : public Accumulator {
public:
    ApproxCountDistinctAccumulator(autil::mem_pool::Pool *pool)
        : _hllCtx(autil::Hyperloglog::hllCtxCreate(autil::Hyperloglog::HLL_DENSE, pool)) {}
    ~ApproxCountDistinctAccumulator() {
        if (_hllCtx != nullptr) {
            POOL_DELETE_CLASS(_hllCtx);
        }
    }

public:
    autil::HllCtx *_hllCtx = nullptr;
};

template <typename InputType>
class ApproxCountDistinctFunc : public AggFunc {
public:
    ApproxCountDistinctFunc(const std::vector<std::string> &inputs,
                            const std::vector<std::string> &outputs,
                            AggFuncMode mode)
        : AggFunc(inputs, outputs, mode) {}
    ~ApproxCountDistinctFunc() = default;

private:
    ApproxCountDistinctFunc(const ApproxCountDistinctFunc &);
    ApproxCountDistinctFunc &operator=(const ApproxCountDistinctFunc &);

public:
    DEF_CREATE_ACCUMULATOR_FUNC(ApproxCountDistinctAccumulator, _funcPool)

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
    table::ColumnData<autil::HllCtx> *_hllCtxColumn;
    table::ColumnData<uint64_t> *_outputColumn;
};

// local
template <typename InputType>
bool ApproxCountDistinctFunc<InputType>::initCollectInput(const table::TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = table::TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    return _inputColumn != nullptr;
};

template <typename InputType>
bool ApproxCountDistinctFunc<InputType>::initAccumulatorOutput(const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _hllCtxColumn
        = table::TableUtil::declareAndGetColumnData<autil::HllCtx>(outputTable, _outputFields[0]);
    return _hllCtxColumn != nullptr;
}

template <typename InputType>
bool ApproxCountDistinctFunc<InputType>::collect(table::Row inputRow, Accumulator *acc) {
    ApproxCountDistinctAccumulator *approxCountDistinctAcc
        = static_cast<ApproxCountDistinctAccumulator *>(acc);
    auto value = _inputColumn->get(inputRow);
    return autil::Hyperloglog::hllCtxAdd(
               approxCountDistinctAcc->_hllCtx, (unsigned char *)&value, sizeof(InputType), nullptr)
           != -1;
}

template <>
bool ApproxCountDistinctFunc<autil::MultiChar>::collect(table::Row inputRow, Accumulator *acc) {
    ApproxCountDistinctAccumulator *approxCountDistinctAcc
        = static_cast<ApproxCountDistinctAccumulator *>(acc);
    auto value = _inputColumn->get(inputRow);
    return autil::Hyperloglog::hllCtxAdd(approxCountDistinctAcc->_hllCtx,
                                         reinterpret_cast<const unsigned char *>(value.data()),
                                         value.size(),
                                         nullptr)
           != -1;
}

template <typename InputType>
bool ApproxCountDistinctFunc<InputType>::outputAccumulator(Accumulator *acc,
                                                           table::Row outputRow) const {
    ApproxCountDistinctAccumulator *approxCountDistinctAcc
        = static_cast<ApproxCountDistinctAccumulator *>(acc);
    _hllCtxColumn->set(outputRow, *(approxCountDistinctAcc->_hllCtx));
    return true;
}

// global
template <typename InputType>
bool ApproxCountDistinctFunc<InputType>::initMergeInput(const table::TablePtr &input) {
    assert(_inputFields.size() == 1);
    _hllCtxColumn = table::TableUtil::getColumnData<autil::HllCtx>(input, _inputFields[0]);
    return _hllCtxColumn != nullptr;
}

template <typename InputType>
bool ApproxCountDistinctFunc<InputType>::initResultOutput(const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _outputColumn
        = table::TableUtil::declareAndGetColumnData<uint64_t>(outputTable, _outputFields[0]);
    return _outputColumn != nullptr;
}

template <typename InputType>
bool ApproxCountDistinctFunc<InputType>::merge(table::Row inputRow, Accumulator *acc) {
    ApproxCountDistinctAccumulator *approxCountDistinctAcc
        = static_cast<ApproxCountDistinctAccumulator *>(acc);
    auto otherCtx = _hllCtxColumn->get(inputRow);
    return autil::Hyperloglog::hllCtxMerge(approxCountDistinctAcc->_hllCtx, &otherCtx, nullptr)
           != -1;
}

template <typename InputType>
bool ApproxCountDistinctFunc<InputType>::outputResult(Accumulator *acc,
                                                      table::Row outputRow) const {
    ApproxCountDistinctAccumulator *approxCountDistinctAcc
        = static_cast<ApproxCountDistinctAccumulator *>(acc);
    uint64_t result = autil::Hyperloglog::hllCtxCount(approxCountDistinctAcc->_hllCtx);
    _outputColumn->set(outputRow, result);
    return true;
}

class ApproxCountDistinctFuncCreator : public AggFuncCreatorR {
public:
    std::string getResourceName() const override {
        return "SQL_AGG_FUNC_HA_APPROX_COUNT_DISTINCT";
    }
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
    std::string getAggFuncName() const override {
        return "HA_APPROX_COUNT_DISTINCT";
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
