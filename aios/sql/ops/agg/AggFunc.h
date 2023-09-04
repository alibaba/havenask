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

#include "sql/ops/agg/AggFuncMode.h"
#include "table/Row.h"
#include "table/Table.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace sql {
class Accumulator;
} // namespace sql

namespace sql {

class AggFunc {
public:
    AggFunc(const std::vector<std::string> &inputs,
            const std::vector<std::string> &outputs,
            AggFuncMode funcMode)
        : _inputFields(inputs)
        , _outputFields(outputs)
        , _funcMode(funcMode) {}
    virtual ~AggFunc() {}
    AggFunc(const AggFunc &) = delete;
    AggFunc &operator=(const AggFunc &) = delete;

public:
    bool init(autil::mem_pool::Pool *pool,
              autil::mem_pool::Pool *funcPool,
              const std::string &funcHint = "") {
        assert(!_inited);
        _pool = pool;
        _funcPool = funcPool;
        if (!initHint(funcHint)) {
            return false;
        }
        _inited = true;
        return true;
    }
    std::string getName() const {
        return _name;
    }
    void setName(const std::string &name) {
        _name = name;
    }
    virtual bool initHint(const std::string &funcHint) {
        return true;
    }

public:
    virtual Accumulator *createAccumulator(autil::mem_pool::Pool *pool) = 0;
    virtual bool accumulatorTriviallyDestruct() const = 0;
    virtual void destroyAccumulator(Accumulator *acc[], size_t n) = 0;

public:
    virtual bool needDependInputTablePools() const {
        return true;
    }

private:
    // local
    virtual bool initCollectInput(const table::TablePtr &inputTable) = 0;
    virtual bool initAccumulatorOutput(const table::TablePtr &outputTable) = 0;
    virtual bool collect(table::Row inputRow, Accumulator *acc) = 0;
    virtual bool outputAccumulator(Accumulator *acc, table::Row outputRow) const = 0;

    // global
    virtual bool initMergeInput(const table::TablePtr &inputTable);
    virtual bool initResultOutput(const table::TablePtr &outputTable);
    virtual bool merge(table::Row inputRow, Accumulator *acc);
    virtual bool outputResult(Accumulator *acc, table::Row outputRow) const;

public:
    bool initInput(const table::TablePtr &inputTable) {
        assert(_inited);
        if (_funcMode == AggFuncMode::AGG_FUNC_MODE_GLOBAL) {
            return initMergeInput(inputTable);
        } else {
            return initCollectInput(inputTable);
        }
    }
    bool initOutput(const table::TablePtr &inputTable) {
        assert(_inited);
        if (_funcMode == AggFuncMode::AGG_FUNC_MODE_LOCAL) {
            return initAccumulatorOutput(inputTable);
        } else {
            return initResultOutput(inputTable);
        }
    }
    bool aggregate(table::Row inputRow, Accumulator *acc) {
        assert(_inited);
        if (_funcMode == AggFuncMode::AGG_FUNC_MODE_GLOBAL) {
            return merge(inputRow, acc);
        } else {
            return collect(inputRow, acc);
        }
    }
    bool setResult(Accumulator *acc, table::Row outputRow) {
        assert(_inited);
        if (_funcMode == AggFuncMode::AGG_FUNC_MODE_LOCAL) {
            return outputAccumulator(acc, outputRow);
        } else {
            return outputResult(acc, outputRow);
        }
    }

protected:
    std::vector<std::string> _inputFields;
    std::vector<std::string> _outputFields;
    AggFuncMode _funcMode;
    std::string _name;
    autil::mem_pool::Pool *_pool = nullptr;
    autil::mem_pool::Pool *_funcPool = nullptr;
    bool _inited = false;
};

#define DEF_CREATE_ACCUMULATOR_FUNC(accType, ...)                                                  \
    sql::Accumulator *createAccumulator(autil::mem_pool::Pool *pool) override {                    \
        auto *addr = pool->allocateUnsafe(sizeof(accType));                                        \
        return new (addr) accType(__VA_ARGS__);                                                    \
    }                                                                                              \
    bool accumulatorTriviallyDestruct() const override {                                           \
        return std::is_trivially_destructible<accType>::value;                                     \
    }                                                                                              \
    void destroyAccumulator(sql::Accumulator *acc[], size_t n) override {                          \
        if (accumulatorTriviallyDestruct()) {                                                      \
            return;                                                                                \
        }                                                                                          \
        for (size_t i = 0; i < n; ++i) {                                                           \
            auto *typedAcc = static_cast<accType *>(acc[i]);                                       \
            POOL_DELETE_CLASS(typedAcc);                                                           \
        }                                                                                          \
    }

typedef std::shared_ptr<AggFunc> AggFuncPtr;
} // namespace sql
