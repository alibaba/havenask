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
#include "sql/ops/agg/builtin/CountAggFunc.h"

#include <assert.h>
#include <iosfwd>
#include <memory>

#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "navi/engine/Resource.h"
#include "sql/common/Log.h"
#include "table/ColumnData.h"
#include "table/TableUtil.h"

using namespace std;
using namespace table;
using namespace matchdoc;

namespace sql {

// local
bool CountAggFunc::initCollectInput(const TablePtr &inputTable) {
    return true;
};

bool CountAggFunc::initAccumulatorOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _countColumn
        = TableUtil::declareAndGetColumnData<int64_t>(outputTable, _outputFields[0], false);
    if (_countColumn == nullptr) {
        return false;
    }
    return true;
}

bool CountAggFunc::collect(Row inputRow, Accumulator *acc) {
    CountAccumulator *countAcc = static_cast<CountAccumulator *>(acc);
    ++countAcc->value;
    return true;
}

bool CountAggFunc::outputAccumulator(Accumulator *acc, Row outputRow) const {
    CountAccumulator *countAcc = static_cast<CountAccumulator *>(acc);
    _countColumn->set(outputRow, countAcc->value);
    return true;
}

bool CountAggFunc::initMergeInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = TableUtil::getColumnData<int64_t>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
}

bool CountAggFunc::initResultOutput(const TablePtr &outputTable) {
    return initAccumulatorOutput(outputTable);
}

bool CountAggFunc::merge(Row inputRow, Accumulator *acc) {
    CountAccumulator *countAcc = static_cast<CountAccumulator *>(acc);
    countAcc->value += _inputColumn->get(inputRow);
    return true;
}

bool CountAggFunc::outputResult(Accumulator *acc, Row outputRow) const {
    return outputAccumulator(acc, outputRow);
}

AggFunc *CountAggFuncCreator::createFirstStageFunction(const vector<ValueType> &inputTypes,
                                                       const vector<string> &inputFields,
                                                       const vector<string> &outputFields,
                                                       AggFuncMode mode) {
    if (inputFields.size() > 1) {
        SQL_LOG(ERROR, "impossible input length, expect 0 or 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible output length, expect 1, actual %lu", outputFields.size());
        return nullptr;
    }
    return new CountAggFunc(inputFields, outputFields, mode);
}

AggFunc *CountAggFuncCreator::createGlobalFunction(const vector<ValueType> &inputTypes,
                                                   const vector<string> &inputFields,
                                                   const vector<string> &outputFields) {
    if (inputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible input length, expect 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible output length, expect 1, actual %lu", outputFields.size());
        return nullptr;
    }
    if (inputTypes[0].getTypeIgnoreConstruct()
        != ValueTypeHelper<int64_t>::getValueType().getTypeIgnoreConstruct()) {
        SQL_LOG(ERROR,
                "impossible input type, expect int64, actual %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    return new CountAggFunc(inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_GLOBAL);
}

REGISTER_RESOURCE(CountAggFuncCreator);

} // namespace sql
