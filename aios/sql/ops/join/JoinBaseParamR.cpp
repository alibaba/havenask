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
#include "sql/ops/join/JoinBaseParamR.h"

#include "autil/StringUtil.h"
#include "sql/common/common.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/join/AntiJoin.h"
#include "sql/ops/join/HashJoinConditionVisitor.h"
#include "sql/ops/join/InnerJoin.h"
#include "sql/ops/join/JoinBase.h"
#include "sql/ops/join/LeftJoin.h"
#include "sql/ops/join/SemiJoin.h"
#include "sql/ops/util/KernelUtil.h"

using namespace std;
using namespace autil;

namespace sql {

const std::string JoinBaseParamR::RESOURCE_ID = "join_base_param_r";

JoinBaseParamR::JoinBaseParamR() {}

JoinBaseParamR::~JoinBaseParamR() {}

void JoinBaseParamR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
    builder.enableBuildR({CalcTableR::RESOURCE_ID});
}

bool JoinBaseParamR::config(navi::ResourceConfigContext &ctx) {
    ctx.JsonizeAsString("condition", _conditionJson, "");
    ctx.JsonizeAsString("equi_condition", _equiConditionJson, "");
    ctx.JsonizeAsString("remaining_condition", _remainConditionJson, "");
    NAVI_JSONIZE(ctx, "join_type", _joinType);
    NAVI_JSONIZE(ctx, "semi_join_type", _semiJoinType, _semiJoinType);
    // for compatible
    if (_joinType == SQL_SEMI_JOIN_TYPE || _semiJoinType == SQL_SEMI_JOIN_TYPE) {
        _joinType = _semiJoinType = SQL_SEMI_JOIN_TYPE;
    }
    NAVI_JSONIZE(ctx, "system_field_num", _systemFieldNum, _systemFieldNum);
    NAVI_JSONIZE(ctx, "left_input_fields", _leftInputFields, _leftInputFields);
    NAVI_JSONIZE(ctx, "right_input_fields", _rightInputFields, _rightInputFields);
    NAVI_JSONIZE(ctx, "output_fields_internal", _outputFields, _outputFields);
    NAVI_JSONIZE(ctx, "output_fields", _outputFieldsForKernel, _outputFieldsForKernel);
    KernelUtil::stripName(_leftInputFields);
    KernelUtil::stripName(_rightInputFields);
    KernelUtil::stripName(_outputFields);
    KernelUtil::stripName(_outputFieldsForKernel);
    if (!convertFields()) {
        SQL_LOG(ERROR, "convert fields failed.");
        return false;
    }
    std::map<std::string, std::map<std::string, std::string>> hintsMap;
    NAVI_JSONIZE(ctx, "hints", hintsMap, hintsMap);
    if (hintsMap.find(SQL_JOIN_HINT) != hintsMap.end()) {
        _joinHintMap.swap(hintsMap[SQL_JOIN_HINT]);
    }
    patchHintInfo();
    return true;
}

void JoinBaseParamR::patchHintInfo() {
    auto iter = _joinHintMap.find("defaultValue");
    if (iter != _joinHintMap.end()) {
        vector<string> values;
        StringUtil::fromString(iter->second, values, ",");
        for (auto &value : values) {
            auto pair = StringUtil::split(value, ":");
            StringUtil::toUpperCase(pair[0]);
            _defaultValue[pair[0]] = pair[1];
            SQL_LOG(TRACE3, "join use default value [%s] : [%s]", pair[0].c_str(), pair[1].c_str());
        }
    }
}

bool JoinBaseParamR::convertFields() {
    if (_systemFieldNum != 0) {
        return false;
    }
    if (_outputFields.empty()) {
        _outputFields = _outputFieldsForKernel;
    }
    size_t joinFiledSize = _leftInputFields.size() + _rightInputFields.size();
    if (_semiJoinType == SQL_SEMI_JOIN_TYPE || _joinType == SQL_ANTI_JOIN_TYPE) {
        if (_outputFieldsForKernel.size() != _leftInputFields.size()) {
            SQL_LOG(ERROR,
                    "semi join or anti join output fields [%ld] not equal left input fields [%ld]",
                    _outputFieldsForKernel.size(),
                    _leftInputFields.size());
            return false;
        }
    } else {
        if (_outputFieldsForKernel.size() != joinFiledSize) {
            return false;
        }
    }
    if (_outputFields.size() != joinFiledSize) {
        return false;
    }
    for (size_t i = 0; i < _outputFields.size(); i++) {
        if (i < _leftInputFields.size()) {
            _output2InputMap[_outputFields[i]] = make_pair(_leftInputFields[i], true);
        } else {
            size_t offset = i - _leftInputFields.size();
            _output2InputMap[_outputFields[i]] = make_pair(_rightInputFields[offset], false);
        }
    }
    return true;
}

navi::ErrorCode JoinBaseParamR::init(navi::ResourceInitContext &ctx) {
    _pool = _queryMemPoolR->getPool().get();
    _poolPtr = _graphMemoryPoolR->getPool();

    if (!initCalcTableAndJoinColumns(ctx)) {
        return navi::EC_ABORT;
    }
    if (_joinType == SQL_INNER_JOIN_TYPE) {
        _joinPtr.reset(new InnerJoin(*this));
    } else if (_joinType == SQL_LEFT_JOIN_TYPE) {
        _joinPtr.reset(new LeftJoin(*this));
    } else if (_joinType == SQL_SEMI_JOIN_TYPE) {
        _joinPtr.reset(new SemiJoin(*this));
    } else if (_joinType == SQL_ANTI_JOIN_TYPE) {
        _joinPtr.reset(new AntiJoin(*this));
    } else if (_joinType == SQL_LEFT_MULTI_JOIN_TYPE) {
        //  pass, left multi join dont need joinptr
    } else {
        SQL_LOG(ERROR, "join type [%s] is not supported", _joinType.c_str());
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

bool JoinBaseParamR::initCalcTableAndJoinColumns(navi::ResourceInitContext &ctx) {
    ConditionParser parser(_poolPtr.get());
    ConditionPtr condition;
    if (!_remainConditionJson.empty()) {
        if (!parser.parseCondition(_equiConditionJson, condition)) {
            SQL_LOG(ERROR, "parse condition [%s] failed", _equiConditionJson.c_str());
            return false;
        }
        auto calcTableR = CalcTableR::buildOne(ctx, {}, {}, _remainConditionJson, "");
        if (!calcTableR) {
            SQL_LOG(WARN, "build CalcTableR failed");
            return false;
        }
        _calcTableR = calcTableR;
    } else {
        if (!parser.parseCondition(_conditionJson, condition)) {
            SQL_LOG(ERROR, "parse condition [%s] failed", _conditionJson.c_str());
            return false;
        }
    }

    HashJoinConditionVisitor visitor(_output2InputMap);
    if (condition) {
        condition->accept(&visitor);
    }
    if (visitor.isError()) {
        SQL_LOG(ERROR, "visit condition failed, error info [%s]", visitor.errorInfo().c_str());
        return false;
    }

    _leftJoinColumns = visitor.getLeftJoinColumns();
    _rightJoinColumns = visitor.getRightJoinColumns();
    if (_leftJoinColumns.size() != _rightJoinColumns.size()) {
        SQL_LOG(ERROR,
                "left join column size [%zu] not match right one [%zu]",
                _leftJoinColumns.size(),
                _rightJoinColumns.size());
        return false;
    }

    return true;
}

void JoinBaseParamR::reserveJoinRow(size_t rowCount) {
    _tableAIndexes.reserve(rowCount);
    _tableBIndexes.reserve(rowCount);
}

REGISTER_RESOURCE(JoinBaseParamR);

} // namespace sql
