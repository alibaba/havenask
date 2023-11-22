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
#include "havenask_plugins/udf_plugins/vectorscore/VectorScoreFunction.h"

#include "autil/StringUtil.h"
#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "alog/Logger.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/framework/ArgumentAttributeExpression.h"
#include "suez/turing/expression/provider/TermMetaInfo.h"
#include "suez/turing/expression/provider/MetaInfo.h"
#include "suez/turing/expression/provider/MatchValues.h"

using namespace matchdoc;

using namespace suez::turing;
BEGIN_HAVENASK_UDF_NAMESPACE(vectorscore);
AUTIL_LOG_SETUP(vectorscore, VectorScoreFunction);

VectorScoreFunction::VectorScoreFunction(const FunctionSubExprVec & exprs) : _exprs(exprs) {

}

VectorScoreFunction::~VectorScoreFunction() {

}


bool VectorScoreFunction::beginRequest(FunctionProvider* functionProvider) {
    assert(functionProvider);
    auto* arg = GET_EXPR_IF_ARGUMENT(_exprs[0]);
    if (!arg || !arg->getConstValue<std::string>(_vectorIndexName)) {
        SQL_LOG(ERROR, "invalid arg expression for in function: %s.",
               _exprs[0]->getOriginalString().c_str());
        return false;
    }
    if(_exprs.size() == 2) {
        auto* arg = GET_EXPR_IF_ARGUMENT(_exprs[1]);
        if (!arg || !arg->getConstValue<double>(_defaultScore)) {
            SQL_LOG(ERROR, "invalid arg expression for in function: %s.",
                _exprs[1]->getOriginalString().c_str());
            return false;
        }
    }
    _matchInfoReader = functionProvider->getMatchInfoReader();
    if(_matchInfoReader == nullptr) {
        SQL_LOG(ERROR, "cant get matchInfo");
        return false;
    }
    // need to call 'GetFloat' from term match value, will discard const
    _matchValuesRef = (matchdoc::Reference<suez::turing::MatchValues>*)_matchInfoReader->getMatchValuesRef();
    if(_matchValuesRef == nullptr) {
        SQL_LOG(ERROR, "cant get matchValues");
        return false;
    }
    return true;
}

void VectorScoreFunction::endRequest() {

}


double VectorScoreFunction::evaluate(matchdoc::MatchDoc matchDoc) {
    std::shared_ptr<MetaInfo> metaInfo = _matchInfoReader->getMetaInfoPtr();
    auto& matchValues = _matchValuesRef->getReference(matchDoc);
    double score = 0.0; 
    bool isMatch = false;
    for(size_t i=0;i<metaInfo->size();i++) {
        auto& termMetaInfo = (*metaInfo)[i];
        if(termMetaInfo.getIndexName() != _vectorIndexName) {
            continue;
        }
        isMatch = true;
        auto& termMatchValue = matchValues.getTermMatchValue(i);
        score += termMatchValue.GetFloat();
    }
    return isMatch?score:_defaultScore;
}

AUTIL_LOG_SETUP(vectorscore, VectorScoreFunctionCreatorImpl);
FunctionInterface* VectorScoreFunctionCreatorImpl::createFunction(
        const FunctionSubExprVec& funcSubExprVec) {
    if(funcSubExprVec.size() != 1 && funcSubExprVec.size() != 2) {
        SQL_LOG(ERROR, "function should have 1 or 2 args");
        return nullptr;
    }
    if(true == funcSubExprVec[0]->isMultiValue() || funcSubExprVec[0]->getType() != vt_string)
    {
        SQL_LOG(ERROR, "first arg expected to be type: string(%d), in fact: %d", vt_string, funcSubExprVec[0]->getType());
        return nullptr;
    }
    if(funcSubExprVec.size() == 2) {
        // xxx.xx 默认情况下识别为double
        if(true == funcSubExprVec[1]->isMultiValue() || funcSubExprVec[1]->getType() != vt_double)
        {
            SQL_LOG(ERROR, "second arg expected to be type: float(%d), in fact: %d", vt_double, funcSubExprVec[1]->getType());
            return nullptr;
        }
    }
    return new VectorScoreFunction(funcSubExprVec);
}


END_HAVENASK_UDF_NAMESPACE(vectorscore);
