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
#ifndef ISEARCH_EXPRESSION_STRCATFUNCEXPRESSION_H_
#define ISEARCH_EXPRESSION_STRCATFUNCEXPRESSION_H_

#include <vector>
#include <string>

#include "expression/function/FunctionInterface.h"
#include "expression/function/FunctionCreator.h"
#include "matchdoc/MatchDoc.h"
#include "autil/mem_pool/Pool.h"

namespace expression {

class StrcatFuncInterface : public FunctionInterfaceTyped<autil::MultiChar>
{
public:
    typedef AttributeExpressionTyped<ExprValueType2AttrValueType<EVT_STRING, false>::AttrValueType> StringAttr;

public:
    StrcatFuncInterface(std::vector<std::pair<StringAttr*, std::string>> *exp_pairs);
    ~StrcatFuncInterface();

public:
    autil::MultiChar evaluate(const matchdoc::MatchDoc &matchDoc) override;
    void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount) override;
private:
    std::vector<std::pair<StringAttr*, std::string>> *_exp_pairs;
    autil::mem_pool::Pool _pool;

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////

class StrcatFuncInterfaceCreator : public FunctionCreator {
    TEMPLATE_FUNCTION_DESCRIBE_DEFINE(autil::MultiChar, "strcat", FUNCTION_UNLIMITED_ARGUMENT_COUNT,
            FT_NORMAL, FUNC_ACTION_SCOPE_ADAPTER, FUNC_BATCH_CUSTOM_MODE);
    FUNCTION_OVERRIDE_FUNC();
};

class TypedStrcatFuncCreator {
public:
    static FunctionInterface *createTypedFunction(const AttributeExpressionVec& funcSubExprVec);

private:
    static bool CastAttributeExpression(AttributeExpression* expr,
                                        StrcatFuncInterface::StringAttr* &attr,
                                        std::string &arg);

private:
    AUTIL_LOG_DECLARE();
};

}

#endif // ISEARCH_EXPRESSION_STRCATFUNCEXPRESSION_H_
