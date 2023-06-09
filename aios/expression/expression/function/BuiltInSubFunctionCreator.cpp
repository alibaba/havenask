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
#include "expression/function/BuiltInSubFunctionCreator.h"

using namespace std;

namespace expression {

FunctionInterface *SubCountFuncInterfaceCreatorImpl::createFunction(
        const AttributeExpressionVec& funcSubExprVec,
        AttributeExpressionCreator *creator)

{
    return SubFunctionCreatorWrapper::createSubCountFunction(
            funcSubExprVec, getFuncProtoInfo());
}

AUTIL_LOG_SETUP(expression, SubJoinFuncInterfaceCreatorImpl);
FunctionInterface *SubJoinFuncInterfaceCreatorImpl::createFunction(
        const AttributeExpressionVec& funcSubExprVec,
        AttributeExpressionCreator *creator)
{
    if (funcSubExprVec.size() > 2) {
        AUTIL_LOG(WARN, "function expect: less than 2 args, actual: [%d] args.",
                (int)funcSubExprVec.size());
        return NULL;
    }

    string split(" ");
    if (funcSubExprVec.size() == 2) {
        ArgumentAttributeExpression *argExpr =
            dynamic_cast<ArgumentAttributeExpression*>(funcSubExprVec[1]);
        if (!argExpr) {
            AUTIL_LOG(WARN, "function arg [%s] is not argument expr.",
                      funcSubExprVec[1]->getExprString().c_str());
            return NULL;
        }
        if (!argExpr->getConstValue(split)) {
            AUTIL_LOG(WARN, "function arg [%s] is not string value.",
                      argExpr->getExprString().c_str());
            return NULL;
        }
    }

    if (ET_ARGUMENT == funcSubExprVec[0]->getType())
    {
        return SubFunctionCreatorWrapper::createSubJoinVarFunction(
                funcSubExprVec, split);
    }
    return SubFunctionCreatorWrapper::createSubJoinFunction(
            funcSubExprVec, split);
}


}
