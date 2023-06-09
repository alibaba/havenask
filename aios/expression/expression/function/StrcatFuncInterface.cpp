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
#include "expression/function/StrcatFuncInterface.h"
#include "expression/function/ArgumentAttributeExpression.h"
#include "autil/StringUtil.h"
#include "autil/StringTokenizer.h"
#include "autil/MultiValueCreator.h"

using namespace std;
using namespace autil;

namespace expression {

AUTIL_LOG_SETUP(expression, StrcatFuncInterface);
AUTIL_LOG_SETUP(expression, TypedStrcatFuncCreator);

StrcatFuncInterface::StrcatFuncInterface(vector<pair<StringAttr*, string>> *exp_pairs)
                                        : _exp_pairs(exp_pairs)
                                        , _pool(16 * 1024)
{
    assert(_exp_pairs);
}

StrcatFuncInterface::~StrcatFuncInterface() {
    DELETE_AND_SET_NULL(_exp_pairs);
}

MultiChar StrcatFuncInterface::evaluate(const matchdoc::MatchDoc &matchDoc)
{
    string res;
    for (const auto &exp_pair : *_exp_pairs) {
        if (exp_pair.first) {
            const MultiChar &multi_char = exp_pair.first->getValue(matchDoc);
            res.append(multi_char.data(), multi_char.size());
        } else {
            res.append(exp_pair.second);
        }
    }
    return MultiChar(MultiValueCreator::createMultiValueBuffer(
                     res.c_str(), res.size(), &_pool));
}

void StrcatFuncInterface::batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount)
{
    assert(_ref);
    string res;
    for (uint32_t i = 0; i < docCount; i++) {
        res.clear();
        const matchdoc::MatchDoc& matchDoc = matchDocs[i];
        for (const auto &exp_pair : *_exp_pairs) {
            if (exp_pair.first) {
                const MultiChar &multi_char = exp_pair.first->getValue(matchDoc);
                res.append(multi_char.data(), multi_char.size());
            } else {
                res.append(exp_pair.second);
            }
        }
        _ref->set(matchDoc, MultiChar(MultiValueCreator::createMultiValueBuffer(
                                     res.c_str(), res.size())));
    }
}

bool TypedStrcatFuncCreator::CastAttributeExpression(AttributeExpression* expr, StrcatFuncInterface::StringAttr* &attr, string &arg)
{
    ExprValueType evt = expr->getExprValueType();
    if (evt != EVT_STRING) {
        AUTIL_LOG(WARN, "function unsupports [%u] type", evt);
        return false;
    }
    if (expr->isMulti()) {
        AUTIL_LOG(WARN, "function unsupports multi value");
        return false;
    }
    if (ET_ARGUMENT == expr->getType()) {
        auto arg_expr = dynamic_cast<ArgumentAttributeExpression*>(expr);
        if (!arg_expr) {
            AUTIL_LOG(WARN, "cast [%s] to Argument Attribute failed", expr->getExprString().c_str());
            return false;
        }
        arg_expr->getConstValue(arg);
    } else {
        attr = dynamic_cast<StrcatFuncInterface::StringAttr*>(expr);
        if (!attr) {
            AUTIL_LOG(WARN, "cast [%s] to Attribute Expression failed", expr->getExprString().c_str());
            return false;
        }
    }
    return true;
}

FunctionInterface *TypedStrcatFuncCreator::createTypedFunction(const AttributeExpressionVec& funcSubExprVec)
{
    auto *exp_pairs = new vector<pair<StrcatFuncInterface::StringAttr*, string>>();
    exp_pairs->reserve(funcSubExprVec.size());
    for (auto expr : funcSubExprVec) {
        exp_pairs->emplace_back(pair<StrcatFuncInterface::StringAttr*, string>{NULL, ""});
        auto &exp_pair = exp_pairs->back();
        if (!CastAttributeExpression(expr, exp_pair.first, exp_pair.second)) {
            DELETE_AND_SET_NULL(exp_pairs);
            return NULL;
        }
    }
    return new StrcatFuncInterface(exp_pairs);
}

bool StrcatFuncInterfaceCreator::init(
        const KeyValueMap &funcParameter,
        const resource_reader::ResourceReaderPtr &resourceReader)
{
    return true;
}

FunctionInterface *StrcatFuncInterfaceCreator::createFunction(
        const AttributeExpressionVec& funcSubExprVec,
        AttributeExpressionCreator *creator)
{
    return TypedStrcatFuncCreator::createTypedFunction(funcSubExprVec);
}

}
