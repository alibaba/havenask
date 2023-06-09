/*===============================================================
*   Copyright (C) 2018 Taobao All rights reserved.
*   
*   File Name：InStringFuncInterface.cpp
*   Creator：ling.wl 
*   Create Date：2018--05--09
*   Description：
*
*   Update Log：
*
================================================================*/

#include "expression/function/InStringFuncInterface.h"
#include "expression/function/ArgumentAttributeExpression.h"
#include "autil/StringUtil.h"
#include "autil/ConstString.h"
#include "autil/StringTokenizer.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace expression {

AUTIL_LOG_SETUP(expression, TypedInStringFuncCreator);

InStringFuncInterface::InStringFuncInterface(AttributeExpressionTyped<autil::MultiChar> *attrExpr,
        bool existFlag, const string &setStr, const string &sep,
        AttributeExpressionTyped<autil::MultiString> *multiStrAttrExpr)
    : _attrExpr(attrExpr)
    , _multiStrAttrExpr(multiStrAttrExpr)
    , _existFlag(existFlag) {
    _pool = new autil::mem_pool::Pool(16 * 1024);
    StringView const_setStr = autil::MakeCString(setStr, _pool);
    const vector<StringView>& c_vec = StringTokenizer::constTokenize(const_setStr, sep,
                                        StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    set<StringView> tempSet(c_vec.begin(), c_vec.end());
    _set.swap(tempSet);
}

InStringFuncInterface::~InStringFuncInterface() {
    _pool->reset();
    delete _pool;
}

bool InStringFuncInterface::doEvaluate(const matchdoc::MatchDoc &matchDoc) {
    bool exist = false;
    if (_attrExpr) {
        const autil::MultiChar &value = _attrExpr->getValue(matchDoc);
        exist = isExist(value);
    } else {
        const autil::MultiString &stringValue = _multiStrAttrExpr->getValue(matchDoc);
        uint32_t size = stringValue.size();
        for (uint32_t i = 0; i < size; ++i) {
            const autil::MultiChar &charValue = stringValue[i];
            if (isExist(charValue)) {
                exist = true;
                break;
            }
        }
    }
    return exist;
}

bool InStringFuncInterface::evaluate(const matchdoc::MatchDoc &matchDoc) {
    bool exist = doEvaluate(matchDoc);
    return exist == _existFlag;
}

void InStringFuncInterface::batchEvaluate(matchdoc::MatchDoc *matchDocs, 
                                       uint32_t docCount) {
    assert(_ref);
    for (uint32_t i = 0; i < docCount; i++) {
        const matchdoc::MatchDoc& matchDoc = matchDocs[i];
        bool exist = doEvaluate(matchDoc);
        _ref->set(matchDoc, exist == _existFlag);
    }
}

////////////////////////////////////////////////////////////

FunctionInterface *TypedInStringFuncCreator::createTypedFunctionImp(
        const AttributeExpressionVec& funcSubExprVec, bool existFlag) {
    AttributeExpressionTyped<autil::MultiChar> *attrExpr = NULL;
    AttributeExpressionTyped<autil::MultiString> *multiStrAttrExpr = NULL;
    attrExpr = dynamic_cast<AttributeExpressionTyped<autil::MultiChar> *>(funcSubExprVec[0]);
    if (!attrExpr) {
        multiStrAttrExpr = dynamic_cast<AttributeExpressionTyped<autil::MultiString> *>(funcSubExprVec[0]);
    }
    if (!attrExpr && !multiStrAttrExpr) {
        AUTIL_LOG(WARN, "unexpected expression for in function: %s.",
                  funcSubExprVec[0]->getExprString().c_str());
        return NULL;
    }
    ArgumentAttributeExpression *arg =
        dynamic_cast<ArgumentAttributeExpression*>(funcSubExprVec[1]);
    if (!arg) {
        AUTIL_LOG(WARN, "invalid arg expression for in_string function: %s.",
                funcSubExprVec[1]->getExprString().c_str());
        return NULL;
    }
    string setStr;
    if (!arg->getConstValue<string>(setStr)) {
        AUTIL_LOG(WARN, "invalid arg expression for in_string function: %s.",
                funcSubExprVec[1]->getExprString().c_str());
        return NULL;
    }
    ArgumentAttributeExpression *arg2 =
        dynamic_cast<ArgumentAttributeExpression*>(funcSubExprVec[2]);
     if (!arg2) {
        AUTIL_LOG(WARN, "invalid arg 2 expression for in_string function: %s.",
                funcSubExprVec[2]->getExprString().c_str());
        return NULL;
    }
    string separator;
    if (!arg2->getConstValue<string>(separator) || separator.size() == 0) {
        AUTIL_LOG(WARN, "invalid arg expression for in_string function: %s.",
                funcSubExprVec[2]->getExprString().c_str());
        return NULL;
    }

    return new InStringFuncInterface(attrExpr, existFlag, setStr, separator, multiStrAttrExpr);
}

FunctionInterface *TypedInStringFuncCreator::createTypedFunction(
        const AttributeExpressionVec& funcSubExprVec, bool existFlag) {
    if (funcSubExprVec.size() != 3) {
        AUTIL_LOG(WARN, "function only accept three args.");
        return NULL;
    }

    ExprValueType evt = funcSubExprVec[0]->getExprValueType();
    if (EVT_STRING == evt) {
        return createTypedFunctionImp(funcSubExprVec, existFlag);
    } else {
        AUTIL_LOG(WARN, "in_string function can't accept %d type arg.", int(evt));
        return NULL;
    }
    return NULL;
}

bool InStringFuncInterfaceCreator::init(
        const KeyValueMap &funcParameter,
        const resource_reader::ResourceReaderPtr &resourceReader)
{
    return true;
}

FunctionInterface *InStringFuncInterfaceCreator::createFunction(
        const AttributeExpressionVec& funcSubExprVec,
        AttributeExpressionCreator *creator)
{
    return TypedInStringFuncCreator::createTypedFunction(funcSubExprVec, true);
}

bool NotInStringFuncInterfaceCreator::init(
        const KeyValueMap &funcParameter,
        const resource_reader::ResourceReaderPtr &resourceReader)
{
    return true;
}

FunctionInterface *NotInStringFuncInterfaceCreator::createFunction(
        const AttributeExpressionVec& funcSubExprVec,
        AttributeExpressionCreator *creator)
{
    return TypedInStringFuncCreator::createTypedFunction(funcSubExprVec, false);
}

}
