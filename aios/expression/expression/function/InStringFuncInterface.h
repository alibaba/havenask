/*===============================================================
*   Copyright (C) 2018 Taobao All rights reserved.
*   
*   File Name：InStringFuncInterface.h
*   Creator：ling.wl 
*   Create Date：2018--05--09
*   Description：
*
*   Update Log：
*
================================================================*/
#ifndef ISEARCH_EXPRESSION_INSTRINGFUNCEXPRESSION_H_
#define ISEARCH_EXPRESSION_INSTRINGFUNCEXPRESSION_H_

#include "expression/function/FunctionInterface.h"
#include "expression/function/FunctionCreator.h"
#include "matchdoc/MatchDoc.h"
#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"

namespace expression {

class InStringFuncInterface : public FunctionInterfaceTyped<bool>
{
public:
    InStringFuncInterface(AttributeExpressionTyped<autil::MultiChar> *attrExpr,
                          bool existFlag, const std::string& setStr,
                          const std::string& sep,
                          AttributeExpressionTyped<autil::MultiString> *multiStrAttrExpr = NULL);
    ~InStringFuncInterface();
public:
    /* override */ bool evaluate(const matchdoc::MatchDoc &matchDoc);
    /* override */ void batchEvaluate(
            matchdoc::MatchDoc *matchDocs, uint32_t docCount);
private:
    bool doEvaluate(const matchdoc::MatchDoc &matchDoc);
    bool isExist(const autil::MultiChar &value) const {
        autil::StringView str_val(value.data(), value.size());
        return (_set.find(str_val) != _set.end());
    }
    
private:
    AttributeExpressionTyped<autil::MultiChar> *_attrExpr;
    AttributeExpressionTyped<autil::MultiString> *_multiStrAttrExpr;
    std::set<autil::StringView> _set;
    bool _existFlag;
    autil::mem_pool::Pool *_pool;
private:
    AUTIL_LOG_DECLARE();
};
////////////////////////////////////////////////////////////

class InStringFuncInterfaceCreator : public FunctionCreator {
    TEMPLATE_FUNCTION_DESCRIBE_DEFINE(bool, "in_string", 3,
            FT_NORMAL, FUNC_ACTION_SCOPE_ADAPTER, FUNC_BATCH_CUSTOM_MODE);
    FUNCTION_OVERRIDE_FUNC();
};
class NotInStringFuncInterfaceCreator : public FunctionCreator {
    TEMPLATE_FUNCTION_DESCRIBE_DEFINE(bool, "notin_string", 3,
            FT_NORMAL, FUNC_ACTION_SCOPE_ADAPTER, FUNC_BATCH_CUSTOM_MODE);
    FUNCTION_OVERRIDE_FUNC();
};

class TypedInStringFuncCreator {
public:
    static FunctionInterface *createTypedFunction(const AttributeExpressionVec& funcSubExprVec, bool existFlag);
private:
    static FunctionInterface *createTypedFunctionImp(const AttributeExpressionVec& funcSubExprVec, bool existFlag);
private:
    AUTIL_LOG_DECLARE();
};


}

#endif  // ISEARCH_EXPRESSION_INSTRINGFUNCEXPRESSION_H_
