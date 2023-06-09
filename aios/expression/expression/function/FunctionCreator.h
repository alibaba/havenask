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
#ifndef ISEARCH_FUNCTIONCREATOR_H
#define ISEARCH_FUNCTIONCREATOR_H

#include "expression/common.h"
#include "expression/framework/TypeTraits.h"
#include "expression/framework/AttributeExpressionCreator.h"
#include "expression/function/FunctionInterface.h"
#include "expression/function/ArgumentAttributeExpression.h"

DECLARE_RESOURCE_READER

namespace expression {
static const uint32_t FUNCTION_UNLIMITED_ARGUMENT_COUNT = (uint32_t)-1;
static const uint32_t TYPE_DEDUCE_BY_LAST_ARGUMENT = (uint32_t)-1; 

enum FunctionType {
    FT_NORMAL,                // resultType and isMulti specified by FunctionProtoInfo
    FT_TEMPLATE,              // resultType and isMulti are same as argument indicated by typeDeduceArgmentIdx
    FT_TEMPLATE_RESULT_TYPE,  // only resultType is same as argument indicated by typeDeduceArgmentIdx
};

enum FuncActionScopeType {
    FUNC_ACTION_SCOPE_MAIN_DOC,
    FUNC_ACTION_SCOPE_SUB_DOC,
    FUNC_ACTION_SCOPE_ADAPTER,
};

// **** custom mode : 
//    FunctionExpression::batchEvaluate will call FunctionInterface::batchEvaluate 
//    1. user should ensure override FunctionInterface::batchEvaluate
//    2. user should ensure non-void type functionInterface call _ref->set()
//            for each matchdoc in FunctionInterface::batchEvaluate
//    3. void type functionInterface _ref == NULL
// **** default mode:
//    FunctionExpression::batchEvaluate will call FunctionInterface::evaluate for each matchdoc
enum FuncBatchEvaluateMode {
    FUNC_BATCH_DEFAULT_MODE,
    FUNC_BATCH_CUSTOM_MODE,
};

struct FunctionProtoInfo
{
    uint32_t argCount;
    ExprValueType resultType;
    bool isMulti;
    FunctionType funcType;
    FuncActionScopeType scopeType;
    FuncBatchEvaluateMode batchEvaluateMode;
    uint32_t typeDeduceArgmentIdx; // default is 0, type deduce from first argument

    FunctionProtoInfo(uint32_t argCount_ = 0, 
                      ExprValueType resultType_ = EVT_UNKNOWN, 
                      bool isMulti_ = false,
                      FunctionType funcType_ = FT_NORMAL,
                      FuncActionScopeType scopeType_ = FUNC_ACTION_SCOPE_ADAPTER,
                      FuncBatchEvaluateMode batchEvaluateMode_ = FUNC_BATCH_DEFAULT_MODE,
                      uint32_t typeDeduceArgmentIdx_ = 0)
        : argCount(argCount_)
        , resultType(resultType_)
        , isMulti(isMulti_)
        , funcType(funcType_)
        , scopeType(scopeType_)
        , batchEvaluateMode(batchEvaluateMode_)
        , typeDeduceArgmentIdx(typeDeduceArgmentIdx_)
    {}

    bool operator == (const FunctionProtoInfo& other) const {
        return argCount == other.argCount
            && resultType == other.resultType
            && isMulti == other.isMulti
            && funcType == other.funcType
            && scopeType == other.scopeType
            && batchEvaluateMode == other.batchEvaluateMode
            && typeDeduceArgmentIdx == other.typeDeduceArgmentIdx;
    }

    bool operator != (const FunctionProtoInfo& protoInfo) const {
        return !(*this == protoInfo);
    }
};

typedef std::map<std::string, FunctionProtoInfo> FuncProtoInfoMap;
#define FUNCTION_CREATOR_CLASS_NAME(classname) classname##Creator
#define FUNCTION_CREATOR_CLASS_BEGIN(classname)     \
    class FUNCTION_CREATOR_CLASS_NAME(classname) :  \
        public expression::FunctionCreator {

#define FUNCTION_DESCRIBE_DEFINE(classname, funcName, argCount, ...)    \
    public:                                                             \
    expression::FunctionProtoInfo getFuncProtoInfo() const {            \
        return expression::FunctionProtoInfo(                           \
                argCount,                                               \
                classname::getType(),                                   \
                classname::isMultiValue(),                              \
                ## __VA_ARGS__);                                        \
    }                                                                   \
    std::string getFuncName() const {                                   \
        return funcName;                                                \
    }

#define TEMPLATE_FUNCTION_DESCRIBE_DEFINE(T, funcName, argCount, ...)   \
    public:                                                             \
    expression::FunctionProtoInfo getFuncProtoInfo() const {            \
        return expression::FunctionProtoInfo(argCount,                  \
                expression::AttrValueType2ExprValueType<T>::evt,        \
                expression::AttrValueType2ExprValueType<T>::isMulti,    \
                ## __VA_ARGS__);                                        \
    }                                                                   \
    std::string getFuncName() const {                                   \
        return funcName;                                                \
    }

#define TEMPLATE_TYPE_FUNCTION_DESCRIBE_DEFINE(funcName, argCount, ...) \
    public:                                                         \
    expression::FunctionProtoInfo getFuncProtoInfo() const {        \
        return expression::FunctionProtoInfo(                       \
                argCount, EVT_UNKNOWN, false, FT_TEMPLATE, ##__VA_ARGS__); \
    }                                                               \
    std::string getFuncName() const {                               \
        return funcName;                                            \
    }


#define FUNCTION_OVERRIDE_FUNC()                                        \
    public:                                                             \
    bool init(const expression::KeyValueMap &funcParameter,             \
              const resource_reader::ResourceReaderPtr &resourceReader); \
    expression::FunctionInterface *createFunction(                      \
            const expression::AttributeExpressionVec& funcSubExprVec,   \
            expression::AttributeExpressionCreator *creator)

#define FUNCTION_CREATOR_CLASS_END() \
    private:                         \
    AUTIL_LOG_DECLARE(); }           \

#define AIOS_DECLARE_FUNCTION_CREATOR(classname, funcName, argCount) \
    FUNCTION_CREATOR_CLASS_BEGIN(classname)                     \
    FUNCTION_DESCRIBE_DEFINE(classname, funcName, argCount);    \
    FUNCTION_CREATOR_CLASS_END()

#define AIOS_DECLARE_TEMPLATE_FUNCTION_CREATOR(classname, T, funcName, argCount, ...) \
    FUNCTION_CREATOR_CLASS_BEGIN(classname)                             \
    TEMPLATE_FUNCTION_DESCRIBE_DEFINE(T, funcName, argCount, ## __VA_ARGS__); \
    FUNCTION_CREATOR_CLASS_END()

#define DECLARE_TEMPLATE_TYPE_FUNCTION_CREATOR(classname, funcName, argCount, ...) \
    FUNCTION_CREATOR_CLASS_BEGIN(classname)                             \
    TEMPLATE_TYPE_FUNCTION_DESCRIBE_DEFINE(funcName, argCount, ## __VA_ARGS__); \
    FUNCTION_CREATOR_CLASS_END()

#define DECLARE_SIMPLE_FUNCTION_CREATOR(classname, funcName, argCount)  \
    FUNCTION_CREATOR_CLASS_BEGIN(classname)                             \
    FUNCTION_DESCRIBE_DEFINE(funcName, argCount);                       \
    FUNCTION_OVERRIDE_FUNC();                                           \
    FUNCTION_CREATOR_CLASS_END()

#define DECLARE_TEMPLATE_SIMPLE_FUNCTION_CREATOR(classname, T, funcName, argCount) \
    FUNCTION_CREATOR_CLASS_BEGIN(classname)                             \
    TEMPLATE_FUNCTION_DESCRIBE_DEFINE(T, funcName, argCount);           \
    FUNCTION_OVERRIDE_FUNC();                                           \
    FUNCTION_CREATOR_CLASS_END()

#define DECLARE_TEMPLATE_TYPE_SIMPLE_FUNCTION_CREATOR(classname, funcName, argCount) \
    FUNCTION_CREATOR_CLASS_BEGIN(classname)                             \
    TEMPLATE_TYPE_FUNCTION_DESCRIBE_DEFINE(funcName, argCount);         \
    FUNCTION_OVERRIDE_FUNC();                                           \
    FUNCTION_CREATOR_CLASS_END()


class FunctionCreator
{
public:
    FunctionCreator() {}
    virtual ~FunctionCreator() {}
private:
    FunctionCreator(const FunctionCreator &);
    FunctionCreator& operator=(const FunctionCreator &);
public:
    virtual bool init(const KeyValueMap &funcParameter,
                      const resource_reader::ResourceReaderPtr &resourceReader) = 0;
    virtual FunctionInterface *createFunction(
            const AttributeExpressionVec& funcSubExprVec,
            AttributeExpressionCreator *creator) = 0;
protected:
    template <typename T>
    bool getParam(AttributeExpression *expression, T &value) {
        ArgumentAttributeExpression *modelNameExpr = 
            dynamic_cast<ArgumentAttributeExpression*>(expression);
        if (!modelNameExpr) {
            return false;
        }
        return modelNameExpr->getConstValue(value);
    }
};

}

#endif //ISEARCH_FUNCTIONCREATOR_H
