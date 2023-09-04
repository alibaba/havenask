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

#include <map>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "build_service/plugin/ModuleFactory.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/function/FunctionCreator.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/function/FunctionMap.h"

namespace suez {
namespace turing {
struct FunctionCreatorResource;

#define REGISTE_FUNCTION_CREATOR(FuncCreatorName)                                                                      \
    {                                                                                                                  \
        FuncCreatorName *creator = new FuncCreatorName();                                                              \
        std::string funcName = creator->getFuncName();                                                                 \
        if (_funcCreatorMap.find(funcName) != _funcCreatorMap.end()) {                                                 \
            delete creator;                                                                                            \
            AUTIL_LOG(ERROR, "func :%s already registed", funcName.c_str());                                           \
            return false;                                                                                              \
        }                                                                                                              \
        _funcProtoInfoMap[funcName] = creator->getFuncProtoInfo();                                                     \
        _funcCreatorMap[funcName] = creator;                                                                           \
        AUTIL_LOG(                                                                                                     \
            INFO, "funcName[%s] need_init[%s]", funcName.c_str(), autil::StringUtil::toString(_defaultInit).c_str());  \
        _needInitMap[funcName] = _defaultInit;                                                                         \
    }

#define REGISTE_FUNCTION_CREATOR_WITH_FUNCNAME(FuncCreatorName, FuncName)                                              \
    {                                                                                                                  \
        FuncCreatorName *creator = new FuncCreatorName(FuncName);                                                      \
        std::string funcName = creator->getFuncName();                                                                 \
        if (_funcCreatorMap.find(funcName) != _funcCreatorMap.end()) {                                                 \
            delete creator;                                                                                            \
            AUTIL_LOG(ERROR, "func :%s already registed", funcName.c_str());                                           \
            return false;                                                                                              \
        }                                                                                                              \
        _funcProtoInfoMap[funcName] = creator->getFuncProtoInfo();                                                     \
        _funcCreatorMap[funcName] = creator;                                                                           \
        AUTIL_LOG(                                                                                                     \
            INFO, "funcName[%s] need_init[%s]", funcName.c_str(), autil::StringUtil::toString(_defaultInit).c_str());  \
        _needInitMap[funcName] = _defaultInit;                                                                         \
    }

static const std::string MODULE_FUNC_FUNCTION = "_Function";

class SyntaxExpressionFactory : public build_service::plugin::ModuleFactory {
public:
    SyntaxExpressionFactory();
    virtual ~SyntaxExpressionFactory();

public:
    virtual bool registeFunctions() = 0;

public:
    virtual bool init(const KeyValueMap &parameters);
    virtual bool initFunctionCreators(const std::map<std::string, KeyValueMap> &funcParameters,
                                      const FunctionCreatorResource &funcCreatorResource);
    virtual void destroy();

public:
    const FuncInfoMap &getFuncInfos() const;
    FunctionInterface *createFuncExpression(const std::string &functionName, const FunctionSubExprVec &funcSubExprVec);

    FunctionCreator *getFuncCreator(const std::string &name) {
        auto it = _funcCreatorMap.find(name);
        if (it != _funcCreatorMap.end()) {
            return it->second;
        }
        return NULL;
    }
    bool registeFunction(const std::string &name, FunctionCreator *creator) {
        if (_funcCreatorMap.find(name) != _funcCreatorMap.end()) {
            return false;
        }
        _funcProtoInfoMap[name] = creator->getFuncProtoInfo();
        _funcCreatorMap[name] = creator;
        return true;
    }
    const std::vector<std::string> &getFuncList() { return _funcList; }

private:
    bool needInit(const std::string &funcName) {
        auto it = _needInitMap.find(funcName);
        if (it != _needInitMap.end()) {
            return it->second;
        }
        return true;
    }
    const KeyValueMap &getKVParam(const std::string &funcName,
                                  const std::map<std::string, KeyValueMap> &funcParameters) const;

protected:
    typedef std::map<std::string, FunctionCreator *> FuncCreatorMap;

protected:
    FuncInfoMap _funcProtoInfoMap;
    FuncCreatorMap _funcCreatorMap;
    std::map<std::string, bool> _needInitMap;
    bool _defaultInit;
    std::vector<std::string> _funcList;
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(SyntaxExpressionFactory);

} // namespace turing
} // namespace suez
