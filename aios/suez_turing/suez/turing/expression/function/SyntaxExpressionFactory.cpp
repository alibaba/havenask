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
#include "suez/turing/expression/function/SyntaxExpressionFactory.h"

#include <algorithm>
#include <cstddef>

#include "alog/Logger.h"

namespace suez {
namespace turing {
struct FunctionCreatorResource;
} // namespace turing
} // namespace suez

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, SyntaxExpressionFactory);

SyntaxExpressionFactory::SyntaxExpressionFactory() : _defaultInit(true) {}

SyntaxExpressionFactory::~SyntaxExpressionFactory() {
    for (FuncCreatorMap::iterator it = _funcCreatorMap.begin(); it != _funcCreatorMap.end(); ++it) {
        delete it->second;
    }
}

bool SyntaxExpressionFactory::initFunctionCreators(const map<string, KeyValueMap> &funcParameters,
                                                   const FunctionCreatorResource &funcCreatorResource) {
    for (FuncCreatorMap::iterator it = _funcCreatorMap.begin(); it != _funcCreatorMap.end(); ++it) {
        const string &funcName = it->first;
        auto funcProcItr = _funcProtoInfoMap.find(funcName);
        if (!needInit(funcName)) {
            delete it->second;
            it->second = NULL;
            if (funcProcItr != _funcProtoInfoMap.end()) {
                _funcProtoInfoMap.erase(funcProcItr);
            }
            AUTIL_LOG(INFO, "default function %s erased.", funcName.c_str());
            continue;
        }
        const auto &params = getKVParam(funcName, funcParameters);
        if (!it->second->init(params, funcCreatorResource)) {
            AUTIL_LOG(ERROR, "init function %s creator failed, params: ", funcName.c_str());
            for (auto item : params) {
                AUTIL_LOG(ERROR, "%s : %s", item.first.c_str(), item.second.c_str());
            }
            return false;
        }
        if (_funcProtoInfoMap.end() != funcProcItr && funcProcItr->second.argCount == 0) {
            _funcList.push_back(funcName);
        }
        AUTIL_LOG(INFO, "init function[%s] success", funcName.c_str());
    }
    return true;
}

const KeyValueMap &SyntaxExpressionFactory::getKVParam(const string &funcName,
                                                       const map<string, KeyValueMap> &funcParameters) const {
    map<string, KeyValueMap>::const_iterator pIt = funcParameters.find(funcName);
    if (pIt != funcParameters.end()) {
        return pIt->second;
    }
    static const KeyValueMap emptyKVMap;
    return emptyKVMap;
}

const FuncInfoMap &SyntaxExpressionFactory::getFuncInfos() const { return _funcProtoInfoMap; }

bool SyntaxExpressionFactory::init(const KeyValueMap &parameters) { return true; }

void SyntaxExpressionFactory::destroy() { delete this; }

FunctionInterface *SyntaxExpressionFactory::createFuncExpression(const string &functionName,
                                                                 const FunctionSubExprVec &funcSubExprVec) {
    FuncCreatorMap::iterator it = _funcCreatorMap.find(functionName);
    if (it == _funcCreatorMap.end()) {
        AUTIL_LOG(ERROR, "no function: [%s]", functionName.c_str());
        return NULL;
    }
    return it->second->createFunction(funcSubExprVec);
}

} // namespace turing
} // namespace suez
