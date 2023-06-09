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
#include "expression/function/FunctionInterfaceFactory.h"
#include "expression/framework/AttributeExpressionCreator.h"
#include "resource_reader/ResourceReader.h"

using namespace std;
using namespace resource_reader;

namespace expression {

AUTIL_LOG_SETUP(expression, FunctionInterfaceFactory);

FunctionInterfaceFactory::FunctionInterfaceFactory() {
}

FunctionInterfaceFactory::~FunctionInterfaceFactory() {
    for (FuncCreatorMap::iterator it = _funcCreatorMap.begin();
         it != _funcCreatorMap.end(); ++it)
    {
        delete it->second;
    }
}

bool FunctionInterfaceFactory::initFunctionCreators(
        const map<string, KeyValueMap> &funcParameters,
        const ResourceReaderPtr &resourceReader)
{
    for (FuncCreatorMap::iterator it = _funcCreatorMap.begin(); 
         it != _funcCreatorMap.end(); ++it)
    {
        const string &funcName = it->first;
        if (!it->second->init(getKVParam(funcName, funcParameters), resourceReader)) {
            AUTIL_LOG(ERROR, "init function[%s] creator failed.", funcName.c_str());
            return false;
        }
    }
    return true;
}

const KeyValueMap& FunctionInterfaceFactory::getKVParam(
        const string &funcName,
        const map<string, KeyValueMap> &funcParameters) const
{
    map<string, KeyValueMap>::const_iterator pIt = funcParameters.find(funcName);
    if (pIt != funcParameters.end()) {
        return pIt->second;
    }
    static const KeyValueMap emptyKVMap;
    return emptyKVMap;
}

const FuncProtoInfoMap& FunctionInterfaceFactory::getFuncProtoInfos() const {
    return _funcProtoInfoMap;
}

bool FunctionInterfaceFactory::getFunctionProtoInfo(const string &funcName,
        FunctionProtoInfo &funcProto) const
{
    FuncProtoInfoMap::const_iterator iter = _funcProtoInfoMap.find(funcName);
    if (iter != _funcProtoInfoMap.end()) {
        funcProto = iter->second;
        return true;
    } else {
        AUTIL_LOG(WARN, "can not function proto info for [%s]", funcName.c_str());
        return false;
    }
}

bool FunctionInterfaceFactory::init(const KeyValueMap &parameters,
                                    const resource_reader::ResourceReaderPtr &resourceReader)
{
    return true;
}

void FunctionInterfaceFactory::destroy() {
    delete this;
}

FunctionInterface* FunctionInterfaceFactory::createFunctionInterface(
        const string &functionName, 
        const AttributeExpressionVec &funcSubExprVec,
        AttributeExpressionCreator *attrExprCreator)
{
    FuncCreatorMap::iterator it = _funcCreatorMap.find(functionName);
    if (it == _funcCreatorMap.end()) {
        AUTIL_LOG(ERROR, "no function: [%s]", functionName.c_str());
        return NULL;
    }
    return it->second->createFunction(funcSubExprVec, attrExprCreator);
}

}
