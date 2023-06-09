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
#ifndef ISEARCH_FUNCTIONINTERFACEFACTORY_H_
#define ISEARCH_FUNCTIONINTERFACEFACTORY_H_

#include "expression/plugin/ModuleFactory.h"
#include "expression/function/FunctionInterface.h"
#include "expression/function/FunctionCreator.h"
#include "expression/function/FunctionProvider.h"

DECLARE_RESOURCE_READER

namespace expression {

class AttributeExpressionCreator;

const std::string MODULE_FUNC_FUNCTION = "_Function";

#define AIOS_REGISTER_FUNCTION_CREATOR(FuncCreatorName)                       \
    {                                                                   \
        FuncCreatorName *creator = new FuncCreatorName();               \
        std::string funcName = creator->getFuncName();                  \
        if (_funcCreatorMap.find(funcName) != _funcCreatorMap.end()) {  \
            delete creator;                                             \
            return false;                                               \
        }                                                               \
        _funcProtoInfoMap[funcName] = creator->getFuncProtoInfo();      \
        _funcCreatorMap[funcName] = creator;                            \
    }
class FunctionInterfaceFactory : public ModuleFactory
{
public:
    FunctionInterfaceFactory();
    virtual ~FunctionInterfaceFactory();
public:
    virtual bool registerFunctions() = 0;
    bool HasRegisted() const { return _funcCreatorMap.size() != 0; }
public:
    bool init(const expression::KeyValueMap &parameters, 
                      const resource_reader::ResourceReaderPtr &resourceReader) override;
    virtual bool initFunctionCreators(
            const std::map<std::string, KeyValueMap> &funcParameters,
            const resource_reader::ResourceReaderPtr &resourceReader);
    void destroy() override;

public:
    const FuncProtoInfoMap &getFuncProtoInfos() const;
    bool getFunctionProtoInfo(const std::string &funcName,
                              FunctionProtoInfo &funcProto) const;
    FunctionInterface* createFunctionInterface(
            const std::string &functionName,
            const AttributeExpressionVec &funcSubExprVec,
            AttributeExpressionCreator *attrExprCreator);
private:
    const KeyValueMap& getKVParam(const std::string &funcName,
            const std::map<std::string, KeyValueMap> &funcParameters) const;
protected:
    typedef std::map<std::string, FunctionCreator*> FuncCreatorMap;
protected:
    FuncProtoInfoMap _funcProtoInfoMap;
    FuncCreatorMap _funcCreatorMap;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FunctionInterfaceFactory> FunctionInterfaceFactoryPtr;
}

#endif //ISEARCH_FUNCTIONINTERFACEFACTORY_H_
