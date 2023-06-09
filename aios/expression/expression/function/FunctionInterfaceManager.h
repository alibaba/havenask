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
#ifndef ISEARCH_FUNCTIONINTERFACEMANAGER_H
#define ISEARCH_FUNCTIONINTERFACEMANAGER_H

#include "expression/framework/AttributeExpression.h"
#include "expression/function/FunctionConfig.h"
#include "expression/function/FunctionInterface.h"
#include "expression/function/FunctionInterfaceFactory.h"
#include "expression/function/FunctionResource.h"

DECLARE_RESOURCE_READER

namespace expression {

class AttributeExpressionCreator;
class PlugInManager;
EXPRESSION_TYPEDEF_PTR(PlugInManager);

class FunctionInterfaceManager
{
public:
    FunctionInterfaceManager();
    ~FunctionInterfaceManager();
private:
    FunctionInterfaceManager(const FunctionInterfaceManager &);
    FunctionInterfaceManager& operator=(const FunctionInterfaceManager &);
public:
    // application should maintain factory* life cycle
    // because : user may set same factory to different ExpressionApplication (avoid double free)
    bool init(const FunctionConfig &funcConfig,
              const resource_reader::ResourceReaderPtr &resourceReaderPtr,
              FunctionInterfaceFactory *factory);

    FunctionInterface* createFunctionInterface(
            const std::string &funcName,
            const AttributeExpressionVec &funcSubExprVec,
            AttributeExpressionCreator *attrExprCreator) const;

    bool getFunctionProtoInfo(const std::string &funcName,
                              FunctionProtoInfo &funcProto) const;

private:
    bool funcParamToMap(const std::vector<FunctionInfo> &funcInfos, 
                        std::map<std::string, KeyValueMap> &funcKVMap);

    bool addFunctions(FunctionInterfaceFactory *factory,
                      const std::map<std::string, KeyValueMap> &allFuncKVMap,
                      const resource_reader::ResourceReaderPtr &resourceReaderPtr);

private:
    bool addBuildInFunctions(
            const std::map<std::string, KeyValueMap> &allFuncKVMap,
            const resource_reader::ResourceReaderPtr &resourceReader);

    bool addNonPluginFunctions(
            FunctionInterfaceFactory *factory,
            const std::map<std::string, KeyValueMap> &allFuncKVMap,
            const resource_reader::ResourceReaderPtr &resourceReader,
            bool keepFactory = true);

private:
    typedef std::map<std::string, FunctionInterfaceFactory*> FactoryInfos;

private:
    PlugInManagerPtr _plugInManagerPtr;
    resource_reader::ResourceReaderPtr _resourceReaderPtr;
    FactoryInfos _factorys;
    std::vector<FunctionInterfaceFactory *> _nonPluginFactorys;
private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<FunctionInterfaceManager> FunctionInterfaceManagerPtr;

}

#endif //ISEARCH_FUNCTIONINTERFACEMANAGER_H
