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
#ifndef ISEARCH_EXPRESSION_MODULE_H
#define ISEARCH_EXPRESSION_MODULE_H

#include "expression/common.h"
#include "expression/plugin/DllWrapper.h"

DECLARE_RESOURCE_READER

namespace expression {

class ModuleFactory;

class Module
{
public:
    static const std::string CREATE_FACTORY_FUNCTION_NAME;
    static const std::string DESTROY_FACTORY_FUNCTION_NAME;
public:
    Module(const std::string &path, const std::string &root,
           const std::string &moduleFuncSuffix = "");
    ~Module();
public:
    typedef ModuleFactory* (*CreateFactoryFun)();
    typedef void (*DestroyFactoryFun)(ModuleFactory *factory);
public:
    bool init(const KeyValueMap &parameters, const resource_reader::ResourceReaderPtr &resourceReader);
    void destroy();
    ModuleFactory* getModuleFactory();
    const std::string& getErrorMsg() const { return _errorMsg; }
private:
    bool doInit(const KeyValueMap &parameters, const resource_reader::ResourceReaderPtr &resourceReader);
    bool initModuleFunctions(const std::string &moduleFuncSuffix = "");
private:
    std::string _moduleRootDir;
    std::string _moduleFileName;
    std::string _moduleFuncSuffix;
    DllWrapper _dllWrapper;

    ModuleFactory *_moduleFactory;
    CreateFactoryFun _createFactoryFun;
    DestroyFactoryFun _destroyFactoryFun;

    std::string _errorMsg;
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_MODULE_H
