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
#ifndef ISEARCH_EXPRESSION_PLUGINMANAGER_H
#define ISEARCH_EXPRESSION_PLUGINMANAGER_H

#include "expression/common.h"
#include "expression/plugin/ModuleInfo.h"

DECLARE_RESOURCE_READER

namespace expression {

class Module;

class PlugInManager
{
public:
    PlugInManager(const resource_reader::ResourceReaderPtr &resourceReaderPtr,
                  const std::string &moduleFuncSuffix = "");
    ~PlugInManager();

public:
    bool addModules(const ModuleInfos &moduleInfos);

    Module* getModule(const std::string &moduleName);

public:
    static bool isBuildInModule(const std::string &moduleName) {
        return moduleName.empty() || moduleName == "BuildInModule";
    }
private:
    bool addModule(const ModuleInfo &moduleInfo);
    ModuleInfo getModuleInfo(const std::string &moduleName);
    void clear();
private:
    std::map<std::string, Module*> _name2moduleMap;
    std::map<std::string, ModuleInfo> _name2moduleInfoMap;
    std::string _moduleFuncSuffix;
    resource_reader::ResourceReaderPtr _resourceReaderPtr;
private:
    AUTIL_LOG_DECLARE();
};

EXPRESSION_TYPEDEF_PTR(PlugInManager);

}

#endif //ISEARCH_EXPRESSION_PLUGINMANAGER_H
