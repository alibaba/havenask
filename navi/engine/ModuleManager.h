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
#ifndef NAVI_MODULEMANAGER_H
#define NAVI_MODULEMANAGER_H

#include "navi/engine/CreatorRegistry.h"
#include <map>

namespace navi {

class Module {
public:
    Module(const std::string &path);
    ~Module();
public:
    CreatorRegistry &getResourceRegistry();
    CreatorRegistry &getKernelRegistry();
    CreatorRegistry &getTypeRegistry();
    void setHandle(void *handle);
    const std::string &getPath() const;
private:
    std::string _path;
    void *_handle;
    CreatorRegistry _resourceRegistry;
    CreatorRegistry _kernelRegistry;
    CreatorRegistry _typeRegistry;
};

NAVI_TYPEDEF_PTR(Module);

class ModuleManager
{
public:
    ModuleManager();
    ~ModuleManager();
private:
    ModuleManager(const ModuleManager &);
    ModuleManager &operator=(const ModuleManager &);
public:
    ModulePtr loadModule(const std::string &configPath,
                         const std::string &modulePath);
private:
    std::map<std::string, ModulePtr> _moduleMap;
};

NAVI_TYPEDEF_PTR(ModuleManager);

}

#endif //NAVI_MODULEMANAGER_H
