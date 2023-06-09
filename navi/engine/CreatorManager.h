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
#ifndef NAVI_CREATORMANAGER_H
#define NAVI_CREATORMANAGER_H

#include "navi/config/NaviConfig.h"
#include "navi/engine/KernelCreator.h"
#include "navi/engine/ModuleManager.h"
#include "navi/engine/ResourceCreator.h"
#include "navi/engine/Type.h"
#include "navi/log/NaviLogger.h"

namespace navi {

typedef std::unordered_map<ResourceLoadType, std::vector<std::string> >
    ResourceLoadTypeMap;

class CreatorManager
{
public:
    CreatorManager();
    ~CreatorManager();
private:
    CreatorManager(const CreatorManager &);
    CreatorManager &operator=(const CreatorManager &);
public:
    bool init(const NaviConfig &config);
    const KernelCreator *getKernelCreator(const std::string &kernelName) const;
    const ResourceCreator *getResourceCreator(const std::string &resourceName) const;
    const Type *getType(const std::string &typeId) const;
    std::vector<std::string> getResourceByLoadType(ResourceLoadType type) const;
private:
    bool initModules(const NaviConfig &config,
                     std::map<std::string, ModulePtr> &modules);
    bool initRegistrys(const std::map<std::string, ModulePtr> &modules);
    bool moduleInit();
    static bool initResourceRegistry(CreatorRegistry &resourceRegistry,
                                     ResourceLoadTypeMap &resourceLoadTypeMap);
    static bool initKernelRegistry(CreatorRegistry &kernelRegistry);
    static bool initTypeRegistry(CreatorRegistry &typeRegistry);
private:
    ModuleManager _moduleManager;
    CreatorRegistry _resourceRegistry;
    CreatorRegistry _kernelRegistry;
    CreatorRegistry _typeRegistry;
    ResourceLoadTypeMap _resourceLoadTypeMap;
};

NAVI_TYPEDEF_PTR(CreatorManager);

}

#endif //NAVI_CREATORMANAGER_H
