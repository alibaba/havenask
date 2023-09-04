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
#include <regex>

namespace navi {

struct BizSummary;

class CreatorManager
{
public:
    CreatorManager(const std::string &bizName,
                   const std::shared_ptr<CreatorManager> &parentManager);
    ~CreatorManager();
private:
    CreatorManager(const CreatorManager &);
    CreatorManager &operator=(const CreatorManager &);
public:
    bool init(const std::string &configPath,
              const std::vector<std::string> &modules,
              const ModuleManagerPtr &moduleManager);
    const KernelCreator *getKernelCreator(const std::string &kernelName) const;
    const ResourceCreator *getResourceCreator(const std::string &resourceName) const;
    const std::map<std::string, bool> *getResourceDependResourceMap(const std::string &name) const;
    const std::unordered_set<std::string> *getResourceReplacerSet(const std::string &name) const;
    bool supportReplace(const std::string &from, const std::string &to);
    ResourceStage getResourceStage(const std::string &name) const;
    const Type *getType(const std::string &typeId) const;
    bool collectMatchedKernels(const std::string &kernelRegex, bool &emptyMatch,
                               std::set<std::string> &kernelSet) const;
    void fillModuleSummary(BizSummary &bizSummary) const;
    std::set<std::string> getKernelSet() const;
    void setTypeMemoryPoolR(const MemoryPoolRPtr &memoryPoolR);
private:
    bool initModules(const std::string &configPath,
                     const std::vector<std::string> &modules,
                     const ModuleManagerPtr &moduleManager);
    bool initBuildinModule();
    bool initRegistrys();
    bool validateRegistry() const;
    bool validateKernelRegistry() const;
    bool validateResourceRegistry() const;
    bool validateTypeRegistry() const;
    bool initMergedMaps();
    bool initKernelMergedMap();
    bool initResourceMergedMap();
    bool initTypeMergedMap();
    void addToKernelMergedMap(const CreatorMap &creatorMap);
    void addToResourceMergedMap(const CreatorMap &creatorMap);
    bool initDependBy();
    bool initProduce();
    bool initReplace();
    bool addResourceDependBy(const std::string &name,
                             ResourceCreator *resourceCreator);
    bool addKernelDependBy(const std::string &name,
                           ResourceCreator *resourceCreator);
    Creator *getDependByResource(const std::string &name, bool &isClone);
    Creator *getDependByKernel(const std::string &name);
    bool initEnableBuilds();
    bool initKernelDependant();
    bool initResourceDependant();
    bool initJsonizeInfo();
private:
    static const char *getRegexErrorStr(std::regex_constants::error_type code);
private:
    std::string _bizName;
    std::shared_ptr<CreatorManager> _parentManager;
    std::map<std::string, ModulePtr> _moduleMap;
    CreatorRegistry _resourceRegistry;
    CreatorRegistry _kernelRegistry;
    CreatorRegistry _typeRegistry;
    CreatorMap _cloneKernelCreatorMap;
    CreatorMap _cloneResourceCreatorMap;
    std::unordered_map<std::string, KernelCreator *> _mergedKernelCreatorMap;
    std::unordered_map<std::string, ResourceCreator *> _mergedResourceCreatorMap;
    std::unordered_map<std::string, Type *> _mergedTypeMap;
};

NAVI_TYPEDEF_PTR(CreatorManager);

}

#endif //NAVI_CREATORMANAGER_H
