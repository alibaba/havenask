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
#ifndef NAVI_RESOURCECREATOR_H
#define NAVI_RESOURCECREATOR_H

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {

class Resource;
typedef std::map<std::string, const DependByDef *> DependByMap;
typedef std::unordered_map<std::string, DynamicResourceGroupDef *>
    DynamicResourceInfoMap;

class ResourceCreator : public Creator
{
private:
    typedef std::shared_ptr<Resource> ResourcePtr;
    typedef std::function<ResourcePtr(void *&baseAddr)> ResourceCreateFunc;
    typedef std::function<bool(Resource *other)> ResourceCastFunc;
public:
    ResourceCreator(const char *sourceFile, const ResourceDefBuilder &builder,
                    const ResourceCreateFunc &func, const ResourceCastFunc &castFunc);
    virtual ~ResourceCreator();
private:
    ResourceCreator(const ResourceCreator &);
    ResourceCreator &operator=(const ResourceCreator &);
public:
    bool init(bool isBuildin, Module *module) override;
    const std::string &getName() const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    CreatorPtr clone() const;
    ResourceStage getStage() const;
    const std::shared_ptr<ResourceDef> &def() const;
    std::string getClassName() const override;
    ResourcePtr create(void *&baseAddr) const;
    const std::map<std::string, bool> &getDependResources() const;
    const std::set<std::string> &getEnableBuildResourceSet() const {
        return _enableBuildResourceSet;
    }
    const std::set<std::string> &getEnableReplaceSet() const {
        return _enableReplaceSet;
    }
    const std::unordered_set<std::string> &getReplaceBySet() const {
        return _replaceBySet;
    }
    bool supportReplace(const std::string &to) const {
        return _replaceBySet.end() != _replaceBySet.find(to);
    }
    bool addReplacer(ResourceCreator *replacerCreator);
    bool addDependBy(const std::string &resource, const DependByDef *def);
    const DependByMap &getDependByResourceMap() const;
    const DependByMap &getDependByKernelMap() const;
    const BindInfos &getBindInfos() const;
    const DynamicResourceInfoMap &getDynamicGroupInfoMap() const;
    void addResourceDependant(const std::string &resource);
    void addKernelDependant(const std::string &kernel);
    const ProduceInfoDef *getProduceInfo() const;
    bool setProducer(const std::string &producer) {
        if (!_producer.empty()) {
            return false;
        }
        _producer = producer;
        return true;
    }
    const std::string &getProducer() const {
        return _producer;
    }
    bool postInit(const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap);
private:
    // for clone
    ResourceCreator(const char *sourceFile, ResourceDef *def,
                    const ResourceCreateFunc &func, const BindInfos &bindInfos);
    bool validateDef() const;
    bool initDependResources();
    bool initEnableBuilds(const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap);
    bool addResourceDependRecur(const std::string &name,
                                bool require,
                                const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap);
    bool initDynamicGroup();
    bool initDependBy();
    bool initReplace();
    bool addToDependResourceMap(const DependResourceDef &resourceDef);
private:
    std::shared_ptr<ResourceDef> _def;
    ResourceCreateFunc _func;
    ResourceCastFunc _castFunc;
    std::map<std::string, bool> _dependResources;
    DynamicResourceInfoMap _dynamicGroupMap;
    DependByMap _dependByResource;
    DependByMap _dependByKernel;
    BindInfos _bindInfos;
    std::set<std::string> _resourceDependant;
    std::set<std::string> _kernelDependant;
    std::string _producer;
    bool _enableBuildInited = false;
    std::set<std::string> _enableBuildResourceSet;
    std::set<std::string> _enableReplaceSet;
    std::unordered_set<std::string> _replaceBySet;
};

}

#endif //NAVI_RESOURCECREATOR_H
