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
#ifndef NAVI_KERNELCREATOR_H
#define NAVI_KERNELCREATOR_H

#include "google/protobuf/map.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {

class Kernel;

struct PortInfo
{
    PortInfo(PortIndex index_, const void *def_)
        : index(index_)
        , def(def_)
    {
    }
    PortInfo()
        : def(nullptr)
    {
    }
    bool isValid() const {
        return def && index.isValid();
    }
    bool isGroup() const {
        return index.group != INVALID_INDEX;
    }
    PortIndex index;
    const void *def;
};

typedef std::unordered_map<std::string, PortInfo> PortMap;
typedef ::google::protobuf::Map<std::string, std::string> AttrMap;
typedef std::function<Kernel *(void *&baseAddr)> KernelCreateFunc;
typedef std::unordered_map<std::string, DynamicResourceGroupDef *>
    DynamicResourceInfoMap;

class ResourceCreator;

class KernelCreator : public Creator
{
public:
    KernelCreator(const char *sourceFile, const KernelDefBuilder &builder,
                  const KernelCreateFunc &func);
    KernelCreator();
    ~KernelCreator();
private:
    KernelCreator(const KernelCreator &);
    KernelCreator& operator=(const KernelCreator &);
public:
    bool init(bool isBuildin, Module *module) override;
public:
    CreatorPtr clone() const;
    const std::string &getName() const override {
        return _def->kernel_name();
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    // todo return error msg
    virtual Kernel *create(void *&baseAddr) const;
    std::string getClassName() const override;
    size_t inputSize() const;
    size_t outputSize() const;
    size_t controlOutputSize() const;
    size_t inputGroupSize() const;
    size_t outputGroupSize() const;
    PortInfo getInputPortInfo(const std::string &port, IndexType index) const;
    PortInfo getInputPortInfo(const std::string &name) const;
    PortInfo getOutputPortInfo(const std::string &name) const;
    std::string getInputPortName(PortIndex portIndex) const;
    std::string getOutputPortName(PortIndex portIndex) const;
    const std::map<std::string, bool> &getDependResources() const;
    const std::map<std::string, bool> &getDependResourcesWithoutSelect() const;
    const std::set<std::string> &getEnableBuildResourceSet() const {
        return _enableBuildResourceSet;
    }
    bool addDependBy(const std::string &resource, const DependByDef *def);
    bool getPortTypeStr(const std::string &portName, IoType ioType,
                        std::string &typeStr) const;
    const KernelDef *def() const;
    const BindInfos &getBindInfos() const;
    const DynamicResourceInfoMap &getDynamicGroupInfoMap() const;
    autil::legacy::RapidDocument *getJsonAttrs() const {
        return _jsonAttrsDocument;
    }
    const std::unordered_map<std::string, std::string> &getConfigKeyMap() const {
        return _configKeyMap;
    }
    bool postInit(const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap);
    bool initSelectors();
    bool initEnableBuilds(const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap);
    bool initConfigKey(const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap);
private:
    // for clone
    KernelCreator(const char *sourceFile, KernelDef *def,
                  const KernelCreateFunc &func, const BindInfos &bindInfos);
private:
    bool validateDef() const;
    bool initInputIndex();
    bool initOutputIndex();
    bool initInputGroupIndex();
    bool initOutputGroupIndex();
    bool initDependResources();
    bool initDynamicGroup();
    bool initAttrs();
    bool addToDependResourceMap(const DependResourceDef &resourceDef);
    bool addResourceDependRecur(const std::string &name,
                                bool require,
                                const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap);
protected:
    KernelDef *_def;
    OutputDef _finishPortDef;
    KernelCreateFunc _func;
    // indexes for search
    PortMap _inputMap;
    PortMap _outputMap;
    PortMap _controlOutputMap;
    PortMap _inputGroupMap;
    PortMap _outputGroupMap;
    std::map<std::string, bool> _dependResources;
    std::map<std::string, bool> _dependResourcesWithoutSelect;
    DynamicResourceInfoMap _dynamicGroupMap;
    std::set<std::string> _enableBuildResourceSet;
    std::unordered_map<std::string, std::string> _configKeyMap;
    autil::legacy::RapidDocument *_jsonAttrsDocument = nullptr;
private:
    BindInfos _bindInfos;
};

NAVI_TYPEDEF_PTR(KernelCreator);

}

#endif //NAVI_KERNELCREATOR_H
