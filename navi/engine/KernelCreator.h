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
typedef std::function<Kernel *(autil::mem_pool::Pool *pool)> KernelCreateFunc;

class KernelCreator : public Creator
{
public:
    KernelCreator(const KernelDefBuilder &builder,
                  const KernelCreateFunc &func);
    KernelCreator();
    ~KernelCreator();
private:
    KernelCreator(const KernelCreator &);
    KernelCreator& operator=(const KernelCreator &);
public:
    bool init() override;
public:
    const std::string &getName() const override {
        return _def->kernel_name();
    }
    // todo return error msg
    virtual Kernel *create(autil::mem_pool::Pool *pool) const;
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
    bool getPortTypeStr(const std::string &portName, IoType ioType,
                        std::string &typeStr) const;
    const KernelDef *def() const;
    const ResourceBindInfos &getBinderInfos() const;
private:
    bool initInputIndex();
    bool initOutputIndex();
    bool initInputGroupIndex();
    bool initOutputGroupIndex();
    bool initDependResources();
protected:
    const KernelDef *_def;
    OutputDef _finishPortDef;
    KernelCreateFunc _func;
    // indexes for search
    PortMap _inputMap;
    PortMap _outputMap;
    PortMap _controlOutputMap;
    PortMap _inputGroupMap;
    PortMap _outputGroupMap;
    std::map<std::string, bool> _dependResources;
private:
    ResourceBindInfos _binderInfos;
};

NAVI_TYPEDEF_PTR(KernelCreator);

}

#endif //NAVI_KERNELCREATOR_H
