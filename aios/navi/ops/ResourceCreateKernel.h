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
#pragma once

#include "navi/engine/Kernel.h"
#include "navi/ops/ResourceData.h"
#include "navi/engine/MultiLayerResourceMap.h"

namespace navi {

class ReplaceInfoMap;

class ResourceCreateKernel : public Kernel
{
public:
    ResourceCreateKernel();
    ~ResourceCreateKernel();
    ResourceCreateKernel(const ResourceCreateKernel &) = delete;
    ResourceCreateKernel &operator=(const ResourceCreateKernel &) = delete;
public:
    void def(KernelDefBuilder &builder) const override;
    bool config(KernelConfigContext &ctx) override;
    ErrorCode init(KernelInitContext &ctx) override;
    ErrorCode compute(KernelComputeContext &ctx) override;
private:
    bool createResource(KernelComputeContext &ctx, ResourceDataPtr &output);
    bool collectInput(KernelComputeContext &ctx, bool &hasWait);
    bool addData(const DataPtr &data, bool eof, bool &hasWait, bool &allEof);
    ResourceDataPtr createOutput(bool hasWaitInput) const;
    bool canReuse() const;
    void doCreateResource(KernelComputeContext &ctx, ResourceDataPtr &output);
    void fillDependAndReplace(KernelComputeContext &ctx, ResourceDataPtr &resourceData);
    bool collectProduceResult(const ResourceDataPtr &output, ResourceDataPtr &produceResult);
    bool needProduce() const {
        return !_produceName.empty();
    }
    bool validateReplace() const;
private:
    std::string _resourceName;
    std::string _produceName;
    std::string _requireNode;
    bool _init : 1;
    bool _require : 1;
    bool _produceRequire : 1;
    bool _inputEof : 1;
    bool _outputEof : 1;
    bool _produceEof : 1;
    IndexType _resourceGroupIndex;
    ResourceStage _stage;
    ResourceMap _resourceMap;
    MultiLayerResourceMap _multiLayerResourceMap;
    const std::map<std::string, bool> *_dependMap = nullptr;
    std::map<std::string, std::shared_ptr<ResourceData>> _depends;
    const std::unordered_set<std::string> *_replaceSet = nullptr;
    const ReplaceInfoMap *_replaceInfoMap = nullptr;
    AsyncPipePtr _flushPipe;
    AsyncPipePtr _producePipe;
    ResourceDataPtr _output;
    ResourceDataPtr _produceResult;
};

}
