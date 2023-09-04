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
#ifndef NAVI_PORTMERGEKERNEL_H
#define NAVI_PORTMERGEKERNEL_H

#include "navi/engine/Kernel.h"

namespace navi {

class Port;
class DefaultMergeKernel;
class PartInfo;

class PortMergeKernel : public Kernel
{
public:
    PortMergeKernel();
    ~PortMergeKernel();
private:
    PortMergeKernel(const PortMergeKernel &);
    PortMergeKernel &operator=(const PortMergeKernel &);
public:
    void def(KernelDefBuilder &builder) const override final;
    bool config(KernelConfigContext &ctx) override final;
    ErrorCode init(KernelInitContext &ctx) override final;
public:
    // user override
    ErrorCode compute(KernelComputeContext &ctx) override;
    virtual std::vector<StaticResourceBindInfo> dependResources() const;
    virtual std::string getName() const = 0;
    virtual std::string dataType() const = 0;
    virtual InputTypeDef inputType() const;
    virtual ErrorCode doInit(KernelInitContext &ctx);
    virtual bool doConfig(KernelConfigContext &ctx);
    virtual ErrorCode doCompute(const std::vector<StreamData> &dataVec,
                                StreamData &outputData) = 0;
public:
    const PartInfo &getPartInfo() const;
private:
    ErrorCode fillInputs(KernelComputeContext &ctx);
private:
    Port *_port;
    std::vector<StreamData> _dataVec;
    friend class DefaultMergeKernel;
protected:
    static IndexType INPUT_PORT_GROUP;
    static IndexType OUTPUT_PORT;
};

}

#endif //NAVI_PORTMERGEKERNEL_H
