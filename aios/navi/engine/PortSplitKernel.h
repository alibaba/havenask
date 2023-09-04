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
#ifndef NAVI_PORTSPLITKERNEL_H
#define NAVI_PORTSPLITKERNEL_H

#include "navi/engine/Kernel.h"

namespace navi {

class Port;
class PartInfo;

class PortSplitKernel : public Kernel
{
public:
    PortSplitKernel();
    ~PortSplitKernel();
private:
    PortSplitKernel(const PortSplitKernel &);
    PortSplitKernel &operator=(const PortSplitKernel &);
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
    virtual ErrorCode doInit(KernelInitContext &ctx);
    virtual bool doConfig(KernelConfigContext &ctx);
    virtual ErrorCode doCompute(const StreamData &streamData,
                                std::vector<DataPtr> &dataVec) = 0;
public:
    const PartInfo &getPartInfo() const;
protected:
    ErrorCode setOutput(KernelComputeContext &ctx, bool eof);
protected:
    std::vector<DataPtr> _dataVec;
private:
    Port *_port;
protected:
    static IndexType INPUT_PORT;
    static IndexType OUTPUT_PORT_GROUP;
};

}

#endif //NAVI_PORTSPLITKERNEL_H
