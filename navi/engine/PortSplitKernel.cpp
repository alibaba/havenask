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
#include "navi/engine/PortSplitKernel.h"
#include "navi/engine/LocalPortBase.h"
#include "navi/engine/Node.h"
#include "navi/engine/LocalSubGraph.h"
#include "navi/engine/SubGraphBase.h"
#include "navi/engine/Graph.h"
#include "navi/engine/PartInfo.h"

namespace navi {

IndexType PortSplitKernel::INPUT_PORT = 0;
IndexType PortSplitKernel::OUTPUT_PORT_GROUP = 0;

PortSplitKernel::PortSplitKernel()
        : _port(nullptr)
{
}

PortSplitKernel::~PortSplitKernel() {
}

void PortSplitKernel::def(KernelDefBuilder &builder) const {
    const auto &type = dataType();
    builder.name(getName())
        .input(PORT_KERNEL_INPUT_PORT, type)
        .outputGroup(PORT_KERNEL_OUTPUT_PORT, type);
    auto infoVec = dependResources();
    for (const auto &info : infoVec) {
        builder.resource(info.name, info.require, info.binder);
    }
}

std::vector<StaticResourceBindInfo> PortSplitKernel::dependResources() const {
    return {};
}

bool PortSplitKernel::config(KernelConfigContext &ctx) {
    return doConfig(ctx);
}

bool PortSplitKernel::doConfig(KernelConfigContext &ctx) {
    return true;
}

ErrorCode PortSplitKernel::init(KernelInitContext &ctx) {
    _port = ctx._port;
    if (!_port) {
        NAVI_KERNEL_LOG(ERROR, "port is null");
        return EC_UNKNOWN;
    }
    _dataVec.resize(_port->getPartInfo().getUsedPartCount());
    return doInit(ctx);
}

ErrorCode PortSplitKernel::doInit(KernelInitContext &ctx) {
    return EC_NONE;
}

ErrorCode PortSplitKernel::compute(KernelComputeContext &ctx) {
    StreamData streamData;
    if (!ctx.getInput(INPUT_PORT, streamData) || !streamData.hasValue) {
        NAVI_KERNEL_LOG(ERROR, "getInput failed, hasValue [%d]",
                        streamData.hasValue);
        return EC_UNKNOWN;
    }
    auto ec = doCompute(streamData, _dataVec);
    if (EC_NONE != ec) {
        return ec;
    }
    return setOutput(ctx, streamData.eof);
}

const PartInfo &PortSplitKernel::getPartInfo() const {
    return _port->getPartInfo();
}

ErrorCode PortSplitKernel::setOutput(KernelComputeContext &ctx, bool eof) {
    if (_port) {
        auto ec = _port->setData(_dataVec, eof);
        return ec;
    } else {
        PortIndex index(0, OUTPUT_PORT_GROUP);
        for (IndexType i = 0; i < _dataVec.size(); i++) {
            auto data = _dataVec[i];
            _dataVec[i].reset();
            index.index = _port->getPartInfo().getUsedPartId(i);
            if (!ctx.setOutput(index, data, eof)) {
                NAVI_KERNEL_LOG(ERROR, "set output failed");
                return EC_UNKNOWN;
            }
        }
        return EC_NONE;
    }
}

}
