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
#include "navi/engine/PortMergeKernel.h"
#include "navi/engine/LocalPortBase.h"
#include "navi/engine/Node.h"
#include "navi/engine/LocalSubGraph.h"
#include "navi/engine/SubGraphBase.h"
#include "navi/engine/Graph.h"
#include "navi/engine/PartInfo.h"

namespace navi {

IndexType PortMergeKernel::INPUT_PORT_GROUP = 0;
IndexType PortMergeKernel::OUTPUT_PORT = 0;

PortMergeKernel::PortMergeKernel()
        : _port(nullptr)
{
}

PortMergeKernel::~PortMergeKernel() {
}

void PortMergeKernel::def(KernelDefBuilder &builder) const {
    const auto &type = dataType();
    builder.name(getName())
        .inputGroup(PORT_KERNEL_INPUT_PORT, type, inputType())
        .output(PORT_KERNEL_OUTPUT_PORT, type);
    auto infoVec = dependResources();
    for (const auto &info : infoVec) {
        builder.dependOn(info.name, info.require, info.binder);
    }
}

std::vector<StaticResourceBindInfo> PortMergeKernel::dependResources() const {
    return {};
}

InputTypeDef PortMergeKernel::inputType() const {
    return IT_REQUIRE;
}

bool PortMergeKernel::config(KernelConfigContext &ctx) {
    return doConfig(ctx);
}

bool PortMergeKernel::doConfig(KernelConfigContext &ctx) {
    return true;
}

ErrorCode PortMergeKernel::init(KernelInitContext &ctx) {
    _port = ctx._port;
    if (!_port) {
        NAVI_KERNEL_LOG(ERROR, "port is null");
        return EC_UNKNOWN;
    }
    _dataVec.resize(_port->getPartInfo().getUsedPartCount());
    return doInit(ctx);
}

ErrorCode PortMergeKernel::doInit(KernelInitContext &ctx) {
    return EC_NONE;
}

ErrorCode PortMergeKernel::compute(KernelComputeContext &ctx) {
    auto ec = fillInputs(ctx);
    if (ec != EC_NONE) {
        return ec;
    }
    StreamData streamData;
    ec = doCompute(_dataVec, streamData);
    for (auto &streamData : _dataVec) {
        streamData.reset();
    }
    if (EC_NONE != ec) {
        return ec;
    }
    if (!ctx.setOutput(OUTPUT_PORT, streamData.data, streamData.eof)) {
        NAVI_KERNEL_LOG(ERROR, "setOutput failed");
        return EC_UNKNOWN;
    }
    return EC_NONE;
}

ErrorCode PortMergeKernel::fillInputs(KernelComputeContext &ctx) {
    const PartInfo &partInfo = getPartInfo();
    for (NaviPartId partId : partInfo) {
        auto &streamData = _dataVec[partId];
        bool oldEof = streamData.eof;
        auto ec = _port->getData(partId, streamData.data, streamData.eof);
        if (EC_NONE != ec) {
            if (EC_NO_DATA != ec) {
                NAVI_KERNEL_LOG(ERROR, "get data failed");
                return ec;
            }
            continue;
        }
        if (oldEof && !streamData.eof) {
            NAVI_KERNEL_LOG(
                ERROR, "eof blink, partId: %d, portId: %d, port key: %s",
                partId, _port->getPortId(), _port->getPortKey().c_str());
            return EC_UNKNOWN;
        }
    }
    return EC_NONE;
}

const PartInfo &PortMergeKernel::getPartInfo() const {
    return _port->getPartInfo();
}

}
