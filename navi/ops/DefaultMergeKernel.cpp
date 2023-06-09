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
#include "navi/ops/DefaultMergeKernel.h"
#include "navi/engine/Port.h"
#include "navi/util/ReadyBitMap.h"
#include "navi/util/CommonUtil.h"

namespace navi {

DefaultMergeKernel::DefaultMergeKernel()
        : _dataReadyMap(nullptr)
        , _loopIndex(0)
{
}

DefaultMergeKernel::~DefaultMergeKernel() {
    ReadyBitMap::freeReadyBitMap(_dataReadyMap);
}

std::string DefaultMergeKernel::getName() const {
    return DEFAULT_MERGE_KERNEL;
}

std::string DefaultMergeKernel::dataType() const {
    return "";
}

InputTypeDef DefaultMergeKernel::inputType() const {
    return IT_OPTIONAL;
}

ErrorCode DefaultMergeKernel::doInit(KernelInitContext &ctx) {
    auto poolPtr = ctx.getPool();
    _dataReadyMap = getPartInfo().createReadyBitMap(poolPtr.get());
    _dataReadyMap->setFinish(false);
    return EC_NONE;
}

ErrorCode DefaultMergeKernel::compute(KernelComputeContext &ctx) {
    for (auto loopCount = 0; loopCount < getUsedPartCount(); ++loopCount) {
        StreamData streamData;
        auto index = getUsedPartId(_loopIndex);
        auto ec = getData(ctx, index, streamData);
        _loopIndex++;
        if (_loopIndex >= getUsedPartCount()) {
            _loopIndex = 0;
        }
        if (EC_NONE != ec) {
            NAVI_KERNEL_LOG(ERROR, "get data failed, index: %d", index);
            return ec;
        }
        if (!streamData.hasValue) {
            continue;
        }
        if (streamData.eof) {
            _dataReadyMap->setFinish(index, true);
        }
        auto eof = _dataReadyMap->isFinish();
        NAVI_KERNEL_LOG(DEBUG, "output data [%p] eof [%d]",
                        streamData.data.get(), eof);
        if (!ctx.setOutput(OUTPUT_PORT, streamData.data, eof)) {
            NAVI_KERNEL_LOG(ERROR, "setOutput failed");
            return EC_UNKNOWN;
        }
        return EC_NONE;
    }
    // release pool per batch
    releasePool();
    ctx.setIgnoreDeadlock();
    return EC_NONE;
}

ErrorCode DefaultMergeKernel::getData(KernelComputeContext &ctx,
                                      NaviPartId partId,
                                      StreamData &streamData)
{
    if (_port) {
        auto ec = _port->getData(partId, streamData.data, streamData.eof);
        if (EC_NO_DATA == ec) {
            return EC_NONE;
        } else if (EC_NONE != ec) {
            NAVI_KERNEL_LOG(ERROR, "get port data failed ec [%s], partId: %d",
                            CommonUtil::getErrorString(ec), partId);
            return ec;
        }
        streamData.hasValue = true;
    } else {
        PortIndex index;
        index.index = partId;
        index.group = 0;
        if (!ctx.getInput(index, streamData)) {
            NAVI_KERNEL_LOG(ERROR, "getInput failed, index: %d", partId);
            return EC_UNKNOWN;
        }
    }
    return EC_NONE;
}

ErrorCode DefaultMergeKernel::doCompute(const std::vector<StreamData> &dataVec,
                                        StreamData &outputData)
{
    assert(false);
    return EC_NONE;
}

NaviPartId DefaultMergeKernel::getUsedPartCount() const {
    return getPartInfo().getUsedPartCount();
}

NaviPartId DefaultMergeKernel::getUsedPartId(NaviPartId index) const {
    return getPartInfo().getUsedPartId(index);
}

void DefaultMergeKernel::releasePool() {
    if (_port) {
        _port->releasePool();
    }
}

REGISTER_KERNEL(DefaultMergeKernel);

}
