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
#ifndef NAVI_DEFAULTMERGEKERNEL_H
#define NAVI_DEFAULTMERGEKERNEL_H

#include "navi/engine/PortMergeKernel.h"

namespace navi {

class ReadyBitMap;

class DefaultMergeKernel : public PortMergeKernel
{
public:
    DefaultMergeKernel();
    ~DefaultMergeKernel();
    DefaultMergeKernel(const DefaultMergeKernel &) = delete;
    DefaultMergeKernel &operator=(const DefaultMergeKernel &) = delete;
public:
    std::string getName() const override;
    std::string dataType() const override;
    InputTypeDef inputType() const override;
    ErrorCode doInit(KernelInitContext &ctx) override;
    ErrorCode compute(KernelComputeContext &ctx) override;
    bool isExpensive() const override {
        return false;
    }
    ErrorCode doCompute(const std::vector<StreamData> &dataVec,
                        StreamData &outputData) override;
protected:
    ErrorCode getData(KernelComputeContext &ctx, NaviPartId partId,
                      StreamData &streamData);
    void setFinish(NaviPartId index);
    bool outputEof() const;
    NaviPartId getUsedPartCount() const;
    NaviPartId getUsedPartId(NaviPartId index) const;
protected:
    ReadyBitMap *_dataReadyMap;
    NaviPartId _loopIndex;
};

}

#endif //NAVI_DEFAULTMERGEKERNEL_H
