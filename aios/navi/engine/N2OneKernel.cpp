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
#include "navi/engine/N2OneKernel.h"

namespace navi {

N2OneKernel::N2OneKernel(const std::string &name)
    : PipeKernel(name)
{
}

N2OneKernel::~N2OneKernel() {
}

ErrorCode N2OneKernel::compute(KernelComputeContext &ctx) {
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    bool eof = false;
    navi::DataPtr data;
    ctx.getInput(inputIndex, data, eof);
    if (data) {
        auto ec = collect(data);
        if (EC_NONE != ec) {
            return ec;
        }
    }
    if (eof) {
        DataPtr data;
        auto ec = finalize(data);
        if (ec != EC_NONE) {
            return ec;
        }
        ctx.setOutput(outputIndex, data, true);
    }
    return EC_NONE;
}

}

