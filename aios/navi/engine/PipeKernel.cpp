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
#include "navi/engine/PipeKernel.h"

namespace navi {

PipeKernel::PipeKernel(const std::string &name)
    : _name(name)
{
}

PipeKernel::~PipeKernel() {
}

void PipeKernel::def(KernelDefBuilder &builder) const {
    builder.name(_name)
        .input(inputPort(), inputType())
        .output(outputPort(), outputType());
    auto infoVec = dependResources();
    for (const auto &info : infoVec) {
        builder.dependOn(info.name, info.require, info.binder);
    }
}

std::vector<StaticResourceBindInfo> PipeKernel::dependResources() const {
    return {};
}

std::string PipeKernel::inputPort() const {
    return MAP_KERNEL_INPUT_PORT;
}

std::string PipeKernel::outputPort() const {
    return MAP_KERNEL_OUTPUT_PORT;
}

}
