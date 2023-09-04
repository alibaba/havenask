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
#include "navi/engine/ScopeTerminatorKernel.h"
#include "navi/engine/LocalSubGraph.h"
#include "navi/engine/Node.h"

namespace navi {

ScopeTerminatorKernel::ScopeTerminatorKernel() {
}

ScopeTerminatorKernel::~ScopeTerminatorKernel() {
}

void ScopeTerminatorKernel::def(KernelDefBuilder &builder) const {
    builder.name(getName())
        .inputGroup(SCOPE_TERMINATOR_INPUT_PORT, "", IT_REQUIRE);
    auto outputs = getOutputs();
    for (const auto &output : outputs) {
        builder.output(output, "");
    }
}

std::vector<std::string> ScopeTerminatorKernel::getOutputs() const {
    return {};
}

bool ScopeTerminatorKernel::config(KernelConfigContext &ctx) {
    return true;
}

ErrorCode ScopeTerminatorKernel::init(KernelInitContext &ctx) {
    return EC_NONE;
}

}
