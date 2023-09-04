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
#include "navi/log/NaviLogger.h"
#include "navi/engine/Kernel.h"
#include "navi/proto/GraphDef.pb.h"

namespace navi {

Kernel::Kernel()
    : _def(nullptr)
{
}

Kernel::~Kernel() {
}

void Kernel::setNodeDef(const NodeDef *def) {
    _def = def;
}

const std::string &Kernel::getNodeName() const {
    return _def->name();
}

const std::string &Kernel::getKernelName() const {
    return _def->kernel_name();
}

const std::string &Kernel::getDeviceName() const {
    return _def->device();
}

}
