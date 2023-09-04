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
#include "suez/turing/expression/util/VirtualAttribute.h"

#include <cstddef>

#include "autil/DataBuffer.h"

using namespace std;

namespace suez {
namespace turing {

VirtualAttribute::VirtualAttribute(const string &name, SyntaxExpr *syntaxExpr) : _name(name), _syntaxExpr(syntaxExpr) {}

VirtualAttribute::~VirtualAttribute() {
    delete _syntaxExpr;
    _syntaxExpr = NULL;
}

void VirtualAttribute::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_name);
    dataBuffer.write(_syntaxExpr);
}

void VirtualAttribute::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_name);
    dataBuffer.read(_syntaxExpr);
}

} // namespace turing
} // namespace suez
