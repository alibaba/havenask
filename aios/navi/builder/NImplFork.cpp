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
#include "navi/builder/NImplFork.h"
#include "navi/builder/PImpl.h"

namespace navi {

NImplFork::NImplFork() {
}

NImplFork::~NImplFork() {
    for (auto p : _ins) {
        delete p.second;
    }
    for (auto p : _outs) {
        delete p.second;
    }
}

P NImplFork::in(const std::string &port) {
    auto it = _ins.find(port);
    if (it != _ins.end()) {
        return P(it->second, INVALID_INDEX);
    }
    auto impl = new PImpl(this, port, true);
    _ins.emplace(port, impl);
    return P(impl, INVALID_INDEX);
}

P NImplFork::out(const std::string &port) {
    auto it = _outs.find(port);
    if (it != _outs.end()) {
        return P(it->second, INVALID_INDEX);
    }
    auto impl = new PImpl(this, port, false);
    _outs.emplace(port, impl);
    return P(impl, INVALID_INDEX);
}

}
