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
#pragma once

#include <future_lite/Common.h>
#include <memory>
#include <unordered_map>

namespace future_lite {

struct Destructor {
    virtual void operator()(void *data) = 0;
    virtual ~Destructor() = default;
};

// coroutine specific storage variable
struct CoroSpecVar {
    void *_data = nullptr;
    std::shared_ptr<Destructor> _destructor = nullptr;

    CoroSpecVar() noexcept = default;

    CoroSpecVar(void *data, std::shared_ptr<Destructor> fn)
        : _data(data), _destructor(std::move(fn)) {
        logicAssert(nullptr != _destructor,
                    "clean up function cannot be null!");
    }

    void destroy() { (*_destructor)(_data); }
};

typedef std::unordered_map<uintptr_t, CoroSpecVar> CoroVarMap;

struct CoroSpecStorage {
    CoroSpecStorage *_parent = nullptr;
    CoroVarMap _varMap{};
    explicit CoroSpecStorage(CoroSpecStorage *parent): _parent(parent) {}
    ~CoroSpecStorage() {
        for (auto &data: _varMap) {
            data.second.destroy();
        }
        _varMap.clear();
    }
};

}  // namespace future_lite