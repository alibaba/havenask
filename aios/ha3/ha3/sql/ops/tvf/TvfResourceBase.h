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
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "ha3/sql/common/Log.h" // IWYU pragma: keep

namespace isearch {
namespace sql {

class TvfResourceBase {
public:
    virtual ~TvfResourceBase() = default;

public:
    virtual std::string name() const = 0;
};

class TvfResourceContainer {
public:
    bool put(TvfResourceBase *obj) {
        auto name = obj->name();
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _runtimeObjs.find(name);
        if (it != _runtimeObjs.end()) {
            return false;
        }
        _runtimeObjs[name] = std::unique_ptr<TvfResourceBase>(obj);
        return true;
    }

    template <typename T>
    T *get(const std::string &name) const {
        static_assert(std::is_base_of<TvfResourceBase, T>::value, "type invalid");
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _runtimeObjs.find(name);
        if (it == _runtimeObjs.end()) {
            return nullptr;
        }
        return dynamic_cast<T *>(it->second.get());
    }

private:
    mutable std::mutex _mutex;
    std::unordered_map<std::string, std::unique_ptr<TvfResourceBase>> _runtimeObjs;
};

typedef std::shared_ptr<TvfResourceContainer> TvfResourceContainerPtr;
typedef std::shared_ptr<TvfResourceBase> TvfResourceBasePtr;
} // namespace sql
} // namespace isearch
