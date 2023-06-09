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

#include <map>
#include <string>

namespace isearch {
namespace sql {

template<typename T>
class RegistryFactory {
public:
    RegistryFactory() {}
    ~RegistryFactory() {}
    RegistryFactory(const RegistryFactory &) = delete;
    RegistryFactory &operator=(const RegistryFactory &) = delete;
public:
    using Registry = std::map<const std::string, T*(*)()>;
private:
    static Registry &registry();
public:
    static bool registerCreatorFunc(const std::string type, T*(*creator)());
    static T *create(const std::string type);
};

template<typename T>
inline typename RegistryFactory<T>::Registry &RegistryFactory<T>::registry() {
    static RegistryFactory::Registry registry;
    return registry;
}

template<typename T>
inline bool RegistryFactory<T>::registerCreatorFunc(const std::string type, T*(*creator)()) {
    registry()[type] = creator;
    return true;
}

template<typename T>
inline T *RegistryFactory<T>::create(const std::string type) {
    const RegistryFactory::Registry &registry = RegistryFactory::registry();
    auto it = registry.find(type);
    if (registry.end() != it) {
        return (it->second)();
    }
    return nullptr;
}

} // namespace sql
} // namespace isearch
