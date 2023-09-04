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
#include "catalog/store/StoreFactory.h"

#include <unordered_map>

#include "autil/Lock.h"

namespace catalog {

struct StoreFactory::Impl {
    std::unordered_map<std::string, StoreCreator> creatorMap;
    mutable autil::ReadWriteLock lock;
};

StoreFactory::StoreFactory() { _impl = std::make_unique<Impl>(); }

StoreFactory::~StoreFactory() = default;

StoreFactory *StoreFactory::getInstance() {
    static StoreFactory instance;
    return &instance;
}

void StoreFactory::registerStore(std::string type, StoreCreator creator) {
    autil::ScopedWriteLock lock(_impl->lock);
    _impl->creatorMap.emplace(std::move(type), std::move(creator));
}

std::unique_ptr<IStore> StoreFactory::create(const std::string &uri) const {
    auto type = parseTypeFromUri(uri);
    autil::ScopedReadLock lock(_impl->lock);
    auto it = _impl->creatorMap.find(type);
    if (it == _impl->creatorMap.end()) {
        return nullptr;
    } else {
        auto store = (it->second)();
        if (!store || !store->init(uri)) {
            return nullptr;
        }
        return store;
    }
}

std::string StoreFactory::parseTypeFromUri(const std::string &uri) {
    if (uri.empty()) {
        return "";
    }
    size_t pos = uri.find_first_of("://");
    if (pos == std::string::npos) {
        return "unknown";
    } else {
        return uri.substr(0, pos);
    }
}

Register::Register(const std::string &type, const StoreCreator &creator) : type(type), creator(creator) {
    StoreFactory::getInstance()->registerStore(std::move(type), std::move(creator));
}
Register::~Register() = default;

} // namespace catalog
