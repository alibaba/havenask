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

#include <functional>
#include <memory>

#include "catalog/store/IStore.h"

namespace catalog {

using StoreCreator = std::function<std::unique_ptr<IStore>()>;

class StoreFactory {
public:
    ~StoreFactory();

private:
    StoreFactory();

public:
    static StoreFactory *getInstance();
    std::unique_ptr<IStore> create(const std::string &uri) const;

public:
    void registerStore(std::string type, StoreCreator creator);

private:
    static std::string parseTypeFromUri(const std::string &uri);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

struct Register {
    std::string type;
    StoreCreator creator;
    Register(const std::string &type, const StoreCreator &creator);
    ~Register();
};

#define REGISTER_STORE(name, type, creator) static Register __regist##name(type, creator)
} // namespace catalog
