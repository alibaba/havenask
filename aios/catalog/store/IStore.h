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

#include <string>

#include "catalog/proto/CatalogEntity.pb.h"

namespace catalog {

class ICatalogReader;
class ICatalogWriter;

class IStore {
public:
    virtual ~IStore() = default;

public:
    virtual bool init(const std::string &uri) = 0;
    virtual bool writeRoot(const proto::Root &root) = 0;
    virtual bool readRoot(proto::Root &root) = 0;

    virtual std::unique_ptr<ICatalogReader> createReader(const std::string &catalogName) = 0;
    virtual std::unique_ptr<ICatalogWriter> createWriter(const std::string &catalogName) = 0;
};

} // namespace catalog
