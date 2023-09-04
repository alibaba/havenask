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
#include <unordered_set>

#include "autil/Log.h"
#include "catalog/entity/Type.h"
#include "catalog/util/StatusBuilder.h"

namespace catalog {

class Root {
public:
    Root() = default;
    ~Root() = default;
    Root(const Root &) = delete;
    Root &operator=(const Root &) = delete;

public:
    CatalogVersion version() const { return _version; }
    const std::string &rootName() const { return _rootName; }
    const auto &catalogNames() const { return _catalogNames; }

    std::unique_ptr<Root> clone() const;
    Status fromProto(const proto::Root &proto);
    Status toProto(proto::Root *proto) const;

    Status createCatalog(const std::string &catalogName);
    Status dropCatalog(const std::string &catalogName);

private:
    CatalogVersion _version = kInitCatalogVersion;
    std::string _rootName;
    std::unordered_set<std::string> _catalogNames;
    AUTIL_LOG_DECLARE();
};

} // namespace catalog
