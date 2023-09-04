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
#include "catalog/entity/Root.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, Root);

std::unique_ptr<Root> Root::clone() const {
    auto rv = std::make_unique<Root>();
    rv->_version = _version;
    rv->_rootName = _rootName;
    rv->_catalogNames = _catalogNames;
    return rv;
}

Status Root::fromProto(const proto::Root &proto) {
    _version = proto.version();
    _rootName = proto.root_name();
    _catalogNames = {proto.catalog_names().begin(), proto.catalog_names().end()};
    CATALOG_CHECK(_catalogNames.size() == proto.catalog_names().size(),
                  Status::DUPLICATE_ENTITY,
                  "duplicated catalog_names in root record");
    return StatusBuilder::ok();
}

Status Root::toProto(proto::Root *proto) const {
    proto->set_version(_version);
    proto->set_root_name(_rootName);
    std::vector<std::string> names = {_catalogNames.begin(), _catalogNames.end()};
    std::sort(names.begin(), names.end());
    for (const auto &name : names) {
        proto->add_catalog_names(name);
    }
    return StatusBuilder::ok();
}

Status Root::createCatalog(const std::string &catalogName) {
    auto result = _catalogNames.emplace(catalogName);
    CATALOG_CHECK(result.second, Status::DUPLICATE_ENTITY, "catalog:[", catalogName, "] already exists");
    _version++;
    return StatusBuilder::ok();
}

Status Root::dropCatalog(const std::string &catalogName) {
    auto result = _catalogNames.erase(catalogName);
    CATALOG_CHECK(result == 1U, Status::NOT_FOUND, "catalog:[", catalogName, "] not exists");
    _version++;
    return StatusBuilder::ok();
}

} // namespace catalog
