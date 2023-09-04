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
#include "catalog/store/CatalogWriter.h"

namespace catalog {

CatalogWriter::CatalogWriter(const std::shared_ptr<CatalogState> &state) : _state(state) {}

CatalogWriter::~CatalogWriter() = default;

Status CatalogWriter::write(CatalogSnapshot *oldSnapshot, CatalogSnapshot *newSnapshot) {
    return _state->write(newSnapshot);
}

Status CatalogWriter::updateStatus(CatalogSnapshot *newSnapshot, const proto::EntityStatus &target) {
    UpdateResult result;
    const_cast<Catalog *>(newSnapshot)->updateStatus(target, result);
    if (!result.hasDiff()) {
        return StatusBuilder::ok();
    }

    return _state->write(newSnapshot);
}

Status CatalogWriter::write(const std::vector<proto::Build> &builds) { return _state->write(builds); }

} // namespace catalog
