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
#include "catalog/store/CatalogReader.h"

namespace catalog {

CatalogReader::CatalogReader(const std::shared_ptr<CatalogState> &state) : _state(state) {}

CatalogReader::~CatalogReader() = default;

Status CatalogReader::getLatestVersion(CatalogVersion &version) { return _state->getLatestVersion(version); }

Status CatalogReader::read(CatalogVersion version, proto::Catalog *proto) { return _state->read(version, proto); }

Status CatalogReader::read(std::vector<proto::Build> *builds) { return _state->read(builds); }

} // namespace catalog
