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
#include "catalog/store/CatalogState.h"

#include "worker_framework/WorkerState.h"

using namespace worker_framework;

namespace catalog {

const std::string CATALOG_STATE_FILE = "catalog.pb";
const std::string BUILD_STATE_FILE = "build.pb";

CatalogState::CatalogState(std::unique_ptr<worker_framework::WorkerState> state) : _state(std::move(state)) {}

CatalogState::~CatalogState() = default;

bool CatalogState::recover() {
    std::string content;
    auto ec = _state->read(content);
    if (ec == WorkerState::EC_FAIL) {
        return false;
    }
    if (ec == WorkerState::EC_UPDATE || ec == WorkerState::EC_OK) {
        proto::CatalogStateEntity proto;
        proto.ParseFromString(content);

        for (size_t i = 0; i < proto.catalogs_size(); i++) {
            _catalogMap.emplace(proto.catalogs(i).version(), proto.catalogs(i));
        }
        for (size_t i = 0; i < proto.builds_size(); i++) {
            _builds.emplace_back(proto.builds(i));
        }
    }

    // not found
    return true;
}

Status CatalogState::write(CatalogSnapshot *newSnapshot) {
    autil::ScopedWriteLock lock(_lock);
    proto::Catalog proto;
    auto s = newSnapshot->toProto(&proto);
    if (!isOk(s)) {
        return s;
    }

    std::map<CatalogVersion, proto::Catalog> newMap = _catalogMap;

    if (newSnapshot->status().code() == proto::EntityStatus::PUBLISHED) {
        for (auto iter = newMap.begin(); iter != newMap.end();) {
            if (iter->first < newSnapshot->version()) {
                iter = newMap.erase(iter);
            } else {
                iter++;
            }
        }
    }
    newMap.emplace(newSnapshot->version(), proto);

    proto::CatalogStateEntity state;
    for (const auto &iter : newMap) {
        auto catalog = state.mutable_catalogs()->Add();
        catalog->CopyFrom(iter.second);
    }
    for (const auto &iter : _builds) {
        auto build = state.mutable_builds()->Add();
        build->CopyFrom(iter);
    }

    std::string content;
    state.SerializeToString(&content);

    auto ec = _state->write(content);
    if (ec != WorkerState::EC_FAIL) {
        _catalogMap = newMap;
        return StatusBuilder::ok();
    }
    return StatusBuilder::make(Status::INTERNAL_ERROR, "write failed, ec code: %d", ec);
}

Status CatalogState::write(const std::vector<proto::Build> &builds) {
    autil::ScopedWriteLock lock(_buildLock);

    proto::CatalogStateEntity state;
    for (const auto &iter : _catalogMap) {
        auto catalog = state.mutable_catalogs()->Add();
        catalog->CopyFrom(iter.second);
    }
    for (const auto &iter : builds) {
        auto build = state.mutable_builds()->Add();
        build->CopyFrom(iter);
    }

    std::string content;
    state.SerializeToString(&content);

    auto ec = _state->write(content);
    if (ec != WorkerState::EC_FAIL) {
        _builds = builds;
        return StatusBuilder::ok();
    }
    return StatusBuilder::make(Status::INTERNAL_ERROR, "write failed, ec code: %d", ec);
}

Status CatalogState::read(CatalogVersion version, proto::Catalog *proto) {
    autil::ScopedReadLock lock(_lock);
    if (_catalogMap.count(version) == 0) {
        return StatusBuilder::make(Status::NOT_FOUND, "not found version %ld", version);
    }

    proto->CopyFrom(_catalogMap[version]);
    return StatusBuilder::ok();
}

Status CatalogState::read(std::vector<proto::Build> *builds) {
    autil::ScopedReadLock lock(_buildLock);
    *builds = _builds;
    return StatusBuilder::ok();
}

Status CatalogState::getLatestVersion(CatalogVersion &version) {
    autil::ScopedReadLock lock(_lock);
    version = kInvalidCatalogVersion;
    for (const auto &iter : _catalogMap) {
        if (iter.first > version) {
            version = iter.first;
        }
    }

    return StatusBuilder::ok();
}

} // namespace catalog
