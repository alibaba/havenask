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

#include "autil/Lock.h"
#include "catalog/entity/CatalogSnapshot.h"
#include "catalog/util/StatusBuilder.h"

namespace worker_framework {
class WorkerState;
}

namespace catalog {

extern const std::string CATALOG_STATE_FILE;
extern const std::string BUILD_STATE_FILE;

class CatalogState {
public:
    CatalogState(std::unique_ptr<worker_framework::WorkerState> state);
    virtual ~CatalogState();

public:
    bool recover();
    Status write(CatalogSnapshot *newSnapshot);
    Status write(const std::vector<proto::Build> &builds);
    Status read(CatalogVersion version, proto::Catalog *proto);
    Status read(std::vector<proto::Build> *builds);
    Status getLatestVersion(CatalogVersion &version);

protected:
    mutable autil::ReadWriteLock _lock;
    std::map<CatalogVersion, proto::Catalog> _catalogMap;

    mutable autil::ReadWriteLock _buildLock;
    std::vector<proto::Build> _builds;

    std::unique_ptr<worker_framework::WorkerState> _state;
};

} // namespace catalog
