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

#include <memory>

#include "catalog/store/CatalogState.h"
#include "catalog/store/IStore.h"

namespace catalog {

extern const std::string ROOT_STATE_FILE;

class BaseStore : public IStore {
public:
    BaseStore();
    ~BaseStore();

public:
    bool init(const std::string &uri) override;
    bool writeRoot(const proto::Root &root) override;
    bool readRoot(proto::Root &root) override;

    std::unique_ptr<ICatalogReader> createReader(const std::string &catalogName) override;
    std::unique_ptr<ICatalogWriter> createWriter(const std::string &catalogName) override;

private:
    std::shared_ptr<CatalogState> getState(const std::string &catalogName);
    std::shared_ptr<CatalogState> createState(const std::string &uri, const std::string &catalogName);

protected:
    mutable autil::ReadWriteLock _lock;
    std::map<std::string, std::shared_ptr<CatalogState>> _stateMap;
    proto::Root _root;
    CatalogVersion _rootVersion = kInvalidCatalogVersion;
    std::string _uri;
    std::unique_ptr<worker_framework::WorkerState> _state;
};

} // namespace catalog
