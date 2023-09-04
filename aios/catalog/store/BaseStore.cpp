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
#include "catalog/store/BaseStore.h"

#include "catalog/store/CatalogReader.h"
#include "catalog/store/CatalogWriter.h"
#include "catalog/store/StoreFactory.h"
#include "worker_framework/CompressedWorkerState.h"
#include "worker_framework/PathUtil.h"

using namespace worker_framework;

namespace catalog {

AUTIL_DECLARE_AND_SETUP_LOGGER(catalog, BaseStore);

const std::string ROOT_STATE_FILE = "root.pb";

BaseStore::BaseStore() = default;

BaseStore::~BaseStore() = default;

bool BaseStore::init(const std::string &uri) {
    _uri = uri;
    std::string filePath;
    if (!uri.empty()) {
        filePath = PathUtil::joinPath(uri, ROOT_STATE_FILE);
    }
    _state = worker_base::CompressedWorkerState::createWithFilePath(filePath, autil::CompressType::LZ4);

    if (_state == nullptr) {
        return false;
    }

    std::string content;
    auto ec = _state->read(content);
    if (ec == WorkerState::EC_FAIL) {
        AUTIL_LOG(ERROR, "read from root state failed");
        return false;
    }

    if (ec == WorkerState::EC_UPDATE || ec == WorkerState::EC_OK) {
        _root.ParseFromString(content);
    }
    return true;
}

bool BaseStore::writeRoot(const proto::Root &root) {
    autil::ScopedWriteLock lock(_lock);

    if (root.version() > _rootVersion) {
        _rootVersion = root.version();
        _root.CopyFrom(root);
    }

    std::string content;
    root.SerializeToString(&content);
    auto ec = _state->write(content);
    if (ec != WorkerState::EC_FAIL) {
        return true;
    }
    AUTIL_LOG(ERROR, "write root [%s] to failed, ec: %d", content.c_str(), ec);
    return false;
}

bool BaseStore::readRoot(proto::Root &root) {
    autil::ScopedReadLock lock(_lock);

    std::string content;
    auto ec = _state->read(content);
    if (ec == WorkerState::EC_FAIL) {
        return false;
    }
    if (ec == WorkerState::EC_UPDATE || ec == WorkerState::EC_OK) {
        _root.ParseFromString(content);
    }

    root.CopyFrom(_root);
    return true;
}

std::unique_ptr<ICatalogReader> BaseStore::createReader(const std::string &catalogName) {
    auto state = getState(catalogName);
    if (state == nullptr) {
        return nullptr;
    }
    return std::make_unique<CatalogReader>(std::move(state));
}

std::unique_ptr<ICatalogWriter> BaseStore::createWriter(const std::string &catalogName) {
    auto state = getState(catalogName);
    if (state == nullptr) {
        return nullptr;
    }
    return std::make_unique<CatalogWriter>(std::move(state));
}

std::shared_ptr<CatalogState> BaseStore::getState(const std::string &catalogName) {
    autil::ScopedWriteLock lock(_lock);

    if (_stateMap.count(catalogName) == 0) {
        _stateMap.emplace(catalogName, createState(_uri, catalogName));
    }
    return _stateMap[catalogName];
}

std::shared_ptr<CatalogState> BaseStore::createState(const std::string &uri, const std::string &catalogName) {
    std::string filePath;
    if (!uri.empty()) {
        filePath = PathUtil::joinPath(uri, PathUtil::joinPath(catalogName, CATALOG_STATE_FILE));
    }
    auto state = worker_base::CompressedWorkerState::createWithFilePath(filePath, autil::CompressType::LZ4);

    if (state == nullptr) {
        AUTIL_LOG(
            ERROR, "create state with filePath: [%s], catalog [%s], failed", filePath.c_str(), catalogName.c_str());
        return nullptr;
    }

    auto catalogState = std::make_shared<CatalogState>(std::move(state));
    if (!catalogState->recover()) {
        return nullptr;
    }
    return catalogState;
}

REGISTER_STORE(BaseStore, "", []() { return std::make_unique<BaseStore>(); });

} // namespace catalog
