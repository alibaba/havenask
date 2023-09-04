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
#include "catalog/service/CatalogControllerManager.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, CatalogControllerManager);

static constexpr auto kDefaultRootName = "default";

CatalogControllerManager::CatalogControllerManager(const std::shared_ptr<IStore> &store) : _store(store) {}

std::vector<std::string> CatalogControllerManager::list() {
    autil::ScopedReadLock lock(_lock);
    std::vector<std::string> catalogNames;
    for (const auto &it : *(_controllers)) {
        catalogNames.emplace_back(it.first);
    }
    return catalogNames;
}

CatalogControllerPtr CatalogControllerManager::get(const std::string &catalogName) {
    autil::ScopedReadLock lock(_lock);
    auto it = _controllers->find(catalogName);
    if (it != _controllers->end()) {
        return it->second;
    }
    return nullptr;
}

Status CatalogControllerManager::tryAdd(const std::string &catalogName, CreateFunction function) {
    if (get(catalogName) != nullptr) { // cheap check
        return StatusBuilder::make(
            Status::DUPLICATE_ENTITY, "controller for catalog:[", catalogName, "] already exists");
    }
    autil::ScopedLock updateLock(_mutex);
    auto controllers = std::make_unique<ControllerMap>();
    { // check and copy
        autil::ScopedReadLock lock(_lock);
        auto it = _controllers->find(catalogName);
        if (it != _controllers->end()) {
            return StatusBuilder::make(
                Status::DUPLICATE_ENTITY, "controller for catalog:[", catalogName, "] already exists");
        }
        *controllers = *_controllers;
    }
    { // exec add
        auto catalogController = function();
        if (catalogController == nullptr) {
            return StatusBuilder::make(Status::OPERATION_FAILURE, "failed to create catalog:[", catalogName, "]");
        }
        controllers->emplace(catalogName, CatalogControllerPtr(catalogController.release()));
    }
    std::unique_ptr<Root> root;
    { // update root
        root = _root->clone();
        CATALOG_CHECK_OK(root->createCatalog(catalogName));
        writeRoot(*root);
    }
    { // swap
        autil::ScopedWriteLock lock(_lock);
        _controllers.swap(controllers);
    }
    _root = std::move(root);
    return StatusBuilder::ok();
}

Status CatalogControllerManager::tryRemove(const std::string &catalogName, RemoveFunction function) {
    if (get(catalogName) == nullptr) { // cheap check
        return StatusBuilder::make(Status::NOT_FOUND, "controller for catalog:[", catalogName, "] not exists");
    }
    autil::ScopedLock updateLock(_mutex);
    auto controllers = std::make_unique<ControllerMap>();
    { // check and copy
        autil::ScopedReadLock lock(_lock);
        auto it = _controllers->find(catalogName);
        if (it == _controllers->end()) {
            return StatusBuilder::make(Status::NOT_FOUND, "controller for catalog:[", catalogName, "] not exists");
        }
        *controllers = *_controllers;
    }
    { // exec remove
        auto it = controllers->find(catalogName);
        bool success = function(it->second);
        if (!success) {
            return StatusBuilder::make(Status::OPERATION_FAILURE, "failed to drop catalog:[", catalogName, "]");
        }
        controllers->erase(it);
    }
    std::unique_ptr<Root> root;
    { // update root
        root = _root->clone();
        CATALOG_CHECK_OK(root->dropCatalog(catalogName));
        writeRoot(*root);
    }
    { // swap
        autil::ScopedWriteLock lock(_lock);
        _controllers.swap(controllers);
    }
    _root = std::move(root);
    return StatusBuilder::ok();
}

bool CatalogControllerManager::recover() {
    autil::ScopedLock updateLock(_mutex);
    { // check
        autil::ScopedReadLock lock(_lock);
        if (_controllers != nullptr && !_controllers->empty()) {
            AUTIL_LOG(ERROR, "dirty CatalogControllerManager");
            return false;
        }
    }

    auto root = readRoot();
    if (root == nullptr) {
        return false;
    }
    auto controllers = std::make_unique<ControllerMap>();
    for (const auto &catalogName : root->catalogNames()) {
        auto controller = std::make_unique<CatalogController>(catalogName, _store);
        if (!controller->init()) {
            AUTIL_LOG(ERROR, "failed to init catalog:[%s]", catalogName.c_str());
            return false;
        }
        if (!controller->recover()) {
            AUTIL_LOG(ERROR, "failed to recover catalog:[%s]", catalogName.c_str());
            return false;
        }
        if (controller->status().code() == proto::EntityStatus::PENDING_DELETE) {
            // 极小概率下会出现dropCatalog操作完成了Catalog的状态持久化但还没来得及持久化Root的情况，需要在recover时redo
            AUTIL_LOG(WARN, "ignore PENDING_DELETE status catalog:[%s]", catalogName.c_str());
            continue;
        }
        controllers->emplace(catalogName, std::move(controller));
    }

    { // swap
        autil::ScopedWriteLock lock(_lock);
        _controllers.swap(controllers);
    }
    _root = std::move(root);
    return true;
}

std::unique_ptr<Root> CatalogControllerManager::readRoot() {
    proto::Root rootProto;
    if (!_store->readRoot(rootProto)) {
        AUTIL_LOG(ERROR, "failed to read rootProto");
        return nullptr;
    }
    auto root = std::make_unique<Root>();
    auto s = root->fromProto(rootProto);
    if (!isOk(s)) {
        AUTIL_LOG(ERROR,
                  "failed to convert rootProto with proto[%s], error:[%s]",
                  rootProto.DebugString().c_str(),
                  s.message().c_str());
        return nullptr;
    }
    return root;
}

Status CatalogControllerManager::writeRoot(const Root &root) {
    proto::Root rootProto;
    CATALOG_CHECK_OK(root.toProto(&rootProto));
    if (rootProto.root_name().empty()) {
        // 预留,暂时无用
        rootProto.set_root_name(kDefaultRootName);
    }
    if (!_store->writeRoot(rootProto)) {
        return StatusBuilder::make(Status::STORE_WRITE_ERROR, "serialize root failed");
    }
    return StatusBuilder::ok();
}

} // namespace catalog
