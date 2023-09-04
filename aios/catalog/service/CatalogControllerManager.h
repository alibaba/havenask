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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "catalog/entity/Root.h"
#include "catalog/service/CatalogController.h"
#include "catalog/store/IStore.h"

namespace catalog {

class CatalogControllerManager {
public:
    using CreateFunction = std::function<std::unique_ptr<CatalogController>()>;
    using RemoveFunction = std::function<bool(CatalogControllerPtr)>;

public:
    CatalogControllerManager(const std::shared_ptr<IStore> &store);
    ~CatalogControllerManager() = default;
    CatalogControllerManager(const CatalogControllerManager &) = delete;
    CatalogControllerManager &operator=(const CatalogControllerManager &) = delete;

public:
    std::vector<std::string> list();
    CatalogControllerPtr get(const std::string &catalogName);
    Status tryAdd(const std::string &catalogName, CreateFunction function);
    Status tryRemove(const std::string &catalogName, RemoveFunction function);
    bool recover();
    std::unique_ptr<Root> readRoot();
    Status writeRoot(const Root &root);

private:
    std::shared_ptr<IStore> _store;
    /**
     * 保证catalog的增删只有一个线程操作(现实中也是极低频的)的同时，避免阻塞其他catalog的读操作
     * 如需突破增删操作的并发度，可以考虑在引入细粒度锁
     */
    autil::ThreadMutex _mutex;
    std::unique_ptr<Root> _root;
    using ControllerMap = std::map<std::string, CatalogControllerPtr>;
    autil::ReadWriteLock _lock;
    std::unique_ptr<ControllerMap> _controllers AUTIL_GUARDED_BY(_lock);
    AUTIL_LOG_DECLARE();
};

} // namespace catalog
