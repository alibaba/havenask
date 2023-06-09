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

#include <functional>
#include <string>
#include <unordered_map>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "iquan/common/catalog/CatalogInfo.h"

namespace catalog {
class CatalogClient;
} // namespace catalog

namespace iquan {

class IquanCatalogClient {
public:
    using SubscribeHandler = std::function<bool(const iquan::CatalogInfo &)>;
    using SubscribeHandlerMap = std::unordered_map<std::string, SubscribeHandler>;

public:
    IquanCatalogClient(catalog::CatalogClient *catalogClient);
    ~IquanCatalogClient();

public:
    void start();
    void workloop();
    void subscribeCatalog(const std::string &dbName, SubscribeHandler fuync);
    void unSubscribeCatalog(const std::string &dbName, SubscribeHandler func);

private:
    autil::LoopThreadPtr _workThread;
    catalog::CatalogClient *_catalogClient;
    CatalogInfo _catalogInfo;
    mutable autil::ReadWriteLock _lock;
    SubscribeHandlerMap _subscribeHandlerMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace iquan
