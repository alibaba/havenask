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
#include "iquan/common/catalog/IquanCatalogClient.h"
#include "autil/DailyRunMode.h"

#ifndef AIOS_OPEN_SOURCE
#include "catalog/client/CatalogClient.h"
#include "iquan/common/catalog/IquanCatalogConvertor.h"
#endif

using namespace autil;

namespace iquan {

AUTIL_LOG_SETUP(iquan, IquanCatalogClient);

IquanCatalogClient::IquanCatalogClient(catalog::CatalogClient *catalogClient) : _catalogClient(catalogClient) {}

IquanCatalogClient::~IquanCatalogClient() {
    if (_workThread) {
        _workThread->stop();
        _workThread.reset();
    }
}

void IquanCatalogClient::start() {
    if (_workThread) {
        _workThread->stop();
    }
    int64_t loopTime = 1000000;
    if (!autil::DailyRunMode::enable()) {
        loopTime = 5000;
    }
    _workThread = LoopThread::createLoopThread(
        std::bind(&IquanCatalogClient::workloop, this), loopTime, "IquanCatalogWork");
}

void IquanCatalogClient::subscribeCatalog(const std::string &dbName, SubscribeHandler func) {
    autil::ScopedWriteLock scope(_lock);
    _subscribeHandlerMap[dbName] = func;
}

void IquanCatalogClient::unSubscribeCatalog(const std::string &dbName, SubscribeHandler func) {
    autil::ScopedWriteLock scope(_lock);
    _subscribeHandlerMap.erase(dbName);
}

void IquanCatalogClient::workloop() {

#ifdef AIOS_OPEN_SOURCE
    return;
#else
    if (!_catalogClient || _catalogClient->stopped()) {
        AUTIL_LOG(WARN, "catalog client has not exist or stop.");
        return;
    }

    catalog::GetCatalogRequest request;
    catalog::GetCatalogResponse response;
    request.set_version(0);
    _catalogClient->getCatalog(&request, &response);
    if (response.status().code() != catalog::Status::OK) {
        AUTIL_LOG(WARN, "catalog client get catalog error, response[%s].", response.ShortDebugString().c_str());
        return;
    }

    AUTIL_LOG(DEBUG, "getCatalog from catalog client, response[%s]", response.ShortDebugString().c_str());
    if (response.catalog().version() != _catalogInfo.version) {
        CatalogInfo catalogInfo;
        auto ret = IquanCatalogConvertor::convertPbToJson(response.catalog(), catalogInfo);
        if (!ret) {
            AUTIL_LOG(WARN, "catalog info convert pb2json failed, response[%s]", response.ShortDebugString().c_str());
            return;
        }

        // TODO update by subscribe db name
        {
            autil::ScopedReadLock scope(_lock);
            if (_subscribeHandlerMap.empty()) {
                AUTIL_LOG(DEBUG, "iquan subscribe map is empty");
                return;
            }

            for (const auto &it : _subscribeHandlerMap) {
                AUTIL_LOG(DEBUG, "notify iquan update catalog[%s]", autil::legacy::ToJsonString(catalogInfo).c_str());
                auto ret = it.second(catalogInfo);
                if (!ret) {
                    AUTIL_LOG(WARN, "db[%s] update catalog info faild.", it.first.c_str());
                    return;
                }
            }
        }
        _catalogInfo = std::move(catalogInfo);
        AUTIL_LOG(INFO, "update catalog info succ.");
    } else {
        AUTIL_LOG(TRACE3, "update catalog info skip, version is same, version[%ld]", _catalogInfo.version);
    }
#endif
}

} // namespace iquan
