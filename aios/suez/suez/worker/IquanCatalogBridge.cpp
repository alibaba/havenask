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
#include "suez/worker/IquanCatalogBridge.h"

#include "aios/network/arpc/arpc/ANetRPCChannelManager.h"
#include "autil/Log.h"

#ifndef AIOS_OPEN_SOURCE
#include "catalog/client/CatalogArpcClient.h"
#include "catalog/client/CatalogClient.h"
#include "catalog/client/SimpleAddressResolver.h"
#include "catalog/proto/Catalog.pb.h"
#include "catalog/proto/CatalogBuilder.h"
#include "iquan/common/catalog/IquanCatalogConvertor.h"
#endif

#include "iquan/common/catalog/CatalogInfo.h"
#include "suez/heartbeat/HeartbeatTarget.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, IquanCatalogBridge);

IquanCatalogBridge::IquanCatalogBridge() : _stopped(false) {}

IquanCatalogBridge::~IquanCatalogBridge() {
    stop();
#ifndef AIOS_OPEN_SOURCE
    _catalogClient.reset();
    _rpcChannelManager.reset();
#endif
}

bool IquanCatalogBridge::init() {
#ifndef AIOS_OPEN_SOURCE
    _rpcChannelManager = std::make_shared<arpc::ANetRPCChannelManager>();
    if (!_rpcChannelManager->StartPrivateTransport("catalog")) {
        AUTIL_LOG(ERROR, "start rpc channel manager for catalog failed");
        return false;
    }
    _addressResolver = std::make_shared<catalog::SimpleAddressResolver>();
    _catalogClient = std::make_shared<catalog::CatalogArpcClient>(_rpcChannelManager, _addressResolver);
#endif
    return true;
}

void IquanCatalogBridge::stop() {
    bool expect = false;
    _stopped.compare_exchange_weak(expect, true);
#ifndef AIOS_OPEN_SOURCE
    _catalogClient->stop();
    _rpcChannelManager->StopPrivateTransport();
#endif
}

bool IquanCatalogBridge::stopped() { return _stopped.load(std::memory_order_relaxed); }

bool IquanCatalogBridge::commitCatalogInfo(int64_t versionId, iquan::CatalogInfo &catalogInfo) {
    if (unlikely(stopped())) {
        AUTIL_LOG(WARN, "commit catalog info failed, iquan catalog bridge has stopped.");
        return false;
    }

    catalogInfo.version = versionId;
    if (catalogInfo.version <= 0) {
        AUTIL_LOG(ERROR, "invalid catalog info, version must be > 0, actual: %ld", catalogInfo.version);
        return false;
    }

    if (catalogInfo.version == _lastCommitVersion) {
        return true;
    }

#ifndef AIOS_OPEN_SOURCE
    catalog::UpdateCatalogRequest request;
    if (!iquan::IquanCatalogConvertor::convertJsonToPb(catalogInfo, *request.mutable_catalog())) {
        AUTIL_LOG(ERROR, "convert catalog info %s to request failed", ToJsonString(catalogInfo).c_str());
        return false;
    }

    AUTIL_LOG(DEBUG,
              "commit catalog info [%s], catalog proto[%s]",
              autil::legacy::ToJsonString(catalogInfo).c_str(),
              request.catalog().ShortDebugString().c_str());
    catalog::CommonResponse response;
    _catalogClient->updateCatalog(&request, &response);
    if (catalog::Status::OK != response.status().code()) {
        AUTIL_LOG(ERROR, "commit to catalog service failed, error: %s", response.status().ShortDebugString().c_str());
        return false;
    }
#endif
    _lastCommitVersion = catalogInfo.version;
    return true;
}

void IquanCatalogBridge::updateCatalogAddress(const std::string &address) {
#ifndef AIOS_OPEN_SOURCE
    _addressResolver->setAddress(address);
#endif
}

catalog::CatalogClient *IquanCatalogBridge::getCatalogClient() const { return _catalogClient.get(); }

} // namespace suez
