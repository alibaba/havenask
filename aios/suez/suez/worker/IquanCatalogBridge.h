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

#include <atomic>
#include <memory>
#include <string>

namespace arpc {
class ANetRPCChannelManager;
}

namespace catalog {
class SimpleAddressResolver;
class CatalogClient;
class CreateCatalogRequest;
} // namespace catalog

namespace iquan {
class CatalogInfo;
}

namespace suez {

class HeartbeatTarget;

class IquanCatalogBridge {
public:
    IquanCatalogBridge();
    ~IquanCatalogBridge();

public:
    bool init();
    void stop();
    bool stopped();

    bool commitCatalogInfo(int64_t versionId, iquan::CatalogInfo &catalogInfo);

    void updateCatalogAddress(const std::string &address);
    catalog::CatalogClient *getCatalogClient() const;

private:
    std::shared_ptr<arpc::ANetRPCChannelManager> _rpcChannelManager;
    std::shared_ptr<catalog::SimpleAddressResolver> _addressResolver;
    std::shared_ptr<catalog::CatalogClient> _catalogClient;
    int64_t _lastCommitVersion = -1;
    std::atomic<bool> _stopped;
};

} // namespace suez
