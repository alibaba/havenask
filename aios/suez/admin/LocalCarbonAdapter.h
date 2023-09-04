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

#include "suez/admin/CarbonAdapter.h"
#include "suez/admin/ClusterServiceImpl.h"

namespace network {
class HttpClient;
}

namespace suez {

class LocalCarbonAdapter : public CarbonAdapter {
public:
    LocalCarbonAdapter(const std::shared_ptr<ClusterManager> &clusterManager);
    virtual ~LocalCarbonAdapter() = default;

public:
    bool commitToCarbon(const AdminTarget &target, const JsonNodeRef::JsonMap *statusInfo) override;

private:
    bool sendHeartbeat(const ZoneTarget &target, const std::string &host);
    std::string calcSignature(const std::string &data);

private:
    std::shared_ptr<network::HttpClient> _httpClient;
    std::shared_ptr<ClusterManager> _clusterManager;
};

} // namespace suez
