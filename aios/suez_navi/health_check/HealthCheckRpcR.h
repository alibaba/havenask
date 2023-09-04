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

#include "navi/engine/Resource.h"
#include "navi/rpc_server/NaviRpcServerR.h"
#include "autil/legacy/jsonizable.h"

namespace suez_navi {

class BizPartInfo : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("part_count", partCount);
        json.Jsonize("part_ids", partIds);
    }
public:
    navi::NaviPartId partCount = navi::INVALID_NAVI_PART_COUNT;
    std::set<navi::NaviPartId> partIds;
};

class HealthCheckRpcR : public navi::Resource
{
public:
    HealthCheckRpcR();
    ~HealthCheckRpcR();
    HealthCheckRpcR(const HealthCheckRpcR &) = delete;
    HealthCheckRpcR &operator=(const HealthCheckRpcR &) = delete;
public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
public:
    static const std::string RESOURCE_ID;
    static const std::string RPC_REQUEST_NAME;
private:
    bool buildGraph(navi::GraphDef &graphDef);
private:
    navi::NaviRpcServerR *_naviRpcServerR = nullptr;
    std::vector<google::protobuf::Service *> _serviceVec;
    std::vector<std::string> _httpAliasVec;
    std::map<std::string, BizPartInfo> _bizPartInfoMap;
};

NAVI_TYPEDEF_PTR(HealthCheckRpcR);

}

