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
#include "suez_navi/health_check/HealthCheckRpcR.h"
#include "navi/builder/GraphBuilder.h"
#include "suez_navi/health_check/HealthCheckKernel.h"
#include "suez_navi/health_check/BizCheckKernel.h"
#include "navi/rpc_server/ArpcRegistryParam.h"
#include "suez_navi/health_check/HealthCheck.pb.h"
#include "navi/ops/NullSourceKernel.h"

namespace suez_navi {

const std::string HealthCheckRpcR::RESOURCE_ID = "suez_navi.health_check_rpc_r";
const std::string HealthCheckRpcR::RPC_REQUEST_NAME = "suez_navi.status_request";

HealthCheckRpcR::HealthCheckRpcR() {
}

HealthCheckRpcR::~HealthCheckRpcR() {
}

void HealthCheckRpcR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_SNAPSHOT)
        .dependOn(navi::NaviRpcServerR::RESOURCE_ID, true,
                  BIND_RESOURCE_TO(_naviRpcServerR));
}

bool HealthCheckRpcR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "cm2_http_alias", _httpAliasVec, _httpAliasVec);
    NAVI_JSONIZE(ctx, "biz_part_info_map", _bizPartInfoMap);
    return true;
}

navi::ErrorCode HealthCheckRpcR::init(navi::ResourceInitContext &ctx) {
    navi::GraphDef graphDef;
    if (!buildGraph(graphDef)) {
        NAVI_KERNEL_LOG(ERROR, "build health check graph failed");
        return navi::EC_ABORT;
    }
    {
        navi::ArpcRegistryParam cm2Param;
        cm2Param.arpcParam.method = "cm2_status";
        cm2Param.arpcParam.httpAliasVec = _httpAliasVec;
        cm2Param.arpcParam.httpAliasVec.emplace_back("/GraphService/cm2_status");
        cm2Param.graphParam.requestDataName = RPC_REQUEST_NAME;
        cm2Param.graphParam.graph = graphDef;
        auto service = _naviRpcServerR->registerArpcService<health_check::HealthCheckService>(cm2Param);
        if (!service) {
            NAVI_KERNEL_LOG(ERROR,
                            "register cm2 health check arpc service failed");
            return navi::EC_ABORT;
        }
    }
    {
        navi::ArpcRegistryParam vipParam;
        vipParam.arpcParam.method = "vip_status";
        vipParam.arpcParam.httpAliasVec.emplace_back("/GraphService/vip_status");
        vipParam.graphParam.requestDataName = RPC_REQUEST_NAME;
        vipParam.graphParam.graph = graphDef;
        auto service = _naviRpcServerR->registerArpcService<health_check::HealthCheckService>(vipParam);
        if (!service) {
            NAVI_KERNEL_LOG(ERROR,
                            "register vip health check arpc service failed");
            return navi::EC_ABORT;
        }
    }
    return navi::EC_NONE;
}

bool HealthCheckRpcR::buildGraph(navi::GraphDef &graphDef) {
    navi::GraphBuilder builder(&graphDef);
    navi::P prevPort;
    if (_bizPartInfoMap.empty()) {
        NAVI_KERNEL_LOG(ERROR, "biz part info map empty");
        return false;
    }
    for (const auto &pair : _bizPartInfoMap) {
        const auto &bizName = pair.first;
        auto partCount = pair.second.partCount;
        const auto &partIds = pair.second.partIds;
        if (partCount < 0) {
            NAVI_KERNEL_LOG(ERROR, "invalid part count [%d], biz [%s]", partCount, bizName.c_str());
            return false;
        }
        if (partIds.empty()) {
            NAVI_KERNEL_LOG(ERROR, "empty part id list, part count [%d], biz [%s]", partCount, bizName.c_str());
            return false;
        }
        for (auto partId : partIds) {
            if (partId < 0) {
                NAVI_KERNEL_LOG(
                    ERROR, "invalid part id [%d], part count [%d], biz [%s]", partId, partCount, bizName.c_str());
                return false;
            }
            auto graphId = builder.newSubGraph(bizName);
            builder.subGraph(graphId)
                .location(bizName, partCount, partId);
            if (!prevPort.isValid()) {
                prevPort = builder.node("source")
                               .kernel(navi::NullSourceKernel::KERNEL_ID)
                               .out(navi::NullSourceKernel::OUTPUT_PORT);
            }
            auto nodeName = bizName + "_" + std::to_string(partCount) + "_" + std::to_string(partId);
            auto node = builder.node(nodeName).kernel(BizCheckKernel::KERNEL_ID);
            node.in(BizCheckKernel::INPUT_PORT).from(prevPort);
            prevPort = node.out(BizCheckKernel::OUTPUT_PORT);
        }
    }
    if (!prevPort.isValid()) {
        NAVI_KERNEL_LOG(ERROR, "build health check graph failed, invalid port");
        return false;
    }
    auto sink = builder.node("sink").kernel(HealthCheckKernel::KERNEL_ID);
    sink.in(HealthCheckKernel::INPUT_PORT).from(prevPort);
    sink.out(HealthCheckKernel::OUTPUT_PORT).asGraphOutput("out");
    return true;
}

REGISTER_RESOURCE(HealthCheckRpcR);

}
