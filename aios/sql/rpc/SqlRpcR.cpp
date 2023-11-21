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
#include "sql/rpc/SqlRpcR.h"

#include <algorithm>
#include <memory>
#include <vector>

#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "ha3/proto/QrsService.pb.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/builder/GraphDesc.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/rpc_server/ArpcRegistryParam.h"
#include "navi/rpc_server/NaviRpcServerR.h"
#include "sql/rpc/SqlClientInfoKernel.h"
#include "sql/rpc/SqlFormatKernel.h"
#include "sql/rpc/SqlProtoJsonizer.h"
#include "sql/rpc/SqlRpcKernel.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string SqlRpcR::RESOURCE_ID = "sql.sql_rpc.r";
const std::string SqlRpcR::SQL_REQUEST_NAME = "sql.sql_request";

SqlRpcR::SqlRpcR() {}

SqlRpcR::~SqlRpcR() {}

void SqlRpcR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_SNAPSHOT);
}

bool SqlRpcR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode SqlRpcR::init(navi::ResourceInitContext &ctx) {
    if (!registerSql()) {
        return navi::EC_INIT_RESOURCE;
    }
    if (!registerClientInfo()) {
        return navi::EC_INIT_RESOURCE;
    }
    return navi::EC_NONE;
}

bool SqlRpcR::registerSql() {
    navi::ArpcRegistryParam param;
    param.arpcParam.method = "searchSql";
    param.arpcParam.httpAliasVec.push_back("/sql");
    param.arpcParam.httpAliasVec.push_back("/QrsService/searchSql");
    param.arpcParam.protoJsonizer.reset(new SqlProtoJsonizer());
    param.graphParam.requestDataName = SQL_REQUEST_NAME;
    if (!buildRunSqlGraph(&param.graphParam.graph)) {
        return false;
    }
    auto service = _naviRpcServerR->registerArpcService<isearch::proto::QrsService>(param);
    if (!service) {
        NAVI_KERNEL_LOG(ERROR, "register /searchSql arpc service failed");
        return false;
    }
    return true;
}

bool SqlRpcR::registerClientInfo() {
    navi::ArpcRegistryParam param;
    param.arpcParam.method = "sqlClientInfo";
    param.arpcParam.httpAliasVec.push_back("/sqlClientInfo");
    param.arpcParam.httpAliasVec.push_back("/QrsService/sqlClientInfo");
    param.arpcParam.protoJsonizer.reset(new SqlProtoJsonizer());
    param.graphParam.requestDataName = SQL_REQUEST_NAME;

    navi::GraphBuilder builder(&param.graphParam.graph);
    builder.newSubGraph("");
    builder.node("sql_client_info")
        .kernel(SqlClientInfoKernel::KERNEL_ID)
        .out(SqlClientInfoKernel::OUTPUT_PORT)
        .asGraphOutput("sql_client_info");

    auto service = _naviRpcServerR->registerArpcService<isearch::proto::QrsService>(param);
    if (!service) {
        NAVI_KERNEL_LOG(ERROR, "register /sqlClientInfo arpc service failed");
        return false;
    }
    return true;
}

bool SqlRpcR::buildRunSqlGraph(navi::GraphDef *graphDef) const {
    navi::GraphBuilder builder(graphDef);
    auto graphId = builder.newSubGraph("");
    auto rpc = builder.subGraph(graphId).node("rpc").kernel(SqlRpcKernel::KERNEL_ID);
    auto parse = builder.node("sql_parse").kernel("sql.SqlParseKernel");
    auto planTransform = builder.node("plan_transform").kernel("sql.PlanTransformKernel");
    auto runGraph
        = builder.node("run_sql_graph").kernel("sql.RunSqlGraphKernel").integerAttr("one_batch", 1);
    auto rpcFormat = builder.getScopeTerminator().kernel(SqlFormatKernel::KERNEL_ID);

    auto rpcOut = rpc.out(SqlRpcKernel::OUTPUT_PORT);
    auto formatOut = rpc.out(SqlRpcKernel::OUTPUT_PORT2);
    parse.in("input0").from(rpcOut);
    planTransform.in("input0").from(parse.out("output0"));
    planTransform.in("input1").from(rpcOut);
    runGraph.in("input0").from(planTransform.out("output0"));
    runGraph.in("input1").from(planTransform.out("output1"));
    rpcFormat.in(navi::SCOPE_TERMINATOR_INPUT_PORT).autoNext().from(rpcOut);
    rpcFormat.in(navi::SCOPE_TERMINATOR_INPUT_PORT).autoNext().from(runGraph.out("output0"));
    rpcFormat.in(navi::SCOPE_TERMINATOR_INPUT_PORT).autoNext().from(parse.out("output0"));
    rpcFormat.in(navi::SCOPE_TERMINATOR_INPUT_PORT).autoNext().from(runGraph.out("output1"));
    rpcFormat.in(navi::SCOPE_TERMINATOR_INPUT_PORT).autoNext().from(formatOut);
    rpcFormat.out(SqlFormatKernel::OUTPUT_PORT).asGraphOutput("rpc_result");

    if (builder.ok()) {
        NAVI_KERNEL_LOG(DEBUG, "graph def: %s", graphDef->ShortDebugString().c_str());
        return true;
    } else {
        return false;
    }
}

REGISTER_RESOURCE(SqlRpcR);

} // namespace sql
