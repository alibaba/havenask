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
#include "ha3/turing/searcher/SearcherServiceImpl.h"

#include <algorithm>
#include <iosfwd>
#include <map>
#include <vector>

#include "aios/network/arpc/arpc/RPCServer.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/http_arpc/HTTPRPCServer.h"
#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"
#include "autil/ThreadPoolManager.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/common/QueryInfo.h"
#include "ha3/monitor/Ha3BizMetrics.h"
#include "ha3/sql/framework/NaviInstance.h"
#include "ha3/turing/searcher/DefaultAggBiz.h"
#include "ha3/turing/searcher/DefaultSqlBiz.h"
#include "ha3/turing/searcher/SearcherServiceSnapshot.h"
#include "ha3/util/EnvParser.h"
#include "ha3/util/TypeDefine.h"
#include "kmonitor/client/MetricMacro.h"
#include "suez/sdk/RpcServer.h"
#include "suez/sdk/SuezError.h"
#include "suez/turing/common/PoolCache.h"
#include "suez/turing/common/WorkerParam.h"

namespace suez {
namespace turing {
class ServiceSnapshot;
} // namespace turing
} // namespace suez

using namespace std;
using namespace autil;
using namespace suez::turing;
using namespace suez;

using namespace isearch::monitor;
using namespace isearch::util;
using namespace isearch::common;
using namespace isearch::sql;

namespace isearch {
namespace turing {

AUTIL_LOG_SETUP(sql, SearcherServiceImpl);

SearcherServiceImpl::SearcherServiceImpl()
    : _enableSql(!autil::EnvUtil::getEnv<bool>("disableSql", false))
{
}

SearcherServiceImpl::~SearcherServiceImpl() {
}

bool SearcherServiceImpl::doInit() {
    const string &bizNamesStr = autil::EnvUtil::getEnv("basicTuringBizNames", "");
    if (!bizNamesStr.empty()) {
        const vector<string> &bizNames = StringUtil::split(bizNamesStr, ",");
        _basicTuringBizNames.insert(bizNames.begin(), bizNames.end());
    }
    _defaultAggStr = autil::EnvUtil::getEnv("defaultAgg", "");
    if (_defaultAggStr.empty()) {
        _defaultAggStr = string(HA3_DEFAULT_AGG_PREFIX) + "4";
    }
    if (!DefaultAggBiz::isSupportAgg(_defaultAggStr)) {
        AUTIL_LOG(INFO, "%s is not support agg by default.", _defaultAggStr.c_str());
        _defaultAggStr = "";
    }

    _paraWaysStr = autil::EnvUtil::getEnv("paraSearchWays", "");
    if (_paraWaysStr.empty()) {
        _paraWaysStr = "2,4";
        AUTIL_LOG(INFO, "paraSearchWays empty, set default 2,4");
    }
    vector<string> paraWaysVec;
    if (!EnvParser::parseParaWays(_paraWaysStr, paraWaysVec)) {
        AUTIL_LOG(WARN, "para search[%s] is not support by default.", _paraWaysStr.c_str());
        _paraWaysStr = "";
    }

    AUTIL_LOG(INFO, "paraSearchWays: [%s]", _paraWaysStr.c_str());
    string blockRpcStr = autil::EnvUtil::getEnv(WorkerParam::BLOCK_RPC_THREAD, "");
    if (blockRpcStr.empty()) {
        _workerParam.blockRpcThread = true;
        AUTIL_LOG(INFO, "set block rpc thread true.");
    }
    rewriteWorkerParam();
    return true;
}

void SearcherServiceImpl::rewriteWorkerParam() {
    _workerParam.useGig = true;
    AUTIL_LOG(INFO, "qrs worker param:\n%s", ToJsonString(_workerParam).c_str());
}

shared_ptr<ServiceSnapshotBase> SearcherServiceImpl::doCreateServiceSnapshot() {
    shared_ptr<ServiceSnapshot> snapshotPtr;
    SearcherServiceSnapshot *searcherSnap = new SearcherServiceSnapshot();
    searcherSnap->setBasicTuringBizNames(_basicTuringBizNames);
    searcherSnap->setDefaultAgg(_defaultAggStr);
    searcherSnap->setParaWays(_paraWaysStr);
    snapshotPtr.reset(searcherSnap);
    return snapshotPtr;
}

bool SearcherServiceImpl::doInitRpcServer(suez::RpcServer *rpcServer) {
    if (!rpcServer || !rpcServer->getArpcServer()) {
        AUTIL_LOG(ERROR, "rpcServer or arpcServer NULL");
        return false;
    }

    if (_enableSql) {
        auto gigRpcServer = rpcServer->gigRpcServer;
        assert(gigRpcServer != nullptr);
        if (!gigRpcServer->hasGrpc()) {
            AUTIL_LOG(ERROR, "sql searcher must have grpc port");
            return false;
        }
        if (!NaviInstance::get().initNavi(gigRpcServer, _workerMetricsReporter.get())) {
            AUTIL_LOG(ERROR, "init global navi failed");
            return false;
        }
    } else {
        AUTIL_LOG(INFO, "skip init global navi as disable sql");
    }

    http_arpc::HTTPRPCServer *httpRpc = rpcServer->httpRpcServer;
    map<string, string> aliasMap;
    aliasMap["/SearchService/cm2_status"] = "/GraphService/cm2_status";
    aliasMap["/SearchService/vip_status"] = "/GraphService/vip_status";
    aliasMap["/SearchService/getBizGraph"] = "/GraphService/getBizGraph";
    aliasMap["/SearchService/runGraph"] = "/GraphService/runGraph";
    aliasMap["/status_check"] = "/GraphService/cm2_status";
    aliasMap["/QrsService/cm2_status"] = "/GraphService/cm2_status";
    if (!httpRpc->addAlias(aliasMap)) {
        AUTIL_LOG(WARN, "alias old cm2 check path failed");
        return false;
    }
    return true;
}

void SearcherServiceImpl::beforeStopServiceSnapshot() {
    AUTIL_LOG(INFO, "before stop service snapshot");
    NaviInstance::get().stopSnapshot();
    SearchManagerAdapter::beforeStopServiceSnapshot();
}

void SearcherServiceImpl::stopWorker() {
    SearchManagerAdapter::stopWorker();
    NaviInstance::get().stopNavi();
}

std::unique_ptr<iquan::CatalogInfo> SearcherServiceImpl::getCatalogInfo() const {
    auto snapshot = getServiceSnapshot();
    if (snapshot) {
        return snapshot->getCatalogInfo();
    }
    return nullptr;
}

} // namespace sql
} // namespace isearch
