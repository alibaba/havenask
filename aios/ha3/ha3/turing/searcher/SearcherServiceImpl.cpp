#include <ha3/turing/searcher/SearcherServiceImpl.h>
#include <ha3/turing/searcher/SearcherServiceSnapshot.h>
#include <ha3/turing/searcher/SearcherContext.h>
#include <ha3/turing/searcher/BasicSearcherContext.h>
#include <ha3/monitor/SearcherBizMetrics.h>
#include <ha3/turing/searcher/DefaultAggBiz.h>
#include <suez/common/CmdLineDefine.h>
#include <suez/turing/common/WorkerParam.h>
#include <ha3/util/EnvParser.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
using namespace suez;

USE_HA3_NAMESPACE(monitor);
BEGIN_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(common);
HA3_LOG_SETUP(turing, SearcherServiceImpl);

SearcherServiceImpl::SearcherServiceImpl() {
}

SearcherServiceImpl::~SearcherServiceImpl() {
}

bool SearcherServiceImpl::doInit() {
    const string &bizNamesStr = sap::EnvironUtil::getEnv("basicTuringBizNames", "");
    if (!bizNamesStr.empty()) {
        const vector<string> &bizNames = StringUtil::split(bizNamesStr, ",");
        _basicTuringBizNames.insert(bizNames.begin(), bizNames.end());
    }
    _defaultAggStr = sap::EnvironUtil::getEnv("defaultAgg", "");
    if (_defaultAggStr.empty()) {
        _defaultAggStr = HA3_DEFAULT_AGG_PREFIX + "4";
    }
    if (!DefaultAggBiz::isSupportAgg(_defaultAggStr)) {
        HA3_LOG(INFO, "%s is not support agg by default.", _defaultAggStr.c_str());
        _defaultAggStr = "";
    }

    _paraWaysStr = sap::EnvironUtil::getEnv("paraSearchWays", "");
    if (_paraWaysStr.empty()) {
        _paraWaysStr = "2,4";
        HA3_LOG(INFO, "paraSearchWays empty, set default 2,4");
    }
    vector<string> paraWaysVec;
    if (!EnvParser::parseParaWays(_paraWaysStr, paraWaysVec)) {
        HA3_LOG(WARN, "para search[%s] is not support by default.",
                _paraWaysStr.c_str());
        _paraWaysStr = "";
    }

    HA3_LOG(INFO, "paraSearchWays: [%s]", _paraWaysStr.c_str());
    string blockRpcStr = sap::EnvironUtil::getEnv(WorkerParam::BLOCK_RPC_THREAD, "");
    if (blockRpcStr.empty()) {
        _workerParam.blockRpcThread = true;
        HA3_LOG(INFO, "set block rpc thread true.");
    }
    return true;
}

SearchContext *SearcherServiceImpl::doCreateContext(
        const SearchContextArgs &args,
        const GraphRequest *request,
        GraphResponse *response)
{
    string bizName = request->bizname();
    if (_basicTuringBizNames.count(bizName) == 1) {
        return GraphServiceImpl::doCreateContext(args, request, response);
    } else if (StringUtil::endsWith(bizName, HA3_DEFAULT_AGG)) {
        return new BasicSearcherContext(args, request, response);
    } else if (StringUtil::endsWith(bizName, HA3_DEFAULT_SQL)) {
        return new GraphSearchContext(args, request, response);
    } else {
        return new SearcherContext(args, request, response);
    }
}

shared_ptr<ServiceSnapshot> SearcherServiceImpl::doCreateServiceSnapshot() {
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
        HA3_LOG(ERROR, "rpcServer or arpcServer NULL");
        return false;
    }
    http_arpc::HTTPRPCServer* httpRpc = rpcServer->httpRpcServer;
    map<string, string> aliasMap;
    aliasMap["/SearchService/cm2_status"] = "/GraphService/cm2_status";
    aliasMap["/SearchService/vip_status"] = "/GraphService/vip_status";
    aliasMap["/SearchService/getBizGraph"] = "/GraphService/getBizGraph";
    aliasMap["/SearchService/runGraph"] = "/GraphService/runGraph";
    aliasMap["/status_check"]= "/GraphService/cm2_status";
    aliasMap["/QrsService/cm2_status"]= "/GraphService/cm2_status";
    if (!httpRpc->addAlias(aliasMap)) {
        HA3_LOG(WARN, "alias old cm2 check path failed");
        return false;
    }
    return true;
}

END_HA3_NAMESPACE(turing);
