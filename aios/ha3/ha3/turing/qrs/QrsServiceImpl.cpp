#include <ha3/turing/qrs/QrsServiceImpl.h>
#include <autil/TimeUtility.h>
#include <ha3/turing/qrs/SearchTuringClosure.h>
#include <ha3/service/SessionWorkItem.h>
#include <ha3/worker/HaProtoJsonizer.h>
#include <ha3/common/XMLResultFormatter.h>
#include <ha3/monitor/QrsBizMetrics.h>
#include <ha3/common/ErrorResult.h>
#include <ha3/common/AccessLog.h>
#include <arpc/ANetRPCController.h>
#include <http_arpc/HTTPRPCControllerBase.h>
#include <sys/sysinfo.h>
#include <iquan_jni/api/IquanEnv.h>
#include <suez/common/CmdLineDefine.h>
#include <suez/turing/common/WorkerParam.h>
#include <multi_call/arpc/ArpcClosure.h>
#include <multi_call/arpc/CommonClosure.h>
using namespace std;
using namespace suez::turing;
using namespace suez;
using namespace autil;
using namespace arpc;
using namespace multi_call;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(worker);
USE_HA3_NAMESPACE(proto);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, QrsServiceImpl);

static const std::string HA_SEARCH_THREAD_POOL_NAME = "ha_search";

QrsServiceImpl::QrsServiceImpl()
    : _enableSql(true)
    , _threadPoolManagerInitFlag(false)
{
    const string &disableSql = WorkerParam::getEnv("disableSql", "");
    if (!disableSql.empty()) {
        _enableSql = !autil::StringUtil::fromString<bool>(disableSql);
    }
}

QrsServiceImpl::~QrsServiceImpl() {
}

bool QrsServiceImpl::doInit() {
    string bizNamesStr = sap::EnvironUtil::getEnv("basicTuringBizNames", "");
    if (!bizNamesStr.empty()) {
        const vector<string> &bizNames = StringUtil::split(bizNamesStr, ",");
        _basicTuringBizNames.insert(bizNames.begin(), bizNames.end());
    }
    if (_enableSql) {
        HA3_LOG(INFO, "jvm setup");
        auto status = iquan::IquanEnv::jvmSetup(iquan::jt_hdfs_jvm, {}, "");
        if (!status.ok()) {
            HA3_LOG(ERROR, "iquan jvm init failed, error msg: %s, will not start sql biz",
                    status.errorMessage().c_str());
            _enableSql = false;
        }
    }
    rewriteWorkerParam();
    if (!initThreadPoolManager()) {
        return false;
    }
    int32_t sessionPoolSize = _threadPoolManager->getTotalThreadNum() +
                              _threadPoolManager->getTotalQueueSize();
    _sessionPool.reset(new SessionPoolImpl<QrsArpcSession>(sessionPoolSize));
    _sqlSessionPool.reset(new SessionPoolImpl<QrsArpcSqlSession>(sessionPoolSize));
    if (!_turingUrlTransform.init()) {
        HA3_LOG(ERROR, "turing url transform init failed");
        return false;
    }
    return true;
}

void QrsServiceImpl::rewriteWorkerParam() {
    auto threadNumScaleFactor = WorkerParam::getEnv(WorkerParam::THREAD_NUMBER_SCALE_FACTOR);
    if (threadNumScaleFactor.empty()) {
        _workerParam.threadNumberScaleFactor = 20;
        sap::EnvironUtil::setEnv(WorkerParam::THREAD_NUMBER_SCALE_FACTOR, "20", true);
        HA3_LOG(INFO, "adjust thread number scale factor to 20");
    }
    auto ioThreadNumber = WorkerParam::getEnv(WorkerParam::IO_THREAD_NUM);
    if (ioThreadNumber.empty()) {
        _workerParam.ioThreadNumber = WorkerParam::DEFAULT_IO_THREAD_NUM;
        HA3_LOG(INFO, "adjust io thread number [%d]", _workerParam.ioThreadNumber);
    }
    _workerParam.useGig = true;
    HA3_LOG(INFO, "qrs worker param:\n%s", ToJsonString(_workerParam).c_str());
}

RpcContextPtr QrsServiceImpl::createRpcContext(const std::string &zoneName,
        RPCController *controller, const proto::QrsRequest *request)
{
    auto appGroup = HA_RESERVED_APPNAME + zoneName;
    const auto &amonitorPath = _workerParam.amonitorPath;

    return RpcContextUtil::qrsReceiveRpc(
            request->traceid(), request->rpcid(), request->userdata(),
            appGroup, amonitorPath, controller);
}

RpcContextPtr QrsServiceImpl::createRpcContext(const std::string &zoneName,
        RPCController *controller, const http_arpc::EagleInfo &eagleInfo)
{
    auto appGroup = HA_RESERVED_APPNAME + zoneName;
    const auto &amonitorPath = _workerParam.amonitorPath;
    return RpcContextUtil::qrsReceiveRpc(
            eagleInfo.traceid, eagleInfo.rpcid, eagleInfo.udata,
            appGroup, amonitorPath, controller);
}

void QrsServiceImpl::gigSearch(google::protobuf::RpcController *controller,
                               google::protobuf::Message *request,
                               google::protobuf::Message *response,
                               GigClosure *closure)
{
    auto requestTyped = dynamic_cast<proto::QrsRequest *>(request);
    auto responseTyped = dynamic_cast<proto::QrsResponse *>(response);
    if (!requestTyped || !responseTyped) {
        controller->SetFailed("invalid request or response");
        closure->Run();
    } else {
        if (_workerParam.grpcOnly) {
            requestTyped->set_protocoltype(PT_GRPC);
        }
        search(controller, requestTyped, responseTyped, closure);
    }
}

void QrsServiceImpl::search(RPCController *controller,
                            const proto::QrsRequest *request,
                            proto::QrsResponse *response, RPCClosure *done)
{
    if (!protocolCheck(controller, done)) {
        return;
    }
    QrsServiceSnapshotPtr snapshot =
        dynamic_pointer_cast<QrsServiceSnapshot>(getServiceSnapshot());
    if (!snapshot) {
        controller->SetFailed("service not ready");
        if (done) {
            done->Run();
        }
        return;
    }
    QrsArpcSession *arpcSession = constructQrsArpcSession(snapshot, controller,
            request, response, done);
     if (NULL != arpcSession) {
        SessionWorkItem *item = new SessionWorkItem(arpcSession);
        arpcSession->setPoolCache(_poolCache);
        arpcSession->setRunIdAllocator(_runIdAllocator);
        string taskQueueName = request->taskqueuename();
        if (taskQueueName.empty()) {
            taskQueueName = DEFAULT_TASK_QUEUE_NAME;
        }
        pushSearchItem(taskQueueName, item);
    } else {
        processFailedSession(snapshot, controller, request, response, done);
    }

}

void QrsServiceImpl::gigSearchSql(google::protobuf::RpcController *controller,
                                  google::protobuf::Message *request,
                                  google::protobuf::Message *response,
                                  GigClosure *closure)
{
    auto requestTyped = dynamic_cast<proto::QrsRequest *>(request);
    auto responseTyped = dynamic_cast<proto::QrsResponse *>(response);
    if (!requestTyped || !responseTyped) {
        controller->SetFailed("invalid request or response");
        closure->Run();
    } else {
        if (_workerParam.grpcOnly) {
            requestTyped->set_protocoltype(PT_GRPC);
        }
        searchSql(controller, requestTyped, responseTyped, closure);
    }
}

void QrsServiceImpl::searchTuring(RPCController *controller,const proto::QrsRequest *request,
                proto::QrsResponse *response, RPCClosure *done)
{
    const auto startTime = TimeUtility::currentTime();
    unique_ptr<common::AccessLog> pAccessLog(new common::AccessLog());
    pAccessLog->setStatusCode(ERROR_GENERAL);
    auto pRpcController = dynamic_cast<ANetRPCController*>(controller);
    pAccessLog->setIp(getClientIp(controller));
    const auto& query = request->assemblyquery();
    pAccessLog->setQueryString(query);

    auto pGraphRequest = _turingUrlTransform.transform(query);
    if (pGraphRequest == nullptr) {
        pAccessLog->setProcessTime((TimeUtility::currentTime() - startTime) / 1000);
        response->set_formattype(proto::FT_PROTOBUF);
        response->set_multicall_ec(MULTI_CALL_ERROR_RPC_FAILED);
        response->mutable_assemblyresult();
        done->Run();
        return;
    }

    pGraphRequest->set_traceid(request->traceid());
    pGraphRequest->set_rpcid(request->rpcid());
    pGraphRequest->set_userdata(request->userdata());
    if (pRpcController != nullptr) {
        pGraphRequest->set_timeout(pRpcController->GetExpireTime());
    }
    auto pGraphResponse = new GraphResponse();
    auto *pGigDone = dynamic_cast<GigClosure *>(done);
    assert(pGigDone);
    auto pClosure = new SearchTuringClosure(
            pAccessLog.release(),
            pGraphRequest,
            pGraphResponse,
            response,
            pGigDone,
            startTime);
    runGraph(controller, pGraphRequest, pGraphResponse, pClosure);
}

void QrsServiceImpl::searchSql(RPCController *controller,
                               const proto::QrsRequest *request,
                               proto::QrsResponse *response,
                               RPCClosure *done)
{
    if (!protocolCheck(controller, done)) {
        return;
    }
    QrsServiceSnapshotPtr snapshot =
        dynamic_pointer_cast<QrsServiceSnapshot>(getServiceSnapshot());
    if (!snapshot) {
        if (done) {
            done->Run();
        }
        return;
    }
    QrsArpcSqlSession *sqlSession = constructQrsArpcSqlSession(snapshot,
            controller, request, response, done);
    if (NULL != sqlSession) {
        SessionWorkItem *item = new SessionWorkItem(sqlSession);
        sqlSession->setPoolCache(_poolCache);
        string taskQueueName = request->taskqueuename();
        if (taskQueueName.empty()) {
            taskQueueName = DEFAULT_TASK_QUEUE_NAME;
        }
        pushSearchItem(taskQueueName, item);
    } else {
        processFailedSqlSession(snapshot, controller, request, response, done);
    }
}

void QrsServiceImpl::sqlClientInfo(RPCController *controller,
                                   const proto::SqlClientRequest *request,
                                   proto::SqlClientResponse *response,
                                   RPCClosure *done)
{
    response->set_multicall_ec(multi_call::MULTI_CALL_ERROR_RPC_FAILED);
    QrsServiceSnapshotPtr snapshot =
        dynamic_pointer_cast<QrsServiceSnapshot>(getServiceSnapshot());
    if (!snapshot) {
        if (done) {
            done->Run();
        }
        return;
    }
    string bizName = snapshot->getBizName(DEFAULT_SQL_BIZ_NAME);
    auto qrsSqlBiz = dynamic_pointer_cast<turing::QrsSqlBiz>(snapshot->getBiz(bizName));
    if (!qrsSqlBiz) {
        HA3_LOG(WARN, "sql biz [%s] not exist.", bizName.c_str());
        if (done) {
            done->Run();
        }
        return;
    }
    std::string result;
    iquan::Status status = qrsSqlBiz->dumpCatalog(result);
    if (!status.ok()) {
        iquan::StatusJsonWrapper statusWrapper;
        statusWrapper.status = status;
        response->set_assemblyresult(autil::legacy::ToJsonString(statusWrapper));
        HA3_LOG(WARN, "dump catalog failed, error msg: [%s].", status.errorMessage().c_str());
        if (done) {
            done->Run();
        }
        return;
    }
    response->set_assemblyresult(std::move(result));
    response->set_multicall_ec(multi_call::MULTI_CALL_ERROR_NONE);
    if (done) {
        done->Run();
    }
}

bool QrsServiceImpl::doInitRpcServer(suez::RpcServer *rpcServer) {
    if (!rpcServer || !rpcServer->getArpcServer()) {
        HA3_LOG(ERROR, "rpcServer or arpcServer NULL");
        return false;
    }
    multi_call::CompatibleFieldInfo info;
    info.requestInfoField = "gigRequestInfo";
    info.ecField = "multicall_ec";
    info.responseInfoField = "gigResponseInfo";

    HaProtoJsonizerPtr haprotoJsonizer(new HaProtoJsonizer());
    http_arpc::HTTPRPCServer* httpRpc = rpcServer->httpRpcServer;
    httpRpc->setProtoJsonizer(haprotoJsonizer);
    if (!rpcServer->RegisterService((dynamic_cast<QrsService *>(this)),
            info,
            arpc::ThreadPoolDescriptor()))
    {
        HA3_LOG(ERROR, "register service failed");
        return false;
    }

    if (!addHttpRpcMethod(httpRpc, "/QrsService/search", "/")) {
        return false;
    }
    string turingPrefix = sap::EnvironUtil::getEnv("basicTuringPrefix", "");
    if (turingPrefix.size() > 1u && turingPrefix[0] == '/') {
        if (!addHttpRpcMethod(httpRpc, "/QrsService/searchTuring", turingPrefix)) {
            return false;
        }
    }

    if (_enableSql && !addHttpRpcMethod(httpRpc, "/QrsService/searchSql", "/sql")) {
        return false;
    }
    if (_enableSql && !addHttpRpcMethod(httpRpc, "/QrsService/sqlClientInfo", "/sqlClientInfo")) {
        return false;
    }
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
    auto gigRpcServer = rpcServer->gigRpcServer;
    if (gigRpcServer->hasGrpc()) {
        gigRpcServer->registerGrpcService("/isearch.proto.QrsService/search",
            new QrsRequest(),
            new QrsResponse(),
            info,
            std::tr1::bind(&QrsServiceImpl::gigSearch,
                this,
                placeholders::_1,
                placeholders::_2,
                placeholders::_3,
                placeholders::_4));
        gigRpcServer->registerGrpcService("/isearch.proto.QrsService/searchSql",
            new QrsRequest(),
            new QrsResponse(),
            info,
            std::tr1::bind(&QrsServiceImpl::gigSearchSql,
                this,
                placeholders::_1,
                placeholders::_2,
                placeholders::_3,
                placeholders::_4));
    }
    return true;
}

bool QrsServiceImpl::addHttpRpcMethod(http_arpc::HTTPRPCServer* httpRpc,
                                      const string &searchMethod,
                                      const string &compatibleRtpMethod) {
    if (!httpRpc->hasMethod(searchMethod)) {
        HA3_LOG(WARN, "method [%s] not found", searchMethod.c_str());
        return false;
    }
    http_arpc::ServiceMethodPair serviceMethodPair = httpRpc->getMethod(searchMethod);
    RPCService *service = serviceMethodPair.first;
    RPCMethodDescriptor *method = serviceMethodPair.second;
    if (!service || !method) {
        HA3_LOG(WARN, "method descriptor [%s] not found", searchMethod.c_str());
        return false;
    } else {
        arpc::ThreadPoolDescriptor tpd(HA_SEARCH_THREAD_POOL_NAME, get_nprocs(), 10000);
        if(!httpRpc->addMethod(compatibleRtpMethod, serviceMethodPair, tpd)) {
            HA3_LOG(WARN, "add rpc method [%s] failed", compatibleRtpMethod.c_str());
            return false;
        }
    }
    return true;
}

void QrsServiceImpl::doReportMetrics() {
    if (!_threadPoolManagerInitFlag.load(memory_order_acquire)) {
        return;
    }
    std::map<std::string, size_t> activeThreadCountMap = _threadPoolManager->getActiveThreadCount();
    std::map<std::string, size_t> totalThreadCountMap = _threadPoolManager->getTotalThreadCount();
    for (auto&& thdPoolItem : activeThreadCountMap) {
        std::string activeThreadMetric = std::string("worker.") + thdPoolItem.first;
        REPORT_USER_MUTABLE_METRIC(_workerMetricsReporter, activeThreadMetric, thdPoolItem.second);

        auto it = totalThreadCountMap.find(thdPoolItem.first);
        if (it != totalThreadCountMap.end() && it->second > 0) {
            std::string activeRatioMetric = std::string("worker.ratio_") + thdPoolItem.first;
            REPORT_USER_MUTABLE_METRIC(
                    _workerMetricsReporter, activeRatioMetric,
                    thdPoolItem.second * 100.f / it->second);
        }
    }
}
multi_call::QuerySessionPtr QrsServiceImpl::constructQuerySession(
        const std::string& zoneName,
        const QrsRequest *request,
        RPCController *controller,
        RPCClosure *done,
        SessionSrcType &type)
{
    auto arpcClosure = dynamic_cast<multi_call::ArpcClosure *>(done);
    multi_call::QuerySessionPtr querySession;
    if (arpcClosure) {
        querySession = arpcClosure->getQuerySession();
        type = SESSION_SRC_ARPC;
    }
    if (querySession) {
        if (request->has_traceid() && querySession->isEmpytInitContext()) {
            auto rpcContext = createRpcContext(zoneName, controller, request);
            querySession->setRpcContext(rpcContext);
        }
    } else {
        service::RpcContextPtr rpcContext;
        auto commonClosure = dynamic_cast<multi_call::CommonClosure *>(done);
        if (commonClosure) {
            querySession = commonClosure->getQuerySession();
            auto closure = commonClosure->getClosure();
            auto httpClosure = dynamic_cast<http_arpc::HTTPRPCServerClosure *>(closure);
            if (httpClosure) {
                type = SESSION_SRC_HTTP;
                const http_arpc::EagleInfo &eagleInfo= httpClosure->getEagleInfo();
                if (!eagleInfo.isEmpty()) {
                    rpcContext = createRpcContext(zoneName, controller, eagleInfo);
                } else {
                    rpcContext = createRpcContext(zoneName, controller, request);
                }
            } else {
                rpcContext = createRpcContext(zoneName, controller, request);
            }
        } else {
            rpcContext = createRpcContext(zoneName, controller, request);
        }
        if (!querySession) {
            querySession.reset(new multi_call::QuerySession(_searchService));
        }
        querySession->setRpcContext(rpcContext);
    }
    return querySession;
}

#define INIT_ARPC_SESSION(                                              \
        session, controller, request, response, done, snapshot)         \
    session->setClientAddress(getClientIp(controller));                 \
    session->setStartTime();                                            \
    session->setRequest(request, response, done, snapshot);             \
    session->setUseGigSrc(_workerParam.useGigSrc);                      \
    SessionSrcType type = SESSION_SRC_UNKNOWN;                          \
    auto querySession = constructQuerySession(snapshot->getZoneName(), request, controller, done, type); \
    session->setGigQuerySession(querySession);                          \
    session->setSessionSrcType(type);

QrsArpcSession *QrsServiceImpl::constructQrsArpcSession(
        const QrsServiceSnapshotPtr &snapshot,
        RPCController *controller, const QrsRequest *request,
        QrsResponse *response, RPCClosure *done)
{
    QrsArpcSession *session = dynamic_cast<QrsArpcSession*>(_sessionPool->get());
    if (session == NULL) {
        return NULL;
    }
    INIT_ARPC_SESSION(session, controller, request, response, done, snapshot);
    return session;
}

QrsArpcSqlSession *QrsServiceImpl::constructQrsArpcSqlSession(
        const QrsServiceSnapshotPtr &snapshot,
        RPCController *controller, const QrsRequest *request,
        QrsResponse *response, RPCClosure *done)
{
    QrsArpcSqlSession *session = dynamic_cast<QrsArpcSqlSession*>(_sqlSessionPool->get());
    if (session == NULL) {
        return NULL;
    }
    INIT_ARPC_SESSION(session, controller, request, response, done, snapshot);
    return session;
}

void QrsServiceImpl::processFailedSession(
        const QrsServiceSnapshotPtr &snapshot,
        RPCController *controller, const QrsRequest *request,
        QrsResponse *response, RPCClosure *done)
{
    QrsArpcSession *failSession = new QrsArpcSession();
    INIT_ARPC_SESSION(failSession, controller, request, response, done, snapshot);
    string errMsg("Server is too busy, session count limit exceed");
    HA3_LOG(WARN, "%s", errMsg.c_str());

    ErrorResult errorResult(ERROR_QRS_SESSION_POOL_EXCEED, errMsg);
    errorResult.setHostName(getClientIp(controller));
    string errorXMLMsg = XMLResultFormatter::xmlFormatErrorResult(errorResult);
    failSession->setDropped();
    QrsSessionSearchResult result = QrsSessionSearchResult(errorXMLMsg);
    failSession->endQrsSession(result);
}

void QrsServiceImpl::processFailedSqlSession(
        const QrsServiceSnapshotPtr &snapshot,
        RPCController *controller, const QrsRequest *request,
        QrsResponse *response, RPCClosure *done)
{
    QrsArpcSqlSession *failSession = new QrsArpcSqlSession();
    INIT_ARPC_SESSION(failSession, controller, request, response, done, snapshot);
    string errMsg("Server is too busy, session count limit exceed");
    HA3_LOG(WARN, "%s", errMsg.c_str());
    ErrorResult errorResult(ERROR_QRS_SESSION_POOL_EXCEED, errMsg);
    errorResult.setHostName(getClientIp(controller));
    failSession->setDropped();
    QrsSessionSqlResult result;
    result.errorResult = errorResult;
    failSession->endQrsSession(result);
}

ServiceSnapshotPtr QrsServiceImpl::doCreateServiceSnapshot() {
    ServiceSnapshotPtr snapshotPtr;
    auto p = new QrsServiceSnapshot(_enableSql);
    p->setBasicTuringBizNames(_basicTuringBizNames);
    snapshotPtr.reset(p);
    return snapshotPtr;
}

bool QrsServiceImpl::initThreadPoolManager() {
    _threadPoolManager.reset(new ThreadPoolManager);
    int threadNum = _workerParam.threadNumber;
    if (_workerParam.threadNumber == sysconf(_SC_NPROCESSORS_ONLN)) {
        threadNum = std::ceil(_workerParam.threadNumber * _workerParam.threadNumberScaleFactor);
        if (threadNum > 400) {
            threadNum = 400;
        }
        HA3_LOG(INFO, "adjust thread number form [%d] to [%d]", _workerParam.threadNumber, threadNum);
    }
    if (!_threadPoolManager->addThreadPool(DEFAULT_TASK_QUEUE_NAME,
                    _workerParam.queueSize, threadNum))
    {
        HA3_LOG(ERROR, "addThreadPool failed, queue name [%s]",
                DEFAULT_TASK_QUEUE_NAME.c_str());
        return false;
    }

    string extraTaskQueueConfig = suez::turing::WorkerParam::getEnv(
            WorkerParam::EXTRA_TASK_QUEUE_CONFIG, "");

    if (!extraTaskQueueConfig.empty()
        && !_threadPoolManager->addThreadPool(extraTaskQueueConfig))
    {
        HA3_LOG(ERROR, "addThreadPool failed, extraTaskQueueConfig [%s]",
                extraTaskQueueConfig.c_str());
        return false;
    }
    if (!_threadPoolManager->start()) {
        HA3_LOG(ERROR, "start thread pool manager failed");
        return false;
    }
    _threadPoolManagerInitFlag.store(true, memory_order_release);
    return true;
}

void QrsServiceImpl::pushSearchItem(const string& taskQueueName, autil::WorkItem *item)
{
    ThreadPool* threadPool = _threadPoolManager->getThreadPool(taskQueueName);
    if (!threadPool) {
        threadPool = _threadPoolManager->getThreadPool(DEFAULT_TASK_QUEUE_NAME);
        if (!threadPool) {
            HA3_LOG(WARN, "pushWorkItem failed, not found taskQueue[%s]",
                    DEFAULT_TASK_QUEUE_NAME.c_str());
            if (item) {
                item->drop();
            }
        return;
        }
    }
    if (!item) {
        HA3_LOG(ERROR, "worker item is NULL");
        return;
    }
    if (threadPool->pushWorkItem(item, false) != ThreadPool::ERROR_NONE) {
        HA3_LOG(ERROR, "Failed to push item into threadPool");
        item->drop();
    }
}

service::RpcContextPtr QrsServiceImpl::qrsReceiveRpc(
            RPCController *controller,
            const proto::QrsRequest *request,
            const suez::turing::ServiceSnapshotPtr& snapshot)
{
    string zoneName = snapshot->getZoneName();
    auto appGroup = HA_RESERVED_APPNAME + zoneName;
    const auto &appid = _workerParam.amonitorPath;
    return RpcContextUtil::qrsReceiveRpc(
            request->traceid(),
            request->rpcid(),
            request->userdata(),
            appGroup,
            appid,
            controller);
}

std::string QrsServiceImpl::getClientIp(RPCController *controller) {
    auto gigController =
        dynamic_cast<multi_call::GigRpcController *>(controller);
    if (gigController) {
        auto protocolController = gigController->getProtocolController();
        auto anetRpcController =
            dynamic_cast<ANetRPCController *>(protocolController);
        if (anetRpcController) {
            return "arpc " + anetRpcController->GetClientAddress();
        } else {
            auto httpController =
                dynamic_cast<http_arpc::HTTPRPCControllerBase *>(
                    protocolController);
            if (httpController) {
                return "http " + httpController->GetAddr();
            }
        }
    }
    return "";
}

END_HA3_NAMESPACE(turing);
