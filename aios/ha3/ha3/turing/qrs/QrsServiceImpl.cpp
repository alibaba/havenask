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
#include "ha3/turing/qrs/QrsServiceImpl.h"

#include <assert.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <algorithm>
#include <functional>
#include <iosfwd>
#include <map>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/RPCServer.h"
#include "autil/CircularQueue.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "autil/WorkItem.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/common/AccessLog.h" // IWYU pragma: keep
#include "ha3/common/CommonDef.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/XMLResultFormatter.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/service/QrsArpcSession.h"
#include "ha3/service/QrsArpcSqlSession.h"
#include "ha3/service/QrsSessionSearchRequest.h"
#include "ha3/service/QrsSessionSearchResult.h"
#include "ha3/service/Session.h"
#include "ha3/service/SessionWorkItem.h"
#include "ha3/service/TraceSpanUtil.h"
#include "ha3/sql/framework/ErrorResult.h"
#include "ha3/sql/framework/QrsSessionSqlResult.h"
#include "ha3/sql/framework/NaviInstance.h"
#include "ha3/turing/qrs/SearchTuringClosure.h"
#include "ha3/turing/qrs/LocalServiceSnapshot.h"
#include "ha3/worker/HaProtoJsonizer.h"
#include "aios/network/http_arpc/HTTPRPCControllerBase.h"
#include "aios/network/http_arpc/HTTPRPCServer.h"
#include "aios/network/http_arpc/HTTPRPCServerClosure.h"
#include "iquan/common/Status.h"
#include "iquan/jni/Iquan.h"
#include "iquan/jni/IquanEnv.h"
#include "iquan/jni/JvmType.h"
#include "kmonitor/client/MetricMacro.h"
#include "aios/network/gig/multi_call/arpc/ArpcClosure.h"
#include "aios/network/gig/multi_call/arpc/CommonClosure.h"
#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "aios/network/gig/multi_call/rpc/GigRpcController.h"
#include "aios/network/gig/multi_call/rpc/GigRpcServer.h"
#include "suez/turing/common/PoolCache.h"
#include "suez/turing/common/WorkerParam.h"
#include "suez/turing/proto/ErrorCode.pb.h"
#include "suez/turing/proto/Search.pb.h"
#include "suez/turing/search/GraphServiceImpl.h"
#include "suez/turing/search/SearchContext.h"
#include "suez/sdk/RpcServer.h"
#include "sys/sysinfo.h"

using namespace std;
using namespace suez::turing;
using namespace suez;
using namespace autil;
using namespace arpc;
using namespace multi_call;
using namespace isearch::common;
using namespace isearch::service;
using namespace isearch::worker;
using namespace isearch::proto;
using namespace isearch::sql;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, QrsServiceImpl);

static const std::string HA_SEARCH_THREAD_POOL_NAME = "ha3_http_arpc";
static int32_t DEFAULT_MAX_THREAD_NUM = 400;
QrsServiceImpl::QrsServiceImpl()
    : _enableSql(true)
    , _enableLocalAccess(false)
{
    const string &disableSql = autil::EnvUtil::getEnv("disableSql", "");
    if (!disableSql.empty()) {
        _enableSql = !autil::StringUtil::fromString<bool>(disableSql);
    }
}

QrsServiceImpl::~QrsServiceImpl() {
}

bool QrsServiceImpl::doInit() {
    string bizNamesStr = autil::EnvUtil::getEnv("basicTuringBizNames", "");
    if (!bizNamesStr.empty()) {
        const vector<string> &bizNames = StringUtil::split(bizNamesStr, ",");
        _basicTuringBizNames.insert(bizNames.begin(), bizNames.end());
    }
    if (_enableSql) {
        AUTIL_LOG(INFO, "jvm setup");
        auto status = iquan::IquanEnv::jvmSetup(iquan::jt_hdfs_jvm, {}, "");
        if (!status.ok()) {
            AUTIL_LOG(ERROR, "iquan jvm init failed, error msg: %s, will not start sql biz",
                    status.errorMessage().c_str());
            _enableSql = false;
        }
    }

    if (_threadPoolManager == nullptr) {
        return false;
    }
    int32_t sessionPoolSize = _threadPoolManager->getTotalThreadNum() +
                              _threadPoolManager->getTotalQueueSize();
    _sessionPool.reset(new SessionPoolImpl<QrsArpcSession>(sessionPoolSize));
    _sqlSessionPool.reset(new SessionPoolImpl<QrsArpcSqlSession>(sessionPoolSize));
    if (!_turingUrlTransform.init()) {
        AUTIL_LOG(ERROR, "turing url transform init failed");
        return false;
    }
    return true;
}

void QrsServiceImpl::rewriteWorkerParam() {
    auto threadNumScaleFactor = autil::EnvUtil::getEnv(WorkerParam::THREAD_NUMBER_SCALE_FACTOR);
    if (_workerParam.threadNumber == sysconf(_SC_NPROCESSORS_ONLN) && threadNumScaleFactor.empty()) {
        float factor = (double)DEFAULT_MAX_THREAD_NUM / _workerParam.threadNumber;
        _workerParam.threadNumberScaleFactor = factor;
        autil::EnvUtil::setEnv(WorkerParam::THREAD_NUMBER_SCALE_FACTOR +"Inner", StringUtil::toString(factor), true);
        AUTIL_LOG(INFO, "adjust thread number scale factor to %f", factor);
    }
    auto ioThreadNumber = autil::EnvUtil::getEnv(WorkerParam::IO_THREAD_NUM);
    if (ioThreadNumber.empty()) {
        _workerParam.ioThreadNumber = WorkerParam::DEFAULT_IO_THREAD_NUM;
        AUTIL_LOG(INFO, "adjust io thread number [%d]", _workerParam.ioThreadNumber);
    }
    _workerParam.useGig = true;
    AUTIL_LOG(INFO, "qrs worker param:\n%s", ToJsonString(_workerParam).c_str());
}

void QrsServiceImpl::initTraceSpan(const service::SpanPtr &span, const std::string &zoneName)
{
    auto appGroup = HA_RESERVED_APPNAME + zoneName;
    const auto &amonitorPath = _workerParam.amonitorPath;

    return TraceSpanUtil::qrsReceiveRpc(span, appGroup, amonitorPath);
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
    dynamic_pointer_cast<GraphServiceImpl>(_rpcService)->runGraph(
        controller, pGraphRequest, pGraphResponse, pClosure);
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
    if (sqlSession == nullptr) {
        processFailedSqlSession(snapshot, controller, request, response, done);
        return;
    }
    sqlSession->processRequest();
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
    auto sqlClient = snapshot->getSqlClient();
    if (!sqlClient) {
        AUTIL_LOG(WARN, "sql client is not enable.");
        if (done) {
            done->Run();
        }
        return;
    }
    std::string result;
    iquan::Status status = sqlClient->dumpCatalog(result);
    if (!status.ok()) {
        iquan::StatusJsonWrapper statusWrapper;
        statusWrapper.status = status;
        response->set_assemblyresult(autil::legacy::ToJsonString(statusWrapper));
        AUTIL_LOG(WARN, "dump catalog failed, error msg: [%s].", status.errorMessage().c_str());
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
        AUTIL_LOG(ERROR, "rpcServer or arpcServer NULL");
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
        AUTIL_LOG(ERROR, "register service failed");
        return false;
    }

    if (_enableSql) {
        auto gigRpcServer = rpcServer->gigRpcServer;
        if (!NaviInstance::get().initNavi(
                        gigRpcServer->hasGrpc() ? gigRpcServer : nullptr,
                        _workerMetricsReporter.get())) {
            AUTIL_LOG(ERROR, "init global navi failed");
            return false;
        }
    } else {
        AUTIL_LOG(INFO, "skip init global navi as disable sql");
    }

    if (!addHttpRpcMethod(httpRpc, "/QrsService/search", "/")) {
        return false;
    }
    string turingPrefix = autil::EnvUtil::getEnv("basicTuringPrefix", "");
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
        AUTIL_LOG(WARN, "alias old cm2 check path failed");
        return false;
    }
    auto gigRpcServer = rpcServer->gigRpcServer;
    if (gigRpcServer->hasGrpc()) {
        gigRpcServer->registerGrpcService("/isearch.proto.QrsService/search",
            new QrsRequest(),
            new QrsResponse(),
            info,
            std::bind(&QrsServiceImpl::gigSearch,
                this,
                placeholders::_1,
                placeholders::_2,
                placeholders::_3,
                placeholders::_4));
        gigRpcServer->registerGrpcService("/isearch.proto.QrsService/searchSql",
            new QrsRequest(),
            new QrsResponse(),
            info,
            std::bind(&QrsServiceImpl::gigSearchSql,
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
        AUTIL_LOG(WARN, "method [%s] not found", searchMethod.c_str());
        return false;
    }
    http_arpc::ServiceMethodPair serviceMethodPair = httpRpc->getMethod(searchMethod);
    RPCService *service = serviceMethodPair.first;
    RPCMethodDescriptor *method = serviceMethodPair.second;
    if (!service || !method) {
        AUTIL_LOG(WARN, "method descriptor [%s] not found", searchMethod.c_str());
        return false;
    } else {
        arpc::ThreadPoolDescriptor tpd(HA_SEARCH_THREAD_POOL_NAME, get_nprocs(), 1000);
        if(!httpRpc->addMethod(compatibleRtpMethod, serviceMethodPair, tpd)) {
            AUTIL_LOG(WARN, "add rpc method [%s] failed", compatibleRtpMethod.c_str());
            return false;
        }
    }
    return true;
}

multi_call::QuerySessionPtr QrsServiceImpl::constructQuerySession(
        const std::string& zoneName,
        RPCClosure *done,
        SessionSrcType &type)
{
    auto arpcClosure = dynamic_cast<multi_call::ArpcClosure *>(done);
    multi_call::QuerySessionPtr querySession;
    if (arpcClosure) {
        querySession = arpcClosure->getQuerySession();
        type = SESSION_SRC_ARPC;
    }

    if (!querySession) {
        auto commonClosure = dynamic_cast<multi_call::CommonClosure *>(done);
        if (commonClosure) {
            querySession = commonClosure->getQuerySession();
            auto closure = commonClosure->getClosure();
            auto httpClosure = dynamic_cast<http_arpc::HTTPRPCServerClosure *>(closure);
            if (httpClosure) {
                type = SESSION_SRC_HTTP;
            }
        }
    }

    if (!querySession) {
        querySession.reset(new multi_call::QuerySession(_searchService));
    }
    initTraceSpan(querySession->getTraceServerSpan(), zoneName);
    return querySession;
}

#define INIT_ARPC_SESSION(session, controller, request, response, done, snapshot)                  \
    SessionSrcType type = SESSION_SRC_UNKNOWN;                                                     \
    auto querySession = constructQuerySession(snapshot->getZoneName(), done, type);                \
    session->setGigQuerySession(querySession);                                                     \
    session->setClientAddress(getClientIp(controller));                                            \
    session->setStartTime();                                                                       \
    session->setRequest(request, response, done, snapshot);                                        \
    session->setUseGigSrc(_workerParam.useGigSrc);                                                 \
    session->setPoolCache(_poolCache.get());                                                       \
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
    AUTIL_LOG(WARN, "%s", errMsg.c_str());

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
    AUTIL_LOG(WARN, "%s", errMsg.c_str());
    ErrorResult errorResult(ERROR_QRS_SESSION_POOL_EXCEED, errMsg);
    errorResult.setHostName(getClientIp(controller));
    failSession->setDropped();
    sql::QrsSessionSqlResult result;
    result.errorResult = errorResult;
    failSession->endQrsSession(result);
}

ServiceSnapshotBasePtr QrsServiceImpl::doCreateServiceSnapshot() {
    ServiceSnapshotPtr snapshotPtr;
    Ha3ServiceSnapshot* ha3ServiceSnapshot = nullptr;
    if (_workerParam.enableLocalAccess) {
         ha3ServiceSnapshot = new LocalServiceSnapshot(_enableSql);
    } else {
        ha3ServiceSnapshot = new QrsServiceSnapshot(_enableSql);
    }
    ha3ServiceSnapshot->setBasicTuringBizNames(_basicTuringBizNames);
    snapshotPtr.reset(ha3ServiceSnapshot);
    return snapshotPtr;
}

bool QrsServiceImpl::initThreadPoolManager() {
    rewriteWorkerParam();
    _threadPoolManager.reset(new ThreadPoolManager);
    int threadNum = _workerParam.threadNumber;
    if (_workerParam.threadNumber == sysconf(_SC_NPROCESSORS_ONLN)) {
        threadNum = std::ceil(_workerParam.threadNumber * _workerParam.threadNumberScaleFactor);
        if (threadNum > DEFAULT_MAX_THREAD_NUM) {
            threadNum = DEFAULT_MAX_THREAD_NUM;
        }
        AUTIL_LOG(INFO, "adjust thread number form [%d] to [%d]", _workerParam.threadNumber, threadNum);
    }
    if (!_threadPoolManager->addThreadPool(DEFAULT_TASK_QUEUE_NAME,
                    _workerParam.queueSize, threadNum))
    {
        AUTIL_LOG(ERROR, "addThreadPool failed, queue name [%s]",
                DEFAULT_TASK_QUEUE_NAME);
        return false;
    }

    string extraTaskQueueConfig = autil::EnvUtil::getEnv(
            WorkerParam::EXTRA_TASK_QUEUE_CONFIG, "");

    if (!extraTaskQueueConfig.empty()
        && !_threadPoolManager->addThreadPool(extraTaskQueueConfig))
    {
        AUTIL_LOG(ERROR, "addThreadPool failed, extraTaskQueueConfig [%s]",
                extraTaskQueueConfig.c_str());
        return false;
    }
    if (!_threadPoolManager->start()) {
        AUTIL_LOG(ERROR, "start thread pool manager failed");
        return false;
    }
    return true;
}

void QrsServiceImpl::beforeStopServiceSnapshot() {
    AUTIL_LOG(INFO, "before stop service snapshot");
    NaviInstance::get().stopSnapshot();
    SearchManagerAdapter::beforeStopServiceSnapshot();
}

void QrsServiceImpl::stopWorker() {
    SearchManagerAdapter::stopWorker();
    NaviInstance::get().stopNavi();
}

std::unique_ptr<iquan::CatalogInfo> QrsServiceImpl::getCatalogInfo() const {
    auto snapshot = getServiceSnapshot();
    if (snapshot) {
        return snapshot->getCatalogInfo();
    }
    return nullptr;
}

void QrsServiceImpl::pushSearchItem(const string& taskQueueName, autil::WorkItem *item)
{
    ThreadPool* threadPool = _threadPoolManager->getThreadPool(taskQueueName);
    if (!threadPool) {
        threadPool = _threadPoolManager->getThreadPool(DEFAULT_TASK_QUEUE_NAME);
        if (!threadPool) {
            AUTIL_LOG(WARN, "pushWorkItem failed, not found taskQueue[%s]",
                    DEFAULT_TASK_QUEUE_NAME);
            if (item) {
                item->drop();
            }
        return;
        }
    }
    if (!item) {
        AUTIL_LOG(ERROR, "worker item is NULL");
        return;
    }
    if (threadPool->pushWorkItem(item, false) != ThreadPool::ERROR_NONE) {
        AUTIL_LOG(ERROR, "Failed to push item into threadPool");
        item->drop();
    }
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

bool QrsServiceImpl::protocolCheck(::google::protobuf::RpcController *controller,
                                   ::google::protobuf::Closure *done) const
{
    auto grpcOnly = _workerParam.grpcOnly;
    auto gigClosure = dynamic_cast<GigClosure *>(done);
    if (nullptr == gigClosure) {
        AUTIL_LOG(ERROR, "illegal request, done closure is not gig closure");
        return false;
    }
    auto protocolType = gigClosure->getProtocolType();
    if (grpcOnly && multi_call::MC_PROTOCOL_GRPC != protocolType) {
        if (controller != nullptr) {
            controller->SetFailed("protocol not supported: "
                    + multi_call::convertProtocolTypeToStr(protocolType));
        }
        done->Run();
        return false;
    } else {
        return true;
    }
}

} // namespace turing
} // namespace isearch
