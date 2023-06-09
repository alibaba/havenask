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

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/ThreadPoolManager.h"
#include "ha3/isearch.h"
#include "ha3/proto/QrsService.pb.h"
#include "ha3/service/QrsArpcSession.h"
#include "ha3/service/QrsArpcSqlSession.h"
#include "ha3/service/SessionPool.h"
#include "ha3/service/TraceSpanUtil.h"
#include "ha3/turing/qrs/QrsServiceSnapshot.h"
#include "ha3/turing/qrs/TuringUrlTransform.h"
#include "aios/network/http_arpc/HTTPRPCServer.h"
#include "aios/network/http_arpc/HTTPRPCServerClosure.h"
#include "suez/turing/proto/Search.pb.h"
#include "suez/turing/search/base/SearchManagerAdapter.h"

namespace isearch {
namespace turing {

class QrsServiceImpl : public suez::turing::SearchManagerAdapter,
                       public proto::QrsService {
public:
    QrsServiceImpl();
    virtual ~QrsServiceImpl();

private:
    QrsServiceImpl(const QrsServiceImpl &);
    QrsServiceImpl &operator=(const QrsServiceImpl &);

public:
    void search(RPCController *controller,
                const proto::QrsRequest *request,
                proto::QrsResponse *response,
                RPCClosure *done) override;
    void searchTuring(RPCController *controller,
                      const proto::QrsRequest *request,
                      proto::QrsResponse *response,
                      RPCClosure *done) override;
    void searchSql(RPCController *controller,
                   const proto::QrsRequest *request,
                   proto::QrsResponse *response,
                   RPCClosure *done) override;
    void sqlClientInfo(RPCController *controller,
                       const proto::SqlClientRequest *request,
                       proto::SqlClientResponse *response,
                       RPCClosure *done) override;

    void gigSearch(google::protobuf::RpcController *controller,
                   google::protobuf::Message *request,
                   google::protobuf::Message *response,
                   multi_call::GigClosure *closure);
    void gigSearchSql(google::protobuf::RpcController *controller,
                      google::protobuf::Message *request,
                      google::protobuf::Message *response,
                      multi_call::GigClosure *closure);
protected:
    suez::turing::ServiceSnapshotBasePtr doCreateServiceSnapshot() override;
    bool doInit() override;
    bool doInitRpcServer(suez::RpcServer *rpcServer) override;
    bool initThreadPoolManager() override;
    void beforeStopServiceSnapshot() override;
    void stopWorker() override;
    std::unique_ptr<iquan::CatalogInfo> getCatalogInfo() const override;

private:
    void pushSearchItem(const std::string &taskQueueName, autil::WorkItem *item);

    service::QrsArpcSession *constructQrsArpcSession(const QrsServiceSnapshotPtr &snapshot,
                                                     RPCController *controller,
                                                     const proto::QrsRequest *request,
                                                     proto::QrsResponse *response,
                                                     RPCClosure *done);
    service::QrsArpcSqlSession *constructQrsArpcSqlSession(const QrsServiceSnapshotPtr &snapshot,
                                                           RPCController *controller,
                                                           const proto::QrsRequest *request,
                                                           proto::QrsResponse *response,
                                                           RPCClosure *done);
    multi_call::QuerySessionPtr constructQuerySession(const std::string &zoneName,
                                                      RPCClosure *done,
                                                      SessionSrcType &type);
    void processFailedSession(const QrsServiceSnapshotPtr &snapshot,
                              RPCController *controller,
                              const proto::QrsRequest *request,
                              proto::QrsResponse *response,
                              RPCClosure *done);
    void processFailedSqlSession(const QrsServiceSnapshotPtr &snapshot,
                                 RPCController *controller,
                                 const proto::QrsRequest *request,
                                 proto::QrsResponse *response,
                                 RPCClosure *done);
    void rewriteWorkerParam();
    bool addHttpRpcMethod(http_arpc::HTTPRPCServer *httpRpc,
                          const std::string &searchMethod,
                          const std::string &compatibleRtpMethod);
    void initTraceSpan(const service::SpanPtr &span,
                       const std::string &zoneName);
    bool protocolCheck(::google::protobuf::RpcController *controller,
                       ::google::protobuf::Closure *done) const;

private:
    static std::string getClientIp(RPCController *controller);

private:
    bool _enableSql;
    bool _enableLocalAccess;
    service::SessionPoolPtr _sessionPool;
    service::SessionPoolPtr _sqlSessionPool;
    std::set<std::string> _basicTuringBizNames;
    TuringUrlTransform _turingUrlTransform;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsServiceImpl> QrsServiceImplPtr;

} // namespace turing
} // namespace isearch
