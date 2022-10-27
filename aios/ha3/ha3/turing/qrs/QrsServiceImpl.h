#pragma once

#include <atomic>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/search/GraphServiceImpl.h>
#include <suez/turing/proto/Search.pb.h>
#include <ha3/proto/QrsService.pb.h>
#include <ha3/service/SessionPool.h>
#include <ha3/service/ThreadPoolManager.h>
#include <ha3/turing/qrs/QrsServiceSnapshot.h>
#include <ha3/service/QrsArpcSession.h>
#include <ha3/service/QrsArpcSqlSession.h>
#include <ha3/service/RpcContextUtil.h>
#include <ha3/turing/qrs/TuringUrlTransform.h>
#include <http_arpc/HTTPRPCServerClosure.h>
BEGIN_HA3_NAMESPACE(turing);
class QrsServiceImpl:public suez::turing::GraphServiceImpl, public proto::QrsService
{
public:
    QrsServiceImpl();
    ~QrsServiceImpl();
private:
    QrsServiceImpl(const QrsServiceImpl &);
    QrsServiceImpl& operator=(const QrsServiceImpl &);
public:
    void search(RPCController *controller, const proto::QrsRequest *request,
                proto::QrsResponse *response, RPCClosure *done) override;
    void searchTuring(RPCController *controller,const proto::QrsRequest *request,
                proto::QrsResponse *response, RPCClosure *done) override;
    void searchSql(RPCController *controller, const proto::QrsRequest *request,
                   proto::QrsResponse *response, RPCClosure *done) override;
    void sqlClientInfo(RPCController *controller, const proto::SqlClientRequest *request,
                       proto::SqlClientResponse *response, RPCClosure *done) override;

    void gigSearch(google::protobuf::RpcController *controller,
                   google::protobuf::Message *request,
                   google::protobuf::Message *response,
                   multi_call::GigClosure *closure);
    void gigSearchSql(google::protobuf::RpcController *controller,
                      google::protobuf::Message *request,
                      google::protobuf::Message *response,
                      multi_call::GigClosure *closure);
protected:
    suez::turing::ServiceSnapshotPtr doCreateServiceSnapshot() override;
    bool doInit() override;
    bool doInitRpcServer(suez::RpcServer *rpcServer) override;
    void doReportMetrics() override;
private:
    bool initThreadPoolManager();
    void pushSearchItem(const std::string& taskQueueName, autil::WorkItem *item);

    service::QrsArpcSession *constructQrsArpcSession(
            const QrsServiceSnapshotPtr &snapshot,
            RPCController *controller,
            const proto::QrsRequest *request,
            proto::QrsResponse *response,
            RPCClosure *done);
    service::QrsArpcSqlSession *constructQrsArpcSqlSession(
            const QrsServiceSnapshotPtr &snapshot,
            RPCController *controller,
            const proto::QrsRequest *request,
            proto::QrsResponse *response,
            RPCClosure *done);
    multi_call::QuerySessionPtr constructQuerySession(
            const std::string& zoneName,
            const proto::QrsRequest *request,
            RPCController *controller,
            RPCClosure *done,
            SessionSrcType &type);
    void processFailedSession(
            const QrsServiceSnapshotPtr &snapshot,
            RPCController *controller,
            const proto::QrsRequest *request,
            proto::QrsResponse *response,
            RPCClosure *done);
    void processFailedSqlSession(
            const QrsServiceSnapshotPtr &snapshot,
            RPCController *controller,
            const proto::QrsRequest *request,
            proto::QrsResponse *response,
            RPCClosure *done);
    void rewriteWorkerParam();
    bool addHttpRpcMethod(http_arpc::HTTPRPCServer* httpRpc,
                          const std::string &searchMethod,
                          const std::string &compatibleRtpMethod);
    service::RpcContextPtr createRpcContext(const std::string &zoneName,
                                            RPCController *controller,
                                            const proto::QrsRequest *request);
    service::RpcContextPtr createRpcContext(const std::string &zoneName,
                                            RPCController *controller,
                                            const http_arpc::EagleInfo &eagleInfo);
    service::RpcContextPtr qrsReceiveRpc(
            RPCController *controller,
            const proto::QrsRequest *request,
            const suez::turing::ServiceSnapshotPtr& snapshot);
private:
    static std::string getClientIp(RPCController *controller);
private:
    bool _enableSql;
    service::SessionPoolPtr _sessionPool;
    service::SessionPoolPtr _sqlSessionPool;
    service::ThreadPoolManagerPtr _threadPoolManager;
    std::atomic_bool _threadPoolManagerInitFlag;
    std::set<std::string> _basicTuringBizNames;
    TuringUrlTransform _turingUrlTransform;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsServiceImpl);

END_HA3_NAMESPACE(turing);
