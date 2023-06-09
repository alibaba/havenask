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

#include "ha3/common/TimeoutTerminator.h"
#include "ha3/config/SqlConfig.h"
#include "ha3/sql/framework/QrsSessionSqlRequest.h"
#include "ha3/sql/framework/QrsSessionSqlResult.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/turing/common/ModelConfig.h"
#include "iquan/config/ExecConfig.h"
#include "iquan/jni/Iquan.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "navi/engine/Navi.h"
#include "navi/engine/RunGraphParams.h"
#include "navi/proto/GraphDef.pb.h"
#include "turing_ops_util/variant/Tracer.h"
#include "autil/ObjectTracer.h"

namespace isearch {
namespace turing {
class QrsSqlBiz;
typedef std::shared_ptr<QrsSqlBiz> QrsSqlBizPtr;
} // namespace turing
} // namespace isearch

namespace isearch {
namespace service {

class QrsSqlHandler;

class RunGraphClosure : public navi::NaviUserResultClosure {
public:
    RunGraphClosure(QrsSqlHandler *handler);
public:
    void run(navi::NaviUserResultPtr userResult) override;
private:
     QrsSqlHandler *_handler;
};

struct QrsSqlHandlerResult {
    uint32_t finalOutputNum;
    sql::SqlSearchInfo searchInfo;
    multi_call::GigStreamRpcInfoMap rpcInfoMap;
};

class QrsSqlHandler : public autil::ObjectTracer<QrsSqlHandler, true>
{
public:
    typedef std::function<void(QrsSqlHandlerResult)> CallbackType;
public:
    QrsSqlHandler(const turing::QrsSqlBizPtr &qrsSqlBiz,
                  const multi_call::QuerySessionPtr &querySession,
                  common::TimeoutTerminator *timeoutTerminator);
    virtual ~QrsSqlHandler() = default;
private:
    QrsSqlHandler(const QrsSqlHandler &);
    QrsSqlHandler &operator=(const QrsSqlHandler &);
public:
    void process(const sql::QrsSessionSqlRequest *sessionRequest,
                 sql::QrsSessionSqlResult *sessionResult,
                 CallbackType callback);

    void setUseGigSrc(bool flag) { _useGigSrc = flag; }
    void setGDBPtr(void *ptr) { _gdbPtr = ptr; }
    void runGraphCallback(navi::NaviUserResultPtr naviUserResult);
private:
    void runGraph();
    bool processResult(navi::NaviUserResultPtr naviUserResult, std::string &runGraphDesc);
    static std::string getTaskQueueName(const sql::QrsSessionSqlRequest &sessionRequest);
    static std::string getTraceLevel(const sql::QrsSessionSqlRequest &sessionRequest);
    static sql::QrsSessionSqlResult::SearchInfoLevel parseSearchInfoLevel(
            const sql::QrsSessionSqlRequest &sessionRequest);
    bool checkTimeOutWithError(isearch::common::ErrorResult &errorResult,
                               ErrorCode errorCode);
    bool getResult(const navi::NaviUserResultPtr &result,
                   std::vector<navi::DataPtr> &tableData,
                   std::vector<navi::DataPtr> &sqlPlanDataVec);
    void tryFillSessionSqlPlan(const std::vector<navi::DataPtr> &sqlPlanDataVec,
                               bool outputSqlPlan,
                               sql::QrsSessionSqlResult &sessionResult,
                               std::vector<std::string> &runGraphDescVec);
    bool fillSessionSqlResult(const navi::NaviResultPtr &result,
                              const std::vector<navi::DataPtr> &tableDataVec,
                              const std::vector<navi::DataPtr> &sqlPlanDataVec,
                              bool outputSqlPlan,
                              sql::QrsSessionSqlResult &sessionResult,
                              std::string &runGraphDesc);
    autil::CompressType
    parseResultCompressType(const sql::QrsSessionSqlRequest &sessionRequest);
    navi::RunGraphParams makeRunGraphParams(const sql::QrsSessionSqlRequest &request,
                                            navi::GraphId graphId,
                                            navi::DataPtr outData);
    virtual std::unique_ptr<navi::GraphDef> buildRunSqlGraph(
            const std::string &bizName,
            navi::GraphId &graphId);
    void afterRun();
    static bool getForbitMergeSearchInfo(const sql::QrsSessionSqlRequest &sessionRequest);
    static bool getForbitSearchInfo(const sql::QrsSessionSqlRequest &sessionRequest);
    static bool getUserParams(const sql::QrsSessionSqlRequest &sessionRequest,
                              std::map<std::string, std::string> &userParam);
    static bool getOutputSqlPlan(const sql::QrsSessionSqlRequest &sessionRequest);
private:
    navi::RunGraphParams _runGraphParams;
    RunGraphClosure _runGraphClosure;
    CallbackType _callback;
    const sql::QrsSessionSqlRequest *_sessionRequest;
    sql::QrsSessionSqlResult *_sessionResult;
    turing::QrsSqlBizPtr _qrsSqlBiz;
    multi_call::QuerySessionPtr _querySession;
    common::TimeoutTerminator *_timeoutTerminator = nullptr;
    uint32_t _finalOutputNum;
    sql::SqlSearchInfo _searchInfo;
    int64_t _beginTime = 0;
    multi_call::GigStreamRpcInfoMap _rpcInfoMap;
    bool _useGigSrc;
    void *_gdbPtr = nullptr;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsSqlHandler> QrsSqlHandlerPtr;

} // namespace service
} // namespace isearch
