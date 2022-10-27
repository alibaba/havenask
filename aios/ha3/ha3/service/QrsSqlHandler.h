#ifndef ISEARCH_QRSSQLHANDLER_H
#define ISEARCH_QRSSQLHANDLER_H

#include <ha3/turing/qrs/QrsSqlBiz.h>
#include <ha3/service/QrsSessionSqlRequest.h>
#include <ha3/service/QrsSessionSqlResult.h>
#include <navi/proto/GraphDef.pb.h>
#include <iquan_common/service/Response.h>
#include <iquan_common/service/Request.h>
#include <multi_call/interface/QuerySession.h>

BEGIN_HA3_NAMESPACE(service);

class QrsSqlHandler
{
public:
    QrsSqlHandler(const turing::QrsSqlBizPtr &qrsSqlBiz,
                  const multi_call::QuerySessionPtr &querySession,
                  common::TimeoutTerminator *timeoutTerminator);
    ~QrsSqlHandler();
private:
    QrsSqlHandler(const QrsSqlHandler &);
    QrsSqlHandler &operator=(const QrsSqlHandler &);
public:
    void process(const QrsSessionSqlRequest &sessionRequest,
                 QrsSessionSqlResult &sessionResult);

    uint32_t getFinalOutputNum() const { return _finalOutputNum; }
    const sql::SqlSearchInfo &getSearchInfo() const { return _searchInfo; }
    void setUseGigSrc(bool flag) { _useGigSrc = flag; }
private:
    std::string getDefaultCatalogName(const sql::SqlQueryRequest *sqlRequest);
    std::string getDefaultDatabaseName(const sql::SqlQueryRequest *sqlRequest);
    bool runGraph(navi::GraphDef *graphDef,
                  const QrsSessionSqlRequest &sessionRequest,
                  QrsSessionSqlResult &sessionResult,
                  std::vector<std::string> &outputPort,
                  std::vector<std::string> &outputNode,
                  int64_t leftTime,
                  const std::string &traceLevel,
                  std::string &runGraphDesc);
    void getSqlPlan(const QrsSessionSqlRequest &sessionRequest,
                    iquan::SqlQueryResponse &sqlQueryResponse,
                    iquan::SqlQueryRequest &sqlQueryRequest,
                    HA3_NS(common)::ErrorResult &errorResult);
    navi::GraphDef* getGraphDef(iquan::SqlPlan &sqlPlan,
                                iquan::SqlQueryRequest &sqlQueryRequest,
                                const QrsSessionSqlRequest &sessionRequest,
                                int64_t leftTime,
                                const std::string &traceLevel,
                                HA3_NS(common)::ErrorResult &errorResult,
                                std::vector<std::string> &outputPort,
                                std::vector<std::string> &outputNode);
    void initExecConfig(const QrsSessionSqlRequest &sessionRequest,
                        const config::SqlConfigPtr &sqlConfig, int64_t leftTime,
                        const std::string &traceLevel,
                        iquan::ExecConfig &execConfig);
    static std::string getTraceLevel(const QrsSessionSqlRequest &sessionRequest);
    static bool getSearchInfo(const QrsSessionSqlRequest &sessionRequest);
    static bool getOutputSqlPlan(const QrsSessionSqlRequest &sessionRequest);
    static bool getForbitMergeSearchInfo(const QrsSessionSqlRequest &sessionRequest);
    static void traceErrorMsg(const std::string &traceLevel,
                              iquan::SqlQueryRequest &sqlQueryRequest,
                              const iquan::SqlQueryResponse &sqlQueryResponse,
                              const QrsSessionSqlRequest &sessionRequest,
                              QrsSessionSqlResult &sessionResult);
    bool checkTimeOutWithError(HA3_NS(common)::ErrorResult &errorResult,
                               ErrorCode errorCode);
    static bool fillSessionSqlResult(navi::GraphOutputPortsPtr &outputs,
            QrsSessionSqlResult &sessionResult, std::string &runGraphDesc);
    static size_t getParallelNum(const HA3_NS(sql)::SqlQueryRequest *sqlQueryRequest,
                                 const HA3_NS(config)::SqlConfigPtr &sqlConfig);
    static std::vector<std::string> getParallelTalbes(
            const HA3_NS(sql)::SqlQueryRequest *sqlQueryRequest,
            const HA3_NS(config)::SqlConfigPtr &sqlConfig);
    bool getSqlDynamicParams(
            const QrsSessionSqlRequest &sessionRequest,
            std::vector<std::vector<autil::legacy::Any>> &dynamicParams);
    bool parseSqlParams(const QrsSessionSqlRequest &sessionRequest,
                        iquan::SqlQueryRequest &sqlQueryRequest);
    bool parseIquanSqlParams(const QrsSessionSqlRequest &sessionRequest, iquan::SqlQueryRequest &sqlQueryRequest);
    bool getLackResultEnable(const HA3_NS(sql)::SqlQueryRequest *sqlQueryRequest,
                        const HA3_NS(config)::SqlConfigPtr &sqlConfig);
private:
    turing::QrsSqlBizPtr _qrsSqlBiz;
    multi_call::QuerySessionPtr _querySession;
    common::TimeoutTerminator *_timeoutTerminator;
    uint32_t _finalOutputNum;
    sql::SqlSearchInfo _searchInfo;
    bool _useGigSrc;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsSqlHandler);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSSQLHANDLER_H
