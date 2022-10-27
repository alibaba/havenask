#ifndef ISEARCH_QRSSEARCHERHANDLER_H
#define ISEARCH_QRSSEARCHERHANDLER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/AccessLog.h>
#include <ha3/qrs/QrsProcessor.h>
#include <ha3/service/QrsSessionSearchRequest.h>
#include <ha3/service/QrsSessionSearchResult.h>
#include <ha3/service/QrsSearchConfig.h>
#include <memory>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/turing/qrs/QrsServiceSnapshot.h>

BEGIN_HA3_NAMESPACE(service);

class QrsSearcherHandler
{
public:
    QrsSearcherHandler(const std::string &partionId,
                       const turing::QrsServiceSnapshotPtr &snapshot,
                       const turing::QrsRunGraphContextPtr &context,
                       common::TimeoutTerminator *timeoutTerminator = nullptr);
    ~QrsSearcherHandler();
public:
    QrsSessionSearchResult process(const QrsSessionSearchRequest &sessionRequest,
                                   proto::ProtocolType protocolType,
                                   const std::string &bizName);

private:
    bool parseConfigClause(common::RequestPtr &requestPtr);

    void formatErrorResult(QrsSessionSearchResult &sessionResult);

    bool fillResult(const common::RequestPtr &requestPtr,
                    const common::ResultPtr &resultPtr,
                    QrsSessionSearchResult &sessionResult);

    bool doProcess(common::RequestPtr &requestPtr,
                   common::ResultPtr &resultPtr);

    void reportTracerMetrics(common::Tracer *tracer);

    bool getFormatType(const std::string &formatTypeStr,
                       ResultFormatType &resultFormatType);
    void transAttributeName(const common::RequestPtr &requestPtr,
                            std::map<std::string, std::string> &attrNameMap);

    void collectStatistics(const common::ResultPtr &resultPtr,
                           const std::string &resultString,
                           bool allowLackOfSummary);

    qrs::QrsProcessorPtr getQrsProcessorChain(const std::string &chainName,
            common::Tracer *tracer);

    std::string formatResultString(const common::RequestPtr &requestPtr,
                                   const common::ResultPtr &resultPtr,
                                   ResultFormatType resultFormatType,
                                   uint32_t protoFormatOption = 0);

    bool runProcessChain(const std::string &qrsChainName,
                         common::TracerPtr &tracerPtr,
                         common::RequestPtr &reqeustPtr,
                         common::ResultPtr &resultPtr);
    void addTraceInfo(const common::ResultPtr &resultPtr);
    void updateTimeoutTerminator(int64_t rpcTimeout);
private:
    // for test
    void setSessionMetricsCollector(
            const monitor::SessionMetricsCollectorPtr &metricsCollectorPtr)
    {
        _metricsCollectorPtr = metricsCollectorPtr;
    }

private:
    QrsSearchConfigPtr _qrsSearchConfig;
    std::string _partitionId;
    const turing::QrsServiceSnapshotPtr _snapshot;
    monitor::SessionMetricsCollectorPtr _metricsCollectorPtr;
    common::AccessLog _accessLog;
    common::ErrorResult _errorResult;
    turing::QrsRunGraphContextPtr _runGraphContext;
    common::TimeoutTerminator *_timeoutTerminator;
private:
    friend class QrsSearcherHandlerTest;
private:
    static std::string RPC_TOCKEN_KEY;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsSearcherHandler);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSSEARCHERHANDLER_H
