#include <ha3/service/QrsSearcherHandler.h>
#include <autil/TimeUtility.h>
#include <ha3/config/QrsConfig.h>
#include <ha3/common/XMLResultFormatter.h>
#include <ha3/common/FBResultFormatter.h>
#include <ha3/queryparser/RequestParser.h>
#include <ha3/proto/ProtoClassUtil.h>
#include <ha3/common/PBResultFormatter.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <memory>
#include <ha3/monitor/TracerMetricsReporter.h>
#include <algorithm>
#include <ha3/turing/qrs/QrsBiz.h>
#include <suez/turing/expression/util/VirtualAttrConvertor.h>

using namespace std;
using namespace autil;
using namespace google::protobuf;
using namespace google::protobuf::io;
using namespace suez::turing;

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(turing);

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, QrsSearcherHandler);

std::string QrsSearcherHandler::RPC_TOCKEN_KEY = "tkn";

QrsSearcherHandler::QrsSearcherHandler(
        const string &partionId,
        const QrsServiceSnapshotPtr &snapshot,
        const turing::QrsRunGraphContextPtr &context,
        common::TimeoutTerminator *timeoutTerminator)
    : _partitionId(partionId)
    , _snapshot(snapshot)
    , _runGraphContext(context)
    , _timeoutTerminator(timeoutTerminator)
{
}

QrsSearcherHandler::~QrsSearcherHandler() {
}

QrsSessionSearchResult QrsSearcherHandler::process(
        const QrsSessionSearchRequest &sessionRequest,
        proto::ProtocolType protocolType,
        const std::string &bizName)
{
    QrsSessionSearchResult sessionResult;
    // bizName not work, if not found, return first biz in map
    QrsBizPtr qrsBiz = dynamic_pointer_cast<QrsBiz>(_snapshot->getBiz(bizName));
    if (!qrsBiz) {
        string errorString = "qrs Biz Not Found";
        HA3_LOG(WARN, "qrs biz [%s] Not Found", bizName.c_str());
        _errorResult.resetError(ERROR_UNKNOWN, errorString);
        formatErrorResult(sessionResult);
        return sessionResult;
    }

    _qrsSearchConfig = qrsBiz->getQrsSearchConfig();
    sessionResult.resultCompressType = _qrsSearchConfig->_resultCompressType;
    _metricsCollectorPtr = sessionRequest.metricsCollectorPtr;
    if (!_qrsSearchConfig->_qrsChainMgrPtr) {
        string errorString =  "_qrsChainMgrPtr is NULL!!!";
        HA3_LOG(ERROR, "%s", errorString.c_str());
        _errorResult.resetError(ERROR_UNKNOWN, errorString);
        _metricsCollectorPtr->increaseProcessErrorQps();
        formatErrorResult(sessionResult);
        return sessionResult;
    }
    _accessLog.setSessionSrcType(sessionRequest.sessionSrcType);
    _accessLog.setIp(sessionRequest.clientIp);
    _accessLog.setQueryString(sessionRequest.requestStr);
    _errorResult.setHostInfo(_partitionId);
    assert(_metricsCollectorPtr);

    RequestPtr requestPtr(new Request(sessionRequest.pool));
    requestPtr->setOriginalString(sessionRequest.requestStr);
    requestPtr->setUseGrpc(protocolType == PT_GRPC);
    // parse config cluase and get compress type before cache and process chain
    if (!parseConfigClause(requestPtr)) {
        formatErrorResult(sessionResult);
        return sessionResult;
    }
    int64_t rpcTimeout = 1000 * (int64_t)requestPtr->getConfigClause()->getRpcTimeOut();
    updateTimeoutTerminator(rpcTimeout);
    _metricsCollectorPtr->setRequestType(requestPtr);

    ResultPtr resultPtr(new Result);
    if (!doProcess(requestPtr, resultPtr)) {
        formatErrorResult(sessionResult);
        return sessionResult;
    }
    _accessLog.setRowKey(requestPtr->getRowKey());

    Tracer *_tracer = resultPtr->getTracer();
    if (_tracer) {
        auto phaseOneSearchInfo = resultPtr->getPhaseOneSearchInfo();
        auto phaseTwoSearchInfo = resultPtr->getPhaseTwoSearchInfo();
        if (phaseOneSearchInfo) {
            REQUEST_TRACE(TRACE3, "PhaseOneSearchInfo: %s",
                          phaseOneSearchInfo->toString().c_str());
            REQUEST_TRACE(TRACE3, "PhaseOneSearchInfo.seekDocCount: %u",
                          phaseOneSearchInfo->seekDocCount);
            REQUEST_TRACE(TRACE3, "Qrs bizName: %s",
                          bizName.c_str());
        }
        if (phaseTwoSearchInfo) {
            REQUEST_TRACE(TRACE3, "PhaseTwoSearchInfo.summaryLatency: %ld us",
                          phaseTwoSearchInfo->summaryLatency);
        }
        REQUEST_TRACE(TRACE3, "session src type : %s", transSessionSrcType(sessionRequest.sessionSrcType).c_str());
    }
    if (!fillResult(requestPtr, resultPtr, sessionResult)) {
        formatErrorResult(sessionResult);
        return sessionResult;
    }
    collectStatistics(resultPtr, sessionResult.resultStr,
                      requestPtr->getConfigClause()->allowLackOfSummary());
    return sessionResult;
}

void QrsSearcherHandler::updateTimeoutTerminator(int64_t rpcTimeout) {
    if (rpcTimeout > 0 && _timeoutTerminator) {
        int64_t expireTime = _timeoutTerminator->getStartTime() + rpcTimeout ;
        if (expireTime < _timeoutTerminator->getExpireTime()) {
            HA3_LOG (DEBUG, "update timeout expire time from [%ld] to [%ld], rpc timeout [%ld]",
                     _timeoutTerminator->getExpireTime(), expireTime, rpcTimeout);
            _timeoutTerminator->updateExpireTime(expireTime);
        }
    }

}
void QrsSearcherHandler::formatErrorResult(QrsSessionSearchResult &sessionResult) {
    ErrorCode ec = _errorResult.getErrorCode();
    _accessLog.setStatusCode(ec);
    // default result format is xml, when error occur.

    sessionResult.formatType = RF_XML;
    const string &queryStr = _accessLog.getQueryString();
    if (!queryStr.empty()) {
        HA3_LOG(WARN, "error code[%d], error query:[%s]", ec, queryStr.c_str());
    }
    sessionResult.resultStr = XMLResultFormatter::xmlFormatErrorResult(_errorResult);
    sessionResult.errorStr = sessionResult.resultStr;
}

bool QrsSearcherHandler::fillResult(const RequestPtr &requestPtr,
                                    const ResultPtr &resultPtr,
                                    QrsSessionSearchResult &sessionResult)
{
    ConfigClause *configClause = requestPtr->getConfigClause();
    sessionResult.srcStr = configClause->getMetricsSrc();
    const string &resultFormat = configClause->getResultFormatSetting();
    if (!getFormatType(resultFormat, sessionResult.formatType)) {
        string errorMsg = "Invalid result format type[" + resultFormat + "]";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        _errorResult.resetError(ERROR_UNSUPPORT_RESULT_FORMAT, errorMsg);
        _metricsCollectorPtr->increaseSyntaxErrorQps();
        return false;
    }

    // update sessionResult.resultCompressType if query set compresstype.
    configClause->getCompressType(sessionResult.resultCompressType);

    _metricsCollectorPtr->resultFormatStartTrigger();
    sessionResult.resultStr = formatResultString(requestPtr, resultPtr,
            sessionResult.formatType, configClause->getProtoFormatOption());
    _metricsCollectorPtr->resultFormatEndTrigger();
    if (resultPtr->lackResult()) {
        sessionResult.multicallEc = multi_call::MULTI_CALL_ERROR_DEC_WEIGHT;
    } else {
        sessionResult.multicallEc = multi_call::MULTI_CALL_ERROR_NONE;
    }
    return true;
}

bool QrsSearcherHandler::parseConfigClause(RequestPtr &requestPtr) {
    RequestParser requestParser;
    if (!requestParser.parseConfigClause(requestPtr)) {
        HA3_LOG(WARN, "ParseConfigClause Failed: request[%s]",
                requestPtr->getOriginalString().c_str());
        const ErrorResult &errorResult = requestParser.getErrorResult();
        _errorResult.resetError(errorResult.getErrorCode(),
                                errorResult.getErrorMsg());
        _metricsCollectorPtr->increaseSyntaxErrorQps();
        return false;
    }
    return true;
}

bool QrsSearcherHandler::doProcess(RequestPtr &requestPtr,
                                   ResultPtr &resultPtr)
{
    ConfigClause *configClause = requestPtr->getConfigClause();
    if (!configClause->getDebugQueryKey().empty()) { //debug query
        configClause->setQrsChainName(DEFAULT_DEBUG_QRS_CHAIN);
    }
    TracerPtr tracer(configClause->createRequestTracer(
                    ProtoClassUtil::simplifyPartitionIdStr(_partitionId),
                    _snapshot->getAddress()));
    _metricsCollectorPtr->setTracer(tracer.get());
    _runGraphContext->setTracer(tracer);
    _metricsCollectorPtr->processChainStartTrigger();
    bool ret = runProcessChain(configClause->getQrsChainName(),
                               tracer, requestPtr, resultPtr);
    _metricsCollectorPtr->processChainEndTrigger();
    addTraceInfo(resultPtr);
    if (!ret) {
        return false;
    }
    reportTracerMetrics(resultPtr->getTracer());

    return true;
}

void QrsSearcherHandler::collectStatistics(const ResultPtr &resultPtr,
        const string &resultString, bool allowLackOfSummary)
{
    if (resultPtr->hasError()) {
        MultiErrorResultPtr& multiErrorResult =
            resultPtr->getMultiErrorResult();
        const ErrorResults& errorResults =
            multiErrorResult->getErrorResults();
        ErrorCode ec = errorResults[0].getErrorCode();
        _accessLog.setStatusCode(ec);
        const string &msg = errorResults[0].getErrorMsg();
        const string &partitionId = errorResults[0].getPartitionID();
        const auto &host = errorResults[0].getHostName();
        if (!(ec == ERROR_QRS_SUMMARY_LACK && allowLackOfSummary)) {
            _metricsCollectorPtr->increaseProcessErrorQps(); //do not report error qrs when allow lack of summary
            HA3_LOG(WARN, "error code[%d], error message[%s], error partition[%s], "
                    "error host[%s], error description[%s], error query:[%s]",
                    ec, msg.c_str(), partitionId.c_str(), host.c_str(),
                    haErrorCodeToString(ec).c_str(),
                    _accessLog.getQueryString().c_str());
        }
    }

    _metricsCollectorPtr->resultLengthTrigger(resultString.length());
    SessionMetricsCollector::RequestType requestType =
        _metricsCollectorPtr->getRequestType();
    uint32_t totalHits = 0;
    Hits *hits = resultPtr->getHits();
    if (hits) {
        _metricsCollectorPtr->returnCountTrigger(hits->size());
        totalHits = hits->totalHits();
    } else {
        _metricsCollectorPtr->returnCountTrigger(
                resultPtr->getMatchDocsForFormat().size());
        totalHits = resultPtr->getTotalMatchDocs();
    }
    if (SessionMetricsCollector::IndependentPhase2 != requestType) {
        _metricsCollectorPtr->estimatedMatchCountTrigger(totalHits);
    }
    if (SessionMetricsCollector::IndependentPhase2 != requestType
        && 0 == totalHits)
    {
        _metricsCollectorPtr->increaseEmptyQps();
    }
    _accessLog.setTotalHits(totalHits);
    PhaseOneSearchInfo *searchInfo = resultPtr->getPhaseOneSearchInfo();
    if (searchInfo) {
        _accessLog.setPhaseOneSearchInfo(*searchInfo);
        if(searchInfo->needReport) {
            _metricsCollectorPtr->fillPhaseOneSearchInfoMetrics(searchInfo);
        }
    }
}

void QrsSearcherHandler::reportTracerMetrics(Tracer *tracer) {
    if (NULL != tracer && NULL != _metricsCollectorPtr) {
        TracerMetricsReporter tracerMetricsReporter;
        tracerMetricsReporter.setTracer(tracer);
        tracerMetricsReporter.setRoleType(ROLE_QRS);
        tracerMetricsReporter.setMetricsCollector(_metricsCollectorPtr);
        tracerMetricsReporter.reportMetrics();
    }
}

bool QrsSearcherHandler::getFormatType(
        const string &resultFormat, ResultFormatType &resultFormatType)
{
    if (resultFormat == RESULT_FORMAT_PROTOBUF) {
        resultFormatType = RF_PROTOBUF;
    } else if (resultFormat == RESULT_FORMAT_XML) {
        resultFormatType = RF_XML;
    } else if (resultFormat == RESULT_FORMAT_FB_SUMMARY) {
        resultFormatType = RF_FB_SUMMARY;
    } else {
        return false;
    }
    return true;
}

QrsProcessorPtr QrsSearcherHandler::getQrsProcessorChain(
        const string &chainName, Tracer *tracer)
{
    QrsProcessorPtr qrsProcessorPtr =
        _qrsSearchConfig->_qrsChainMgrPtr->getProcessorChain(chainName);
    if (qrsProcessorPtr == NULL) {
        return QrsProcessorPtr();
    }

    QrsProcessorPtr currentProcessor = qrsProcessorPtr;
    while (currentProcessor) {
        currentProcessor->setTracer(tracer);
        currentProcessor->setRunGraphContext(_runGraphContext);
        currentProcessor->setSessionMetricsCollector(
                _metricsCollectorPtr);
        currentProcessor = currentProcessor->getNextProcessor();
    }
    return qrsProcessorPtr;
}

string QrsSearcherHandler::formatResultString(const RequestPtr &requestPtr,
        const ResultPtr &resultPtr,
        ResultFormatType resultFormatType,
        uint32_t protoFormatOption)
{
    stringstream ss;
    if (resultFormatType == RF_PROTOBUF) {
        PBResultFormatter formatter;
        map<string, string> attrNameMap;
        transAttributeName(requestPtr,attrNameMap);
        formatter.setOption(protoFormatOption);
        formatter.setAttrNameTransMap(attrNameMap);
        formatter.format(resultPtr, ss);
    } else if (resultFormatType == RF_FB_SUMMARY) {
        FBResultFormatter formatter(requestPtr->getPool());
        return formatter.format(resultPtr);
    } else {
        XMLResultFormatter formatter;
        formatter.format(resultPtr, ss);
    }
    return ss.str();
}

void QrsSearcherHandler::transAttributeName(const RequestPtr &requestPtr,
        map<string, string> &attrNameMap)
{
    VirtualAttrConvertor convertor;
    auto virtualAttributeClause = requestPtr->getVirtualAttributeClause();
    if (virtualAttributeClause) {
        convertor.initVirtualAttrs(virtualAttributeClause->getVirtualAttributes());
    }
    AttributeClause *attrCluase = requestPtr->getAttributeClause();
    if (!attrCluase) {
        return;
    }
    const set<string> &attrNames = attrCluase->getAttributeNames();
    for (set<string>::const_iterator it = attrNames.begin();
         it != attrNames.end(); ++it)
    {
        string refName = convertor.nameToName(*it);
        attrNameMap[refName] = *it;
    }
}

bool QrsSearcherHandler::runProcessChain(const string &qrsChainName,
        TracerPtr &tracerPtr, RequestPtr &requestPtr,
        ResultPtr &resultPtr)
{
    QrsProcessorPtr qrsProcessorPtr = getQrsProcessorChain(
            qrsChainName, tracerPtr.get());
    if (NULL == qrsProcessorPtr) {
        _errorResult.resetError(ERROR_QRS_NOT_FOUND_CHAIN,
                                string(", chainName[") + qrsChainName + "]");
        _metricsCollectorPtr->increaseProcessErrorQps();
        return false;
    }
    qrsProcessorPtr->setTimeoutTerminator(_timeoutTerminator);
    int64_t startTime = TimeUtility::currentTime();
    ErrorCode ec = ERROR_NONE;
    string errorMsg;
    try {
        qrsProcessorPtr->process(requestPtr, resultPtr);
    } catch (const std::exception &e) {
        ec = ERROR_QRS_PROCESSOR_EXCEPTION;
        errorMsg = string("exception happened in qrs processor: ") + e.what();
    } catch (...) {
        ec = ERROR_QRS_PROCESSOR_EXCEPTION;
        errorMsg = string("exception happened in qrs processor.");
    }
    if (ec != ERROR_NONE) {
        HA3_LOG(ERROR, "%s", errorMsg.c_str());
        _errorResult.resetError(ec, errorMsg);
        if (!_metricsCollectorPtr->isIncreaseSyntaxErrorQps()) {
            _metricsCollectorPtr->increaseProcessErrorQps();
        }
        return false;
    }
    int64_t endTime = TimeUtility::currentTime();
    _accessLog.setProcessTime((endTime - startTime)/1000);

    // qrs processor can do anything, we should check everything used later
    if ( requestPtr == NULL || resultPtr == NULL
        || requestPtr->getConfigClause() == NULL)
    {
        _metricsCollectorPtr->increaseProcessErrorQps();
        _errorResult.resetError(ERROR_UNKNOWN);
        return false;
    }
    HA3_LOG(DEBUG, "QrsChain Process success");

    resultPtr->setTracer(tracerPtr);
    resultPtr->setErrorHostInfo(_partitionId);
    resultPtr->setTotalTime(endTime - startTime);
    return true;
}


void QrsSearcherHandler::addTraceInfo(const common::ResultPtr &resultPtr)
{
    sap::RpcContextPtr rpcContext;
    if (_runGraphContext->getQueryResource()) {
        auto querySession = _runGraphContext->getQueryResource()->getGigQuerySession();
        if (querySession) {
            rpcContext = querySession->getRpcContext();
        }
    }
    if (!rpcContext) {
        return;
    }
    string traceId = rpcContext->getTraceId();
    _accessLog.setTraceId(traceId);
    const map<string, string> &userDataMap = rpcContext->getUserDataMap();
    string userData;
    for (auto iter = userDataMap.begin(); iter != userDataMap.end(); iter++) {
        if (iter->first == "s") {
            continue;
        }
        userData += iter->first + "=" + iter->second + ";";
    }
    _accessLog.setUserData(userData);
    Tracer *_tracer = resultPtr ? resultPtr->getTracer() : NULL;
    if (!_tracer) {
        return;
    }
    REQUEST_TRACE(TRACE3, "qrs session eagle info:");
    REQUEST_TRACE(TRACE3, "eagle traceid : %s", traceId.c_str());
    REQUEST_TRACE(TRACE3, "eagle userdata : %s", userData.c_str());

    string serviceName = _snapshot->getServiceName();
    string appid = rpcContext->getAppId();
    if (serviceName != "" && appid != "") {
        REQUEST_TRACE(TRACE3, "eagle group : %s, service : %s",
                      serviceName.c_str(), appid.c_str());
    }
}

END_HA3_NAMESPACE(service);
