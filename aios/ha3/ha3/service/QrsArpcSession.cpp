#include <ha3/service/QrsArpcSession.h>
#include <ha3/common/ErrorResult.h>
#include <ha3/common/XMLResultFormatter.h>
#include <ha3/service/QrsSearcherHandler.h>
#include <ha3/turing/qrs/QrsBiz.h>
#include <ha3/turing/qrs/QrsRunGraphContext.h>
#include <suez/turing/search/SearchContext.h>

using namespace std;
using namespace kmonitor;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(turing);
BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, QrsArpcSession);

QrsArpcSession::QrsArpcSession(SessionPool* pool)
    : Session(pool)
{}

QrsArpcSession::~QrsArpcSession() {
    reset();
}

void QrsArpcSession::dropRequest() {
    QrsSessionSearchResult result("Server is too busy, thread pool queue is full\n");
    endQrsSession(result);
}
void QrsArpcSession::reset() {
    _snapshot.reset();
    _qrsResponse = NULL;
    _done = NULL;
    _queryStr.clear();
    _clientAddress.clear();
	_bizName.clear();
    _timeout = -1;
    POOL_DELETE_CLASS(_timeoutTerminator);
    _timeoutTerminator = NULL;
    recycleRunGraphResource();
	_useFirstBiz = true;
    Session::reset();
}

bool QrsArpcSession::beginSession() {
    if (!Session::beginSession()) {
        return false;
    }
    if (_snapshot == nullptr) {
        HA3_LOG(WARN, "snapshot not setted");
        return false;
    }
	_bizName = getBizName(_bizName);
    // bizName not work, if not found, return first biz in map
	turing::QrsBizPtr qrsBiz = dynamic_pointer_cast<turing::QrsBiz>(_snapshot->getBiz(_bizName));
	if (qrsBiz == nullptr && _useFirstBiz) {
		qrsBiz = _snapshot->getFirstQrsBiz();
	}
    if (!qrsBiz) {
        HA3_LOG(WARN, "get qrs biz failed");
        return false;
    }
	_bizName = qrsBiz->getBizNameInSnapshot();
    QrsRunGraphContextArgs args = qrsBiz->getQrsRunGraphContextArgs();
    _runId = _runIdAllocator->get();
    _pool = _poolCache->get();
    args.runOptions.mutable_run_id()->set_value(_runId);
    args.runOptions.set_inter_op_thread_pool(-1);
    args.pool = _pool;
    _runGraphContext.reset(new QrsRunGraphContext(args));
    auto queryResource = qrsBiz->prepareQueryResource(_pool);
    queryResource->setGigQuerySession(_gigQuerySession);
    auto sessionResource = qrsBiz->getSessionResource();
    if(!sessionResource->addQueryResource(_runId, queryResource)) {
        HA3_LOG(WARN, "add query resource failed, runid = %ld", _runId);
        return false;
    }
    _runGraphContext->setQueryResource(queryResource.get());
    return true;
}

void QrsArpcSession::processRequest() {
    HA3_LOG(DEBUG, "QrsArpcSession::handleSearchClientRequest");
    if (!beginSession()) {
        string errorMsg = "Qrs begin session failed!";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        ErrorResult errorResult(ERROR_BEGIN_SESSION_FAILED, errorMsg);
        string errorXMLMsg = XMLResultFormatter::xmlFormatErrorResult(errorResult);
        QrsSessionSearchResult qrsSessionSearchResult(errorXMLMsg);
        endQrsSession(qrsSessionSearchResult);
        return;
    }

    int64_t currentTime = autil::TimeUtility::currentTime();
    if (!initTimeoutTerminator(currentTime)) {
        string errorMsg = "Qrs wait in queue timeout!";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        ErrorResult errorResult(ERROR_QRS_OUTPOOLQUEUE_TIMEOUT, errorMsg);
        string errorXMLMsg = XMLResultFormatter::xmlFormatErrorResult(errorResult);
        QrsSessionSearchResult qrsSessionSearchResult(errorXMLMsg);
        endQrsSession(qrsSessionSearchResult);
        return;
    }
    assert(_timeoutTerminator);
    QrsSessionSearchRequest request(_queryStr, _clientAddress,
                                    _pool, _sessionMetricsCollectorPtr);
    request.sessionSrcType = _sessionSrcType;
    QrsSessionSearchResult result;
    {
        const string& partitionIdStr = ProtoClassUtil::partitionIdToString(_snapshot->getPid());
        QrsSearcherHandler searcherHandler(partitionIdStr, _snapshot, _runGraphContext, _timeoutTerminator);
        string bizName = getBizName(_bizName);
        try {
            //result = searcherHandler.process(request, _protocolType, bizName);
			result = searcherHandler.process(request, _protocolType, _bizName);

        } catch (const std::exception& e) {
            HA3_LOG(ERROR, "EXCEPTION: %s", e.what());
            result = QrsSessionSearchResult(
                    XMLResultFormatter::xmlFormatErrorResult(ErrorResult(ERROR_UNKNOWN, e.what())));
        } catch (...) {
            HA3_LOG(ERROR, "UnknownException");
            result = QrsSessionSearchResult(
                    XMLResultFormatter::xmlFormatErrorResult(ErrorResult(ERROR_UNKNOWN, "UnknownException")));
        }
    }
    endQrsSession(result);
}

// in failSession zoneResource and bizInfoManager is null ptr, should be checked!
void QrsArpcSession::endQrsSession(QrsSessionSearchResult &result) {
    Session::endSession();
    fillResponse(result.resultStr, result.resultCompressType,
                 result.formatType, _qrsResponse);

    MetricsReporterPtr metricsReporter;
    MetricsReporterPtr oldMetricsReporter;
    if (_snapshot) {
        string bizName = getBizName(_bizName);
        // bizName not work, if not found, return first biz in map
        // turing::QrsBizPtr qrsBiz = dynamic_pointer_cast<turing::QrsBiz>(_snapshot->getBiz(bizName));
		turing::QrsBizPtr qrsBiz = dynamic_pointer_cast<turing::QrsBiz>(_snapshot->getBiz(_bizName));
        if (qrsBiz) {
            string srcStr = result.srcStr.empty() ? "empty" : result.srcStr;
            map<string, string> tagsMap = {
                {"src", srcStr},
                {"session_src", transSessionSrcType(_sessionSrcType)}
            };
            suez::turing::SearchContext::addGigMetricTags(_gigQuerySession, tagsMap, _useGigSrc);
            MetricsTags tags(tagsMap);
            metricsReporter = qrsBiz->getQueryMetricsReporter(tags, "kmon");
            oldMetricsReporter = qrsBiz->getQueryMetricsReporter({});
        }
    }
    reportMetrics(metricsReporter.get());
    reportMetrics(oldMetricsReporter.get());
    if (_gigQuerySession) {
        auto rpcContext = _gigQuerySession->getRpcContext();
        if (rpcContext.get()) {
            rpcContext->setRespSize(_qrsResponse->ByteSize());
            if (result.errorStr.empty()) {
                rpcContext->serverEnd(sap::RpcContext::SUCCESS, "");
            } else {
                rpcContext->serverEnd(sap::RpcContext::FAILED, result.errorStr);
            }
        }
    }
    if (_done) {
        _done->setErrorCode(result.multicallEc);
        _done->Run();
    }
    destroy();
}


void QrsArpcSession::recycleRunGraphResource() {
    _runGraphContext.reset();
    if (_poolCache && _pool) {
        _poolCache->put(_pool);
        _poolCache.reset();
        _pool = nullptr;
    }
    if (_runIdAllocator && _runId >= 0) {
        _runIdAllocator->put(_runId);
        _runIdAllocator.reset();
        _runId = -1;
    }
}

void QrsArpcSession::reportMetrics(MetricsReporter *metricsReporter) {
    if (_sessionMetricsCollectorPtr && metricsReporter) {
        metricsReporter->report<QrsBizMetrics, SessionMetricsCollector>(nullptr,
                _sessionMetricsCollectorPtr.get());
    }
}

void QrsArpcSession::fillResponse(const std::string &result,
                                  HaCompressType compressType,
                                  ResultFormatType formatType,
                                  proto::QrsResponse *response)
{
    std::string compressedResult;
    bool ret = false;
    if (NO_COMPRESS != compressType && _pool) {
        _sessionMetricsCollectorPtr->resultCompressStartTrigger();
        ret = util::HaCompressUtil::compress(result,
                compressType, compressedResult, _pool);
        _sessionMetricsCollectorPtr->resultCompressEndTrigger();

        if (ret) {
            _sessionMetricsCollectorPtr->setIsCompress(true);
            _sessionMetricsCollectorPtr->resultCompressLengthTrigger(compressedResult.size());
            response->set_compresstype(proto::ProtoClassUtil::convertCompressType(
                            compressType));
            response->set_assemblyresult(compressedResult);
        }
    }

    if (response) {
        if (NO_COMPRESS == compressType || !ret) {
            response->set_compresstype(proto::CT_NO_COMPRESS);
            response->set_assemblyresult(result);
        }
        response->set_formattype(convertFormatType(formatType));
    }
}

void QrsArpcSession::setDropped() {
    if (_sessionMetricsCollectorPtr) {
        _sessionMetricsCollectorPtr->increaseProcessErrorQps();
    }
}

bool QrsArpcSession::initTimeoutTerminator(int64_t currentTime) {
    if (_timeout >= 0 && currentTime - getStartTime() >= _timeout) {
        HA3_LOG(WARN, "wait in pool timeout, start time [%ld], current time [%ld], "
                "rpc timeout [%ld] us", getStartTime(), currentTime, _timeout);
        return false;
    }
    if (_timeoutTerminator) {
        POOL_DELETE_CLASS(_timeoutTerminator);
        _timeoutTerminator = NULL;
    }
    _timeoutTerminator = POOL_NEW_CLASS(_pool, TimeoutTerminator, _timeout, getStartTime());
    _timeoutTerminator->init(1);
    HA3_LOG(DEBUG, "init timeout terminator, start time [%ld], current time [%ld], "
                "rpc timeout [%ld] us", getStartTime(), currentTime, _timeout);
    return true;
}

string QrsArpcSession::getBizName(const std::string &bizName) {
    string ret = "";
    if (bizName.empty()) {
        ret = _snapshot->getBizName(DEFAULT_BIZ_NAME);
    } else {
        ret = _snapshot->getBizName(bizName);
    }
    return ret;
}

END_HA3_NAMESPACE(service);
