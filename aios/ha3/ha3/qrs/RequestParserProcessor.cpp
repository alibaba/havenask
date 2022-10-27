#include <ha3/qrs/RequestParserProcessor.h>
#include <ha3/queryparser/RequestParser.h>

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, RequestParserProcessor);

RequestParserProcessor::RequestParserProcessor(const config::ClusterConfigMapPtr &clusterConfigMapPtr) 
    : _clusterConfigMapPtr(clusterConfigMapPtr)
{ 
}

RequestParserProcessor::~RequestParserProcessor() { 
    
}

void RequestParserProcessor::process(RequestPtr &requestPtr, 
                                     ResultPtr &resultPtr) 
{
    assert(resultPtr != NULL);

    _metricsCollectorPtr->requestParserStartTrigger();
    RequestParser requestParser;
    bool succ = requestParser.parseRequest(requestPtr, _clusterConfigMapPtr);
    if (!succ) {
        HA3_LOG(WARN, "ParseRequset Failed: request[%s]",
                requestPtr->getOriginalString().c_str());
        ErrorResult errorResult = requestParser.getErrorResult();
        resultPtr->getMultiErrorResult()->addError(errorResult.getErrorCode(), 
                errorResult.getErrorMsg());
        _metricsCollectorPtr->requestParserEndTrigger();
        _metricsCollectorPtr->increaseSyntaxErrorQps();
        return;
    }
    
    _metricsCollectorPtr->requestParserEndTrigger();
    FetchSummaryClause *fetchSummaryClause =
        requestPtr->getFetchSummaryClause();
    if (!fetchSummaryClause) {
        ConfigClause *configClause = requestPtr->getConfigClause();
        configClause->setAllowLackSummaryConfig("no");
    }
    QrsProcessor::process(requestPtr, resultPtr);
}

QrsProcessorPtr RequestParserProcessor::clone() {
    QrsProcessorPtr ptr(new RequestParserProcessor(*this));
    return ptr;
}

END_HA3_NAMESPACE(qrs);

