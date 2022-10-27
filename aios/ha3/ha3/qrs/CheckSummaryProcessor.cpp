#include <ha3/qrs/CheckSummaryProcessor.h>
#include <ha3/common/Request.h>

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, CheckSummaryProcessor);

CheckSummaryProcessor::CheckSummaryProcessor() { 
}

CheckSummaryProcessor::~CheckSummaryProcessor() { 
}

QrsProcessorPtr CheckSummaryProcessor::clone() {
    QrsProcessorPtr ptr(new CheckSummaryProcessor(*this));
    return ptr;
}

void CheckSummaryProcessor::process(RequestPtr &requestPtr,
                                    ResultPtr &resultPtr)
{
    QrsProcessor::process(requestPtr, resultPtr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    assert(configClause);
    if (configClause->isNoSummary()) {
        Hits *hits = resultPtr->getHits();
        if (hits) {
            hits->setNoSummary(true);
            hits->setIndependentQuery(true);
        }
    } else {
        QrsProcessor::fillSummary(requestPtr, resultPtr);
    }
}

END_HA3_NAMESPACE(qrs);

