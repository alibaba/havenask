#include <ha3/qrs/test/FakeFillSummaryProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(common);
HA3_LOG_SETUP(qrs, FakeFillSummaryProcessor);

FakeFillSummaryProcessor::FakeFillSummaryProcessor() { 
}

FakeFillSummaryProcessor::~FakeFillSummaryProcessor() { 
}

void FakeFillSummaryProcessor::process(RequestPtr &requestPtr, 
                                       ResultPtr &resultPtr)
{
}

QrsProcessorPtr FakeFillSummaryProcessor::clone() {
    HA3_LOG(TRACE3, "FakeFillSummaryProcessor clone");
    QrsProcessorPtr ptr(new FakeFillSummaryProcessor(*this));
    return ptr;
}

void FakeFillSummaryProcessor::fillSummary(const common::RequestPtr &requestPtr,
        const common::ResultPtr &resultPtr) 
{
    Hits* tmpHits = new Hits;
    tmpHits->setSummaryFilled();
    resultPtr->setHits(tmpHits);
}

END_HA3_NAMESPACE(qrs);

