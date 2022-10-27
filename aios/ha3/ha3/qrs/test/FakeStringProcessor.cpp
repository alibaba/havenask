#include <ha3/qrs/test/FakeStringProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, FakeStringProcessor);

FakeStringProcessor::FakeStringProcessor() { 
}

FakeStringProcessor::~FakeStringProcessor() { 
}

void FakeStringProcessor::process(common::RequestPtr &requestPtr, 
                                  common::ResultPtr &resultPtr) 
{
    HA3_LOG(TRACE3, "process in FakeStringProcessor");
    QrsProcessor::process(requestPtr, resultPtr);
}

QrsProcessorPtr FakeStringProcessor::clone() {
    QrsProcessorPtr ptr(new FakeStringProcessor(*this));
    HA3_LOG(TRACE3, "parameter size: %zd", ptr->getParams().size());
    return ptr;
}

END_HA3_NAMESPACE(qrs);

