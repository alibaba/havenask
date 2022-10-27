#include <ha3/qrs/test/FakeLSDSProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, FakeLSDSProcessor);

FakeLSDSProcessor::FakeLSDSProcessor() { 
}

FakeLSDSProcessor::~FakeLSDSProcessor() { 
}

void FakeLSDSProcessor::process(common::RequestPtr &requestPtr, 
                                   common::ResultPtr &resultPtr) 
{
    HA3_LOG(TRACE3, "process in FakeLSDSProcessor");
    QrsProcessor::process(requestPtr, resultPtr);
}

QrsProcessorPtr FakeLSDSProcessor::clone() {
    QrsProcessorPtr ptr(new FakeLSDSProcessor(*this));
    HA3_LOG(TRACE3, "parameter size: %ld", ptr->getParams().size());
    return ptr;
}

END_HA3_NAMESPACE(qrs);

