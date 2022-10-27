#include <ha3/qrs/test/FakeRequestProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, FakeRequestProcessor);

FakeRequestProcessor::FakeRequestProcessor() { 
}

FakeRequestProcessor::~FakeRequestProcessor() { 
}

void FakeRequestProcessor::process(common::RequestPtr &requestPtr, 
                                   common::ResultPtr &resultPtr) 
{
    HA3_LOG(TRACE3, "process in FakeRequestProcessor1");
    QrsProcessor::process(requestPtr, resultPtr);
}

QrsProcessorPtr FakeRequestProcessor::clone() {
    QrsProcessorPtr ptr(new FakeRequestProcessor(*this));
    return ptr;
}

END_HA3_NAMESPACE(qrs);

