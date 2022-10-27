#include <ha3/qrs/test/FakeRequestProcessor2.h>

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, FakeRequestProcessor2);

FakeRequestProcessor2::FakeRequestProcessor2() { 
}

FakeRequestProcessor2::~FakeRequestProcessor2() { 
}

void FakeRequestProcessor2::process(common::RequestPtr &requestPtr, 
                                   common::ResultPtr &resultPtr) 
{
    HA3_LOG(TRACE3, "process in FakeRequestProcessor2");
    QrsProcessor::process(requestPtr, resultPtr);
}

QrsProcessorPtr FakeRequestProcessor2::clone() {
    QrsProcessorPtr ptr(new FakeRequestProcessor2(*this));
    return ptr;
}

END_HA3_NAMESPACE(qrs);



