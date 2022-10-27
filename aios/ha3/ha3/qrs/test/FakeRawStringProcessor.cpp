#include <ha3/qrs/test/FakeRawStringProcessor.h>

using namespace std;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, FakeRawStringProcessor);

FakeRawStringProcessor::FakeRawStringProcessor(const string &retString) { 
    _retString = retString;
}

FakeRawStringProcessor::~FakeRawStringProcessor() { 
}

void FakeRawStringProcessor::process(RequestPtr &requestPtr, 
                                     ResultPtr &resultPtr) 
{
    requestPtr->setOriginalString(_retString);
    QrsProcessor::process(requestPtr, resultPtr);
}

QrsProcessorPtr FakeRawStringProcessor::clone() {
    QrsProcessorPtr ptr(new FakeRawStringProcessor(*this));
    return ptr;    
}

END_HA3_NAMESPACE(qrs);

