#include <ha3/service/test/FakeQrsChainProcessor.h>

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(searcher, FakeQrsChainProcessor);

FakeQrsChainProcessor::FakeQrsChainProcessor() { 
    _requestSet = false;
}

FakeQrsChainProcessor::~FakeQrsChainProcessor() { 
}

END_HA3_NAMESPACE(service);

