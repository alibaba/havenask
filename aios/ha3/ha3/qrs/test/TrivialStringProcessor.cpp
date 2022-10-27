#include <ha3/qrs/test/TrivialStringProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, TrivialStringProcessor);

TrivialStringProcessor::TrivialStringProcessor() { 
}

TrivialStringProcessor::~TrivialStringProcessor() { 

}
void TrivialStringProcessor::init(const KeyValueMap &keyValues) {
}

void TrivialStringProcessor::process(RequestPtr &requestPtr, 
                                     ResultPtr &resultPtr) 
{
    
}

QrsProcessorPtr TrivialStringProcessor::clone() {
    QrsProcessorPtr ptr(new TrivialStringProcessor(*this));
    return ptr;
}

END_HA3_NAMESPACE(qrs);

