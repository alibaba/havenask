#include <ha3_sdk/example/qrsprocessor/QrsProcessorSample.h>

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);
//LOG_SETUP(qrs, QrsProcessorSample);

QrsProcessorSample::QrsProcessorSample() 
{ 
}

QrsProcessorSample::~QrsProcessorSample() { 
}

QrsProcessorPtr QrsProcessorSample::clone() {
    QrsProcessorPtr ptr(new QrsProcessorSample(*this));
    return ptr;
}

void QrsProcessorSample::process(RequestPtr &requestPtr, 
                                 ResultPtr &resultPtr) 
{
    /* do something before doing search from 'proxy' */
    //doSomething(requestPtr, resultPtr);
    
    /* call QrsProcessor::process() function to search. */
    QrsProcessor::process(requestPtr, resultPtr);

    /* do other things after doing search to 'proxy' */
    //doOtherthing(requestPtr, resultPtr);
    

    /* ATTENTION: you can do searching again if you think the 'Result' is not satisfied. */
    //rewriteRequest(requestPtr);
    //QrsProcessor::process(requestPtr, resultPtr);
}

END_HA3_NAMESPACE(qrs);

