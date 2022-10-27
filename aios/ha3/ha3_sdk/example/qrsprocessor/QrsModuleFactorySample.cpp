#include <ha3/qrs/QrsProcessor.h>
#include <ha3_sdk/example/qrsprocessor/QrsModuleFactorySample.h>
#include <ha3_sdk/example/qrsprocessor/QrsProcessorSample.h>
#include <string>


using namespace std;
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(qrs);

QrsModuleFactorySample::QrsModuleFactorySample() { 
}

QrsModuleFactorySample::~QrsModuleFactorySample() { 
}

bool QrsModuleFactorySample::init(const KeyValueMap &factoryParameters) {
    //do initialization according 'factoryParameters' 
    return true;
}

void QrsModuleFactorySample::destroy() {
    //release source
    ;
}

QrsProcessor* QrsModuleFactorySample::createQrsProcessor(const string &processorName) {
    if (processorName ==  "QrsProcessorSample") {
        return new QrsProcessorSample();
    } else {
        //LOG(TRACE3, "Not support this QrsProcessor : [%s]", name.c_str());
        return NULL;
    }
}

extern "C" 
ModuleFactory* createFactory() {
    return new QrsModuleFactorySample;
}

extern "C" 
void destroyFactory(ModuleFactory *factory) {
    delete factory;
}

END_HA3_NAMESPACE(qrs);

