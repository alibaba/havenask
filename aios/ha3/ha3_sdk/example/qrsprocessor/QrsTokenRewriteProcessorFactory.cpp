#include "QrsTokenRewriteProcessorFactory.h"
#include "QrsTokenRewriteProcessor.h"
#include <ha3/qrs/QrsProcessor.h>
#include <string>


using namespace std;
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, QrsTokenRewriteProcessorFactory);

QrsTokenRewriteProcessorFactory::QrsTokenRewriteProcessorFactory() { 
}

QrsTokenRewriteProcessorFactory::~QrsTokenRewriteProcessorFactory() { 
}

bool QrsTokenRewriteProcessorFactory::init(const KeyValueMap &factoryParameters) {
    return true;
}

void QrsTokenRewriteProcessorFactory::destroy() {
    ;
}

QrsProcessor* QrsTokenRewriteProcessorFactory::createQrsProcessor(const string &name) {
    QrsProcessor* processor = NULL;
    processor = new QrsTokenRewriteProcessor();
    return processor;
}

extern "C" 
ModuleFactory* createFactory() {
    return new QrsTokenRewriteProcessorFactory;
}

extern "C" 
void destroyFactory(ModuleFactory *factory) {
    delete factory;
}

END_HA3_NAMESPACE(qrs);

