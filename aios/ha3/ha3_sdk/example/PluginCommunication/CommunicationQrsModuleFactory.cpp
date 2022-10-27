#include <ha3/qrs/QrsProcessor.h>
#include "CommunicationQrsModuleFactory.h"
#include "CommunicationQrsProcessor.h"
#include <string>


using namespace std;
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(qrs);

CommunicationQrsModuleFactory::CommunicationQrsModuleFactory() { 
}

CommunicationQrsModuleFactory::~CommunicationQrsModuleFactory() { 
}

bool CommunicationQrsModuleFactory::init(const KeyValueMap &factoryParameters) {
    //do initialization according 'factoryParameters' 
    return true;
}

void CommunicationQrsModuleFactory::destroy() {
    //release source
    ;
}

QrsProcessor* CommunicationQrsModuleFactory::createQrsProcessor(const string &processorName) {
    if (processorName ==  "CommunicationQrsProcessor") {
        return new CommunicationQrsProcessor();
    } else {
        //LOG(TRACE3, "Not support this QrsProcessor : [%s]", name.c_str());
        return NULL;
    }
}

extern "C" 
ModuleFactory* createFactory() {
    return new CommunicationQrsModuleFactory;
}

extern "C" 
void destroyFactory(ModuleFactory *factory) {
    delete factory;
}

END_HA3_NAMESPACE(qrs);

