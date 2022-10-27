#include <ha3/qrs/test/FakeQrsModuleFactory.h>
#include <ha3/qrs/test/FakeStringProcessor.h>
#include <ha3/qrs/test/FakeRequestProcessor.h>
#include <ha3/qrs/test/FakeRequestProcessor2.h>
#include <ha3/qrs/test/KeywordFilterProcessor.h>
#include <ha3/qrs/test/KeywordReplaceProcessor.h>
#include <ha3/qrs/test/FakeLSDSProcessor.h>
#include <ha3/qrs/QrsProcessor.h>
#include <build_service/plugin/ModuleFactory.h>
#include <string>


using namespace std;
using namespace build_service::plugin;

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, FakeQrsModuleFactory);

FakeQrsModuleFactory::FakeQrsModuleFactory() { 
}

FakeQrsModuleFactory::~FakeQrsModuleFactory() { 
}

bool FakeQrsModuleFactory::init(const KeyValueMap &factoryParameters) {
    return true;
}

void FakeQrsModuleFactory::destroy() {
    ;
}

QrsProcessor* FakeQrsModuleFactory::createQrsProcessor(const string &name) {
    QrsProcessor* processor = NULL;
    if (0 == strcasecmp(name.c_str(), "FakeStringProcessor")) {
        processor = new FakeStringProcessor();
    } else if (0==strcasecmp(name.c_str(), "FakeRequestProcessor")) {
        processor = new FakeRequestProcessor();
    } else if (0==strcasecmp(name.c_str(), "FakeRequestProcessor2")) {
        processor = new FakeRequestProcessor2();
    } else if (0==strcasecmp(name.c_str(), "FakeLSDSProcessor")) {
        processor = new FakeLSDSProcessor();
    } else if (0==strcasecmp(name.c_str(), "KeywordFilterProcessor")) {
        processor = new KeywordFilterProcessor();
    } else if (0==strcasecmp(name.c_str(), "KeywordReplaceProcessor")) {
        processor = new KeywordReplaceProcessor();
    } else {
        HA3_LOG(TRACE3, "Not support this QrsProcessor : [%s]", name.c_str());
    }
    return processor;
}

extern "C" 
ModuleFactory* createFactory() {
    return new FakeQrsModuleFactory;
}

extern "C" 
void destroyFactory(ModuleFactory *factory) {
    delete factory;
}

END_HA3_NAMESPACE(qrs);

