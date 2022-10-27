#include <ha3/sorter/test/SorterModuleFactoryForTest.h>

BEGIN_HA3_NAMESPACE(sorter);
HA3_LOG_SETUP(sorter, SorterModuleFactoryForTest);

SorterModuleFactoryForTest::SorterModuleFactoryForTest() { 
}

SorterModuleFactoryForTest::~SorterModuleFactoryForTest() { 
}

suez::turing::Sorter* SorterModuleFactoryForTest::createSorter(const char *sorterName, 
        const KeyValueMap &parameters, 
        suez::ResourceReader *resourceReader)
{
    if (std::string(sorterName) != "not_exist") {
        return new FakeSorter(sorterName);
    }
    return NULL;
}

extern "C" 
build_service::plugin::ModuleFactory* createFactory() {
    return new SorterModuleFactoryForTest(); 
}

extern "C" 
void destroyFactory(build_service::plugin::ModuleFactory *factory) {
    delete factory;
}

END_HA3_NAMESPACE(sorter);

