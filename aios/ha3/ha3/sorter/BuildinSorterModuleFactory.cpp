#include <ha3/sorter/BuildinSorterModuleFactory.h>
#include <ha3/sorter/DefaultSorter.h>
#include <ha3/sorter/NullSorter.h>

using namespace std;

BEGIN_HA3_NAMESPACE(sorter);
HA3_LOG_SETUP(sorter, BuildinSorterModuleFactory);

BuildinSorterModuleFactory::BuildinSorterModuleFactory() { 
}

BuildinSorterModuleFactory::~BuildinSorterModuleFactory() { 
}

Sorter *BuildinSorterModuleFactory::createSorter(const char *sorterName,
        const KeyValueMap &sorterParameters,
        suez::ResourceReader *resourceReader)
{
    if (std::string(sorterName) == "DefaultSorter") {
        return new DefaultSorter();
    }
    if (std::string(sorterName) == "NullSorter") {
        return new NullSorter();
    }
    return NULL;
}

extern "C"
build_service::plugin::ModuleFactory* createFactory_Sorter() {
    return new BuildinSorterModuleFactory();
}

extern "C"
void destroyFactory_Sorter(build_service::plugin::ModuleFactory *factory) {
    factory->destroy();
}

END_HA3_NAMESPACE(sorter);

