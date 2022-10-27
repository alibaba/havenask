#include "CheatSorterFactory.h"
#include "CheatSorter.h"

using namespace std;

using namespace build_service::plugin;

BEGIN_HA3_NAMESPACE(sorter);
HA3_LOG_SETUP(sorter, CheatSorterFactory);

CheatSorterFactory::CheatSorterFactory() {
}

CheatSorterFactory::~CheatSorterFactory() {
}

bool CheatSorterFactory::init(const KeyValueMap&) {
    return true;
}

Sorter* CheatSorterFactory::createSorter(const char *sorterName, 
        const KeyValueMap &parameters, config::ResourceReader *resourceReader)
{
    string sorterNameStr(sorterName);
    if (sorterNameStr == "CheatSorter") {
        return new CheatSorter(sorterNameStr);
    }
    return NULL;
}

void CheatSorterFactory::destroy() {
    delete this;
}

extern "C" 
ModuleFactory* createFactory() {
    return new CheatSorterFactory;
}

extern "C" 
void destroyFactory(ModuleFactory *factory) {
    factory->destroy();
}

END_HA3_NAMESPACE(sorter);
