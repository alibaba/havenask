#include "indexlib/table/test/broken_table_factory.h"
#include "indexlib/table/demo/demo_table_writer.h"
#include "indexlib/table/demo/demo_table_resource.h"

using namespace std;

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, BrokenTableFactory);

BrokenTableFactory::BrokenTableFactory() 
{
}

BrokenTableFactory::~BrokenTableFactory() 
{
}

TableWriter* BrokenTableFactory::CreateTableWriter(const util::KeyValueMap& parameters)
{
    TableWriter* tableWriter = new DemoTableWriter(parameters);
    return tableWriter;
}

TableResource* BrokenTableFactory::CreateTableResource(const util::KeyValueMap& parameters)
{
    TableResource* tableResource = new DemoTableResource(parameters);
    return tableResource;
}

TableReader* BrokenTableFactory::CreateTableReader(const util::KeyValueMap& parameters)
{
    return NULL;
}

extern "C"
plugin::ModuleFactory* createINVALIDfactoryName() {
    return new BrokenTableFactory();
}

extern "C"
void destroyTableFactory(plugin::ModuleFactory *factory) {
    factory->destroy();
}


IE_NAMESPACE_END(table);

