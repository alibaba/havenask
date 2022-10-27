#include "indexlib/table/demo/demo_table_factory.h"
#include "indexlib/table/demo/demo_table_writer.h"
#include "indexlib/table/demo/demo_table_resource.h"
#include "indexlib/table/demo/demo_table_merger.h"
#include "indexlib/table/demo/demo_table_reader.h"
#include "indexlib/table/demo/demo_merge_policy.h"

using namespace std;

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, DemoTableFactory);

DemoTableFactory::DemoTableFactory() 
{
}

DemoTableFactory::~DemoTableFactory() 
{
}

TableWriter* DemoTableFactory::CreateTableWriter(const util::KeyValueMap& parameters)
{
    TableWriter* tableWriter = new DemoTableWriter(parameters);
    return tableWriter;
}

TableResource* DemoTableFactory::CreateTableResource(const util::KeyValueMap& parameters)
{
    TableResource* tableResource = new DemoTableResource(parameters);
    return tableResource;
}

TableMerger* DemoTableFactory::CreateTableMerger(const util::KeyValueMap& parameters)
{
    TableMerger* tableMerger = new DemoTableMerger(parameters);
    return tableMerger;
}

MergePolicy* DemoTableFactory::CreateMergePolicy(const util::KeyValueMap& parameters)
{
    MergePolicy* policy = new DemoMergePolicy(parameters);
    return policy;
}

TableReader* DemoTableFactory::CreateTableReader(const util::KeyValueMap& parameters)
{
    TableReader* reader = new DemoTableReader(parameters);
    return reader;
}

extern "C"
plugin::ModuleFactory* createTableFactory() {
    return new DemoTableFactory();
}

extern "C"
void destroyTableFactory(plugin::ModuleFactory *factory) {
    factory->destroy();
}


IE_NAMESPACE_END(table);

