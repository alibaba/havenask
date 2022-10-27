#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_factory.h"
#include "indexlib/table/table_writer.h"
#include "indexlib/table/table_resource.h"
#include "indexlib/table/merge_policy.h"
#include "indexlib/table/table_merger.h"
#include "indexlib/table/table_reader.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/customized_table_config.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/plugin/plugin_manager.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(plugin);

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, TableFactoryWrapper);

std::string TableFactoryWrapper::TABLE_FACTORY_NAME = "TableFactory";

TableFactoryWrapper::TableFactoryWrapper(const config::IndexPartitionSchemaPtr& schema,
                                         const config::IndexPartitionOptions& options,
                                         const plugin::PluginManagerPtr& pluginManager)
    : mSchema(schema)
    , mOptions(options)
    , mPluginManager(pluginManager)
    , mTableFactory(NULL)
{
    
}

TableFactoryWrapper::~TableFactoryWrapper() 
{
    mTableFactory = NULL;
}

bool TableFactoryWrapper::Init()
{
    if (!mSchema)
    {
        IE_LOG(ERROR, "schema is NULL");
        return false;
    }
    if (!mPluginManager)
    {
        IE_LOG(ERROR, "PluginManager is NULL in table[%s]",
               mSchema->GetSchemaName().c_str());
        return false;
    }
    CustomizedTableConfigPtr customTableConfig = mSchema->GetCustomizedTableConfig();
    if (!customTableConfig)
    {
        IE_LOG(ERROR, "customized_table_config is NULL in Schema[%s]",
               mSchema->GetSchemaName().c_str());
        return false;
    }
    mParameters = customTableConfig->GetParameters();
    string moduleName = customTableConfig->GetPluginName();
    Module* module = mPluginManager->getModule(moduleName);
    if (!module)
    {
        IE_LOG(ERROR, "cannot find module [%s]", moduleName.c_str());
        return false;
    }
    mTableFactory = dynamic_cast<TableFactory*>(
            module->getModuleFactory(TABLE_FACTORY_NAME));
    if (!mTableFactory)
    {
        IE_LOG(ERROR, "get %s from module [%s] fail!",
               TABLE_FACTORY_NAME.c_str(), moduleName.c_str());
        return false;
    }
    return true;
}

TableWriterPtr TableFactoryWrapper::CreateTableWriter() const
{
    assert(mTableFactory);
    return TableWriterPtr(mTableFactory->CreateTableWriter(mParameters));
}

TableResourcePtr TableFactoryWrapper::CreateTableResource() const
{
    assert(mTableFactory);
    return TableResourcePtr(mTableFactory->CreateTableResource(mParameters)); 
}

MergePolicyPtr TableFactoryWrapper::CreateMergePolicy() const
{
    assert(mTableFactory);
    return MergePolicyPtr(mTableFactory->CreateMergePolicy(mParameters)); 
}

TableMergerPtr TableFactoryWrapper::CreateTableMerger() const
{
    assert(mTableFactory);
    return TableMergerPtr(mTableFactory->CreateTableMerger(mParameters)); 
}

TableReaderPtr TableFactoryWrapper::CreateTableReader() const
{
    assert(mTableFactory);
    return TableReaderPtr(mTableFactory->CreateTableReader(mParameters)); 
}

const PluginManagerPtr& TableFactoryWrapper::GetPluginManager() const
{
    return mPluginManager;
}

IE_NAMESPACE_END(table);

