#include "indexlib/table/test/table_factory_wrapper_unittest.h"
#include "indexlib/table/table_plugin_loader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/customized_table_config.h"
#include "indexlib/config/impl/customized_table_config_impl.h"
#include "indexlib/plugin/plugin_manager.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, TableFactoryWrapperTest);

TableFactoryWrapperTest::TableFactoryWrapperTest()
{
}

TableFactoryWrapperTest::~TableFactoryWrapperTest()
{
}

void TableFactoryWrapperTest::CaseSetUp()
{
    mSchema = SchemaAdapter::LoadSchema(
        TEST_DATA_PATH,"demo_table_schema.json");
}

void TableFactoryWrapperTest::CaseTearDown()
{
}

void TableFactoryWrapperTest::TestInitException()
{
    IndexPartitionOptions options;
    string pluginPath = TEST_DATA_PATH;
    IndexPartitionSchemaPtr schema(mSchema->Clone());
    CustomizedTableConfigPtr customizedTableConfig(schema->GetCustomizedTableConfig());
    customizedTableConfig->mImpl->mPluginName = "broken_factory";
    
    PluginManagerPtr pluginManager = TablePluginLoader::Load(pluginPath, schema, options);
    ASSERT_TRUE(pluginManager);
    
    TableFactoryWrapper tableFactoryWrapper(schema, options, pluginManager);
    ASSERT_FALSE(tableFactoryWrapper.Init());
}

void TableFactoryWrapperTest::TestInitNormal()
{
    IndexPartitionOptions options;
    string pluginPath = TEST_DATA_PATH;
    PluginManagerPtr pluginManager = TablePluginLoader::Load(pluginPath, mSchema, options);
    ASSERT_TRUE(pluginManager);
    TableFactoryWrapper tableFactoryWrapper(mSchema, options, pluginManager);
    ASSERT_TRUE(tableFactoryWrapper.Init()); 
}

IE_NAMESPACE_END(table);

