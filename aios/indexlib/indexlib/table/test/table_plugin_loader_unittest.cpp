#include "indexlib/table/test/table_plugin_loader_unittest.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/customized_table_config.h"
#include "indexlib/config/impl/customized_table_config_impl.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/plugin/Module.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, TablePluginLoaderTest);

TablePluginLoaderTest::TablePluginLoaderTest()
{
}

TablePluginLoaderTest::~TablePluginLoaderTest()
{
}

void TablePluginLoaderTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema = SchemaAdapter::LoadSchema(
        TEST_DATA_PATH,"demo_table_schema.json");
}

void TablePluginLoaderTest::CaseTearDown()
{
}

void TablePluginLoaderTest::TestLoadException()
{
    IndexPartitionOptions options;
    string pluginPath = TEST_DATA_PATH;
    
    // test no CustomizedTableConfig in schema
    IndexPartitionSchemaPtr schema(mSchema->Clone());
    schema->SetCustomizedTableConfig(CustomizedTableConfigPtr());
    ASSERT_FALSE(TablePluginLoader::Load(pluginPath, schema, options));
    
    // test no match plugin
    schema.reset(mSchema->Clone());
    CustomizedTableConfigPtr customizedTableConfig(schema->GetCustomizedTableConfig());
    customizedTableConfig->mImpl->mPluginName = "NOT_EXIST_PLUGIN";
    schema->SetCustomizedTableConfig(customizedTableConfig);
    PluginManagerPtr pluginManager = TablePluginLoader::Load(pluginPath, schema, options);
    ASSERT_TRUE(pluginManager);
    ASSERT_FALSE(pluginManager->getModule("NOT_EXIST_PLUGIN"));
}

void TablePluginLoaderTest::TestLoadNormal()
{
    string pluginPath = TEST_DATA_PATH;

    IndexPartitionSchemaPtr schema(mSchema->Clone());
    IndexPartitionOptions options;
    PluginManagerPtr pluginManager = TablePluginLoader::Load(pluginPath, schema, options);
    ASSERT_TRUE(pluginManager);
    Module* module = pluginManager->getModule("demoTable");
    ASSERT_TRUE(module);
    ASSERT_TRUE(module->getModuleFactory(TableFactoryWrapper::TABLE_FACTORY_NAME));
}

IE_NAMESPACE_END(table);

