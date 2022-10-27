#include "indexlib/table/test/table_factroy_unittest.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, TableFactroyTest);

TableFactroyTest::TableFactroyTest()
{
}

TableFactroyTest::~TableFactroyTest()
{
}

void TableFactroyTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema = SchemaAdapter::LoadSchema(
        TEST_DATA_PATH,"demo_table_schema.json");
}

void TableFactroyTest::CaseTearDown()
{
}

void TableFactroyTest::TestSimpleBuild()
{
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = false;  

    string pluginRoot = TEST_DATA_PATH;
    IndexPartitionResource partitionResource;
    partitionResource.indexPluginPath = pluginRoot;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, true, partitionResource);

    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_FULL, docString, "", ""));

    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(directory->IsExist("segment_0_level_0"));
    ASSERT_TRUE(directory->IsExist("segment_0_level_0/custom/"));
    ASSERT_TRUE(directory->IsExist("segment_0_level_0/segment_info")); 
    ASSERT_TRUE(directory->IsExist("segment_1_level_0"));
    ASSERT_TRUE(directory->IsExist("segment_1_level_0/custom/"));
    ASSERT_TRUE(directory->IsExist("segment_1_level_0/segment_info")); 
    ASSERT_FALSE(directory->IsExist("segment_2_level_0"));
    ASSERT_TRUE(directory->IsExist("version.0"));
}

IE_NAMESPACE_END(table);

