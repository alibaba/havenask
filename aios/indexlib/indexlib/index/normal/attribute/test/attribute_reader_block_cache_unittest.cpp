#include "indexlib/index/normal/attribute/test/attribute_reader_block_cache_unittest.h"
#include <autil/StringUtil.h>
#include "indexlib/test/schema_maker.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeReaderBlockCacheTest);

AttributeReaderBlockCacheTest::AttributeReaderBlockCacheTest()
{
}

AttributeReaderBlockCacheTest::~AttributeReaderBlockCacheTest()
{
}

void AttributeReaderBlockCacheTest::CaseSetUp()
{
    string field = "pk:uint64:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;"
                   "updatable_multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "string1;";
    mSchema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);
    mRootDir = GET_TEST_DATA_PATH();
}

void AttributeReaderBlockCacheTest::CaseTearDown()
{
}

void AttributeReaderBlockCacheTest::TestSingleValue()
{
    string jsonStr = "                                                \
    {                                                                   \
    \"load_config\" :                                                   \
    [                                                                   \
    {                                                                   \
        \"file_patterns\" : [\"_ATTRIBUTE_\"],                          \
        \"load_strategy\" : \"mmap\"                                   \
    }                                                                   \
    ]                                                                   \
    }                                                                   \
    ";
    IndexPartitionOptions options;
    FromJsonString(options.GetOnlineConfig().loadConfigList, jsonStr);

    string docString = "cmd=add,pk=1,long1=1";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pk:1", "docid=0,long1=1;"));
}

IE_NAMESPACE_END(index);

