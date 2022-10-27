#include "indexlib/index/normal/primarykey/test/primary_key_formatter_creator_unittest.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PrimaryKeyFormatterCreatorTest);

PrimaryKeyFormatterCreatorTest::PrimaryKeyFormatterCreatorTest()
{
}

PrimaryKeyFormatterCreatorTest::~PrimaryKeyFormatterCreatorTest()
{
}

void PrimaryKeyFormatterCreatorTest::CaseSetUp()
{
    string field = "pk:string:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;"
                   "updatable_multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "string1;";
    
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);
    mPkConfig = DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig,
            schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
}

void PrimaryKeyFormatterCreatorTest::CaseTearDown()
{
}

void PrimaryKeyFormatterCreatorTest::TestCreate()
{
    {
        PrimaryKeyIndexConfigPtr indexConfig(
                (PrimaryKeyIndexConfig*)mPkConfig->Clone());
        indexConfig->SetPrimaryKeyIndexType(pk_sort_array);
        ASSERT_TRUE(DYNAMIC_POINTER_CAST(SortedPrimaryKeyFormatter<uint64_t>,
                        PrimaryKeyFormatterCreator<uint64_t>::CreatePrimaryKeyFormatter(indexConfig)));
        
    }

    {
        // TODO
        // PrimaryKeyIndexConfigPtr indexConfig(
        //         (PrimaryKeyIndexConfig*)mPkConfig->Clone());
        // ASSERT_TRUE(DYNAMIC_POINTER_CAST(UnsortedPrimaryKeyFormatter<uint64_t>,
        //                 PrimaryKeyFormatterCreator<uint64_t>::CreatePrimaryKeyFormatter(indexConfig)));
        
    }

    {
        PrimaryKeyIndexConfigPtr indexConfig(
                (PrimaryKeyIndexConfig*)mPkConfig->Clone());
        indexConfig->SetPrimaryKeyLoadParam(
                PrimaryKeyLoadStrategyParam::HASH_TABLE, false, "");
        ASSERT_TRUE(DYNAMIC_POINTER_CAST(HashPrimaryKeyFormatter<uint64_t>,
                        PrimaryKeyFormatterCreator<uint64_t>::CreatePrimaryKeyFormatter(indexConfig)));
    }   
}

IE_NAMESPACE_END(index);

