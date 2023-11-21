#include "indexlib/common_define.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_reader.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/test/build_test_util.h"

namespace indexlib { namespace index {
class MultiShardingIndexReaderTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    MultiShardingIndexReaderTest() {}
    ~MultiShardingIndexReaderTest() {}

    DECLARE_CLASS_NAME(MultiShardingIndexReaderTest);

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, MultiShardingIndexReaderTest);

TEST_P(MultiShardingIndexReaderTest, TestSimpleProcess)
{
    std::string fields = "name:string;title:text;pk:string";
    std::string indexs = "pk_index:primarykey64:pk;"
                         "str_index:string:name:false:2;"
                         "pack_index:pack:title:false:2";

    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(fields, indexs, "", "");
    config::IndexPartitionOptions options;
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);
    options.GetBuildConfig(true).enablePackageFile = false;
    options.GetBuildConfig(false).enablePackageFile = false;

    test::PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    // full build
    std::string fullDoc = "cmd=add,pk=0,name=yida,title=staff;"
                          "cmd=add,pk=1,name=pujian,title=abc;"
                          "cmd=add,pk=2,name=shoujing,title=def;"
                          "cmd=add,pk=3,name=yexiao,title=hig;"
                          "cmd=add,pk=4,name=pujian,title=rst;";

    std::string rtDoc = "cmd=add,pk=5,name=yida_1,title=staff_11,ts=1;"
                        "cmd=add,pk=6,name=pujian,title=abc_22,ts=2;"
                        "cmd=add,pk=7,name=shoujing_3,title=def_33,ts=3;"
                        "cmd=add,pk=8,name=yexiao_4,title=hig,ts=4;";

    ASSERT_TRUE(psm.Transfer(test::PsmEvent::BUILD_FULL_NO_MERGE, fullDoc, "str_index:pujian", "docid=1;docid=4"));
    ASSERT_TRUE(psm.Transfer(test::PsmEvent::BUILD_RT, rtDoc, "str_index:pujian", "docid=1;docid=4;docid=6"));

    partition::IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(reader);

    // check section reader
    MultiShardingIndexReaderPtr shardingIndexReader =
        DYNAMIC_POINTER_CAST(MultiShardingIndexReader, reader->GetInvertedIndexReader("pack_index"));
    ASSERT_TRUE(shardingIndexReader);

    const SectionAttributeReader* shardingSectionReader =
        shardingIndexReader->GetSectionReader(shardingIndexReader->GetIndexName());
    ASSERT_TRUE(shardingSectionReader);

    ASSERT_EQ((size_t)2, shardingIndexReader->mShardingIndexReaders.size());
    for (size_t i = 0; i < shardingIndexReader->mShardingIndexReaders.size(); ++i) {
        const std::shared_ptr<InvertedIndexReader>& inShardingReader = shardingIndexReader->mShardingIndexReaders[i];

        const SectionAttributeReader* sectionReader =
            inShardingReader->GetSectionReader(inShardingReader->GetIndexName());
        ASSERT_EQ(shardingSectionReader, sectionReader);
    }
}

TEST_P(MultiShardingIndexReaderTest, TestLayerHash)
{
    std::string fields = "name:string;title:text;pk:string;number:int64;";
    std::string indexs = "pk_index:primarykey64:pk;"
                         "str_index:string:name:false:2;"
                         "pack_index:pack:title:false:2;"
                         "number_index:range:number;";

    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(fields, indexs, "", "");
    {
        config::FieldConfigPtr fieldConfig = schema->GetFieldConfig("name");
        fieldConfig->SetUserDefinedParam(util::ConvertToJsonMap({{"dict_hash_func", "LayerHash"}}));
    }
    {
        // pk not affect
        config::FieldConfigPtr fieldConfig = schema->GetFieldConfig("pk");
        fieldConfig->SetUserDefinedParam(util::ConvertToJsonMap({{"dict_hash_func", "LayerHash"}}));
    }
    {
        // range not affect
        config::FieldConfigPtr fieldConfig = schema->GetFieldConfig("number");
        fieldConfig->SetUserDefinedParam(util::ConvertToJsonMap({{"dict_hash_func", "LayerHash"}}));
    }
    config::IndexPartitionOptions options;
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);
    options.GetBuildConfig(true).enablePackageFile = false;
    options.GetBuildConfig(false).enablePackageFile = false;

    test::PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    // full build
    std::string fullDoc = "cmd=add,pk=0,name=yida#0,title=staff,number=0;"
                          "cmd=add,pk=1,name=pujian,title=abc,number=1;"
                          "cmd=add,pk=2,name=shoujing#1,title=def,number=2;"
                          "cmd=add,pk=3,name=yexiao#2,title=hig,number=3;"
                          "cmd=add,pk=4,name=pujian,title=rst,number=1;";

    std::string rtDoc = "cmd=add,pk=5,name=yida#1,title=staff_11,number=0,ts=1;"
                        "cmd=add,pk=6,name=pujian,title=abc_22,number=1,ts=2;"
                        "cmd=add,pk=7,name=shoujing#3,title=def_33,number=2,ts=3;"
                        "cmd=add,pk=8,name=yexiao#4,title=hig,number=3,ts=4;";

    ASSERT_TRUE(psm.Transfer(test::PsmEvent::BUILD_FULL_NO_MERGE, fullDoc, "str_index:pujian", "docid=1;docid=4"));
    ASSERT_TRUE(psm.Transfer(test::PsmEvent::BUILD_RT, rtDoc, "str_index:pujian", "docid=1;docid=4;docid=6"));
    ASSERT_TRUE(psm.Transfer(test::PsmEvent::QUERY, "", "str_index:yida#1", "docid=5"));
    ASSERT_TRUE(
        psm.Transfer(test::PsmEvent::QUERY, "", "number_index:[0,1]", "docid=0;docid=1;docid=4;docid=5;docid=6"));

    partition::IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(reader);

    // check section reader
    MultiShardingIndexReaderPtr shardingIndexReader =
        DYNAMIC_POINTER_CAST(MultiShardingIndexReader, reader->GetInvertedIndexReader("pack_index"));
    ASSERT_TRUE(shardingIndexReader);

    const SectionAttributeReader* shardingSectionReader =
        shardingIndexReader->GetSectionReader(shardingIndexReader->GetIndexName());
    ASSERT_TRUE(shardingSectionReader);

    ASSERT_EQ((size_t)2, shardingIndexReader->mShardingIndexReaders.size());
    for (size_t i = 0; i < shardingIndexReader->mShardingIndexReaders.size(); ++i) {
        const std::shared_ptr<InvertedIndexReader>& inShardingReader = shardingIndexReader->mShardingIndexReaders[i];

        const SectionAttributeReader* sectionReader =
            inShardingReader->GetSectionReader(inShardingReader->GetIndexName());
        ASSERT_EQ(shardingSectionReader, sectionReader);
    }
}

TEST_P(MultiShardingIndexReaderTest, TestCreateKeyIterator)
{
    std::string fields = "name:string;title:text;pk:string";
    std::string indexs = "pk_index:primarykey64:pk;"
                         "str_index:string:name:false:2;"
                         "pack_index:pack:title:false:2";

    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(fields, indexs, "", "");

    config::IndexPartitionOptions options;
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);
    options.GetBuildConfig(true).enablePackageFile = false;
    options.GetBuildConfig(false).enablePackageFile = false;

    test::PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    // full build
    std::string fullDoc = "cmd=add,pk=0,name=yida,title=staff;"
                          "cmd=add,pk=1,name=pujian,title=abc;"
                          "cmd=add,pk=2,name=shoujing,title=def;"
                          "cmd=add,pk=3,name=yexiao,title=hig;"
                          "cmd=add,pk=4,name=pujian,title=rst;";

    ASSERT_TRUE(psm.Transfer(test::PsmEvent::BUILD_FULL_NO_MERGE, fullDoc, "str_index:pujian", "docid=1;docid=4"));

    partition::IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(reader);

    // check section reader
    MultiShardingIndexReaderPtr shardingIndexReader =
        DYNAMIC_POINTER_CAST(MultiShardingIndexReader, reader->GetInvertedIndexReader("pack_index"));
    ASSERT_TRUE(shardingIndexReader);

    std::vector<docid_t> docIds;
    size_t i = 0;
    std::shared_ptr<KeyIterator> keyIter = shardingIndexReader->CreateKeyIterator("");
    while (keyIter->HasNext()) {
        std::string key;
        PostingIterator* iter = keyIter->NextPosting(key);
        ASSERT_TRUE(iter);
        docIds.push_back(iter->SeekDoc(INVALID_DOCID));
        i++;
        IE_POOL_COMPATIBLE_DELETE_CLASS(NULL, iter);
    }
    ASSERT_EQ((size_t)5, i);
    EXPECT_THAT(docIds, UnorderedElementsAre(0, 1, 2, 3, 4));
}

INSTANTIATE_TEST_CASE_P(BuildMode, MultiShardingIndexReaderTest, testing::Values(0, 1, 2));
}} // namespace indexlib::index
