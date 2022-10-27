#include "indexlib/index/normal/inverted_index/test/multi_sharding_index_reader_unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiShardingIndexReaderTest);

MultiShardingIndexReaderTest::MultiShardingIndexReaderTest()
{
}

MultiShardingIndexReaderTest::~MultiShardingIndexReaderTest()
{
}

void MultiShardingIndexReaderTest::TestSimpleProcess()
{
    string fields = "name:string;title:text;pk:string";
    string indexs = "pk_index:primarykey64:pk;"
                    "str_index:string:name:false:2;"
                    "pack_index:pack:title:false:2";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(fields, indexs, "", "");

    IndexPartitionOptions options;
    options.GetBuildConfig(true).enablePackageFile = false;
    options.GetBuildConfig(false).enablePackageFile = false;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // full build
    string fullDoc = "cmd=add,pk=0,name=yida,title=staff;"
                     "cmd=add,pk=1,name=pujian,title=abc;"
                     "cmd=add,pk=2,name=shoujing,title=def;"
                     "cmd=add,pk=3,name=yexiao,title=hig;"
                     "cmd=add,pk=4,name=pujian,title=rst;";
    
    string rtDoc = "cmd=add,pk=5,name=yida_1,title=staff_11,ts=1;"
                   "cmd=add,pk=6,name=pujian,title=abc_22,ts=2;"
                   "cmd=add,pk=7,name=shoujing_3,title=def_33,ts=3;"
                   "cmd=add,pk=8,name=yexiao_4,title=hig,ts=4;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc,
                             "str_index:pujian", "docid=1;docid=4"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "str_index:pujian",
                             "docid=1;docid=4;docid=6"));

    partition::IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(reader);

    // check section reader
    MultiShardingIndexReaderPtr shardingIndexReader = DYNAMIC_POINTER_CAST(
            MultiShardingIndexReader, reader->GetIndexReader("pack_index"));
    ASSERT_TRUE(shardingIndexReader);

    const SectionAttributeReader* shardingSectionReader =
        shardingIndexReader->GetSectionReader(
                shardingIndexReader->GetIndexName());
    ASSERT_TRUE(shardingSectionReader);
        
    ASSERT_EQ((size_t)2, shardingIndexReader->mShardingIndexReaders.size());
    for (size_t i = 0; i < shardingIndexReader->mShardingIndexReaders.size(); ++i)
    {
        const IndexReaderPtr& inShardingReader = 
            shardingIndexReader->mShardingIndexReaders[i];

        const SectionAttributeReader* sectionReader =
            inShardingReader->GetSectionReader(inShardingReader->GetIndexName());
        ASSERT_EQ(shardingSectionReader, sectionReader);
    }
}

void MultiShardingIndexReaderTest::TestCreateKeyIterator()
{
    string fields = "name:string;title:text;pk:string";
    string indexs = "pk_index:primarykey64:pk;"
                    "str_index:string:name:false:2;"
                    "pack_index:pack:title:false:2";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(fields, indexs, "", "");

    IndexPartitionOptions options;
    options.GetBuildConfig(true).enablePackageFile = false;
    options.GetBuildConfig(false).enablePackageFile = false;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // full build
    string fullDoc = "cmd=add,pk=0,name=yida,title=staff;"
                     "cmd=add,pk=1,name=pujian,title=abc;"
                     "cmd=add,pk=2,name=shoujing,title=def;"
                     "cmd=add,pk=3,name=yexiao,title=hig;"
                     "cmd=add,pk=4,name=pujian,title=rst;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc,
                             "str_index:pujian", "docid=1;docid=4"));

    partition::IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(reader);

    // check section reader
    MultiShardingIndexReaderPtr shardingIndexReader = DYNAMIC_POINTER_CAST(
            MultiShardingIndexReader, reader->GetIndexReader("pack_index"));
    ASSERT_TRUE(shardingIndexReader);

    vector<docid_t> docIds;
    size_t i = 0;
    KeyIteratorPtr keyIter = shardingIndexReader->CreateKeyIterator("");
    while (keyIter->HasNext())
    {
        string key;
        PostingIterator* iter = keyIter->NextPosting(key);
        ASSERT_TRUE(iter);
        docIds.push_back(iter->SeekDoc(INVALID_DOCID));
        i++;
        IE_POOL_COMPATIBLE_DELETE_CLASS(NULL, iter);
    }
    ASSERT_EQ((size_t)5, i);
    EXPECT_THAT(docIds, UnorderedElementsAre(0, 1, 2, 3, 4));
}

IE_NAMESPACE_END(index);

