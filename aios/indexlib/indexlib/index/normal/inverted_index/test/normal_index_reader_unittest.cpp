#include <autil/StringUtil.h>
#include <fslib/fs/FileSystem.h>
#include <autil/mem_pool/Pool.h>
#include "indexlib/index/normal/inverted_index/test/normal_index_reader_unittest.h"
#include "indexlib/common/in_mem_file_writer.h"
#include "indexlib/common/term.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/inverted_index/test/normal_index_reader_helper.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/fake_partition_data_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, NormalIndexReaderTest);

NormalIndexReaderTest::NormalIndexReaderTest()
{
    mBitmapIndexReader = NULL;
}

NormalIndexReaderTest::~NormalIndexReaderTest()
{
}

void NormalIndexReaderTest::CaseSetUp()
{
    mIndexReader.SetMultiFieldIndexReader(&mMultiFieldIndexReader);
    EXPECT_CALL(mIndexReader, GetSegmentPosting(_, _, _))
        .WillRepeatedly(Return(false));

    mTruncateIndexReader.reset(new MockNormalIndexReader);
    mIndexReaderPtr = mTruncateIndexReader;
    EXPECT_CALL(*mTruncateIndexReader, GetSegmentPosting(_, _, _))
        .WillRepeatedly(Return(false));
    EXPECT_CALL(mMultiFieldIndexReader, GetIndexReader(_))
        .WillRepeatedly(ReturnRef(mIndexReaderPtr));

    DELETE_AND_SET_NULL(mBitmapIndexReader);
    mBitmapIndexReader = new MockBitmapIndexReader;
    EXPECT_CALL(*mBitmapIndexReader, GetSegmentPosting(_, _, _))
        .WillRepeatedly(Return(false));
    mIndexReader.SetBitmapIndexReader(mBitmapIndexReader);

    mRootDir = GET_TEST_DATA_PATH();
}

void NormalIndexReaderTest::CaseTearDown()
{
}

void NormalIndexReaderTest::innerTestIndexWithReferenceCompress(
        docid_t docCount, docid_t realtimeDocCount)
{
    TearDown();
    SetUp();

    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    SingleFieldPartitionDataProvider provider(options);
    provider.Init(GET_TEST_DATA_PATH(), "text", SFP_INDEX);
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetDictInlineCompress(true);
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE);//TODO: delete
    indexConfig->SetIsReferenceCompress(true);

    string docStr;
    for (docid_t i = 0; i < docCount; ++i)
    {
        docStr += "A B,";
    }
    provider.Build(docStr, SFP_OFFLINE);

    if (realtimeDocCount)
    {
        string docStr;
        for (docid_t i = 0; i < realtimeDocCount; ++i)
        {
            docStr += "a b,";
        }
        provider.Build(docStr, SFP_REALTIME);
    }

    PartitionDataPtr partitionData = provider.GetPartitionData();
    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);

    if (docCount)
    {
        Term term("A", "index");
        PostingIterator* iter = reader.Lookup(term);
        ASSERT_TRUE(iter);
        for (docid_t i = 0; i < docCount; i++)
        {
            EXPECT_EQ((docid_t)i, iter->SeekDoc(i));
        }
        ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(docCount));
        delete iter;
        iter = NULL;
    }
    if (realtimeDocCount)
    {
        docid_t rtBaseDocId = docCount;
        Term term("a", "index");
        PostingIterator* iter = reader.Lookup(term);
        ASSERT_TRUE(iter);
        for (docid_t i = 0; i < realtimeDocCount; i++)
        {
            EXPECT_EQ((docid_t)i + rtBaseDocId, iter->SeekDoc(i + rtBaseDocId));
        }
        ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(realtimeDocCount + rtBaseDocId));
        delete iter;
        iter = NULL;
    }
}

void NormalIndexReaderTest::TestFillTruncSegmentPostingWithTruncAndMain()
{
    PrepareSegmentPosting(*mTruncateIndexReader, TermMeta(1, 1, 1));
    TermMeta termMeta(100, 100, 100);
    PrepareSegmentPosting(mIndexReader, termMeta);

    SegmentPosting resultSegPosting(mPostingFormatOption);
    bool ret = mIndexReader.FillTruncSegmentPosting(Term("", "index", "trunc"),
            1, 1, resultSegPosting);
    ASSERT_TRUE(ret);

    SegmentPosting truncSegPosting(mPostingFormatOption);
    mTruncateIndexReader->GetSegmentPosting(1, 1, truncSegPosting);
    truncSegPosting.SetMainChainTermMeta(termMeta);
    ASSERT_EQ(truncSegPosting, resultSegPosting);
}

void NormalIndexReaderTest::TestFillTruncSegmentPostingWithTruncAndNoMain()
{
    PrepareSegmentPosting(*mTruncateIndexReader, TermMeta(1, 1, 1));

    //TODO: bitmap should exist
    SegmentPosting resultSegPosting(mPostingFormatOption);
    bool ret = mIndexReader.FillTruncSegmentPosting(Term("", "index", "trunc"),
            1, 1, resultSegPosting);
    ASSERT_TRUE(!ret);

    TermMeta termMeta(100, 100, 100);
    SegmentPosting bitmapSegPosting = CreateSegmentPosting(termMeta);
    EXPECT_CALL(*mBitmapIndexReader, GetSegmentPosting(_, _, _))
        .WillRepeatedly(DoAll(SetArgReferee<2>(bitmapSegPosting), Return(true)));

    ret = mIndexReader.FillTruncSegmentPosting(Term("", "index", "trunc"),
            1, 1, resultSegPosting);
    ASSERT_TRUE(ret);

    SegmentPosting truncSegPosting(mPostingFormatOption);
    mTruncateIndexReader->GetSegmentPosting(1, 1, truncSegPosting);
    truncSegPosting.SetMainChainTermMeta(termMeta);
    ASSERT_EQ(truncSegPosting, resultSegPosting);
}

void NormalIndexReaderTest::TestFillTruncSegmentPostingWithNoTruncAndMain()
{
    TermMeta termMeta(100, 100, 100);
    PrepareSegmentPosting(mIndexReader, termMeta);

    SegmentPosting mainSegPosting(mPostingFormatOption);
    mIndexReader.GetSegmentPosting(1, 1, mainSegPosting);

    SegmentPosting resultSegPosting(mPostingFormatOption);
    bool ret = mIndexReader.FillTruncSegmentPosting(Term("", "index", "trunc"),
            1, 1, resultSegPosting);
    ASSERT_TRUE(ret);

    mainSegPosting.SetMainChainTermMeta(termMeta);
    ASSERT_EQ(mainSegPosting, resultSegPosting);
}

void NormalIndexReaderTest::TestFillTruncSegmentPostingWithNoTruncAndNoMain()
{
    SegmentPosting resultSegPosting(mPostingFormatOption);
    bool ret = mIndexReader.FillTruncSegmentPosting(Term("", "index", "trunc"),
            1, 1, resultSegPosting);
    ASSERT_FALSE(ret);
}

void NormalIndexReaderTest::PrepareSegmentPosting(MockNormalIndexReader &indexReader,
                                                  const TermMeta &termMeta)
{
    SegmentPosting segPosting = CreateSegmentPosting(termMeta);
    EXPECT_CALL(indexReader, GetSegmentPosting(_, _, _))
        .WillRepeatedly(DoAll(SetArgReferee<2>(segPosting), Return(true)));
}

index::SegmentPosting NormalIndexReaderTest::CreateSegmentPosting(
            const index::TermMeta &termMeta)
{
    InMemFileWriterPtr fileWriter(new InMemFileWriter);
    fileWriter->Init(100);

    //TODO: at first, we must write 4 bytes to fileWrite.
    //GetByteSliceList in fileWrite will skip 4 bytes
    int32_t len;
    fileWriter->Write((const void*)&len, sizeof(int32_t));
    TermMetaDumper tmDumper;
    tmDumper.Dump(fileWriter, termMeta);

    SegmentPosting segPosting(mPostingFormatOption);
    segPosting.Init(fileWriter->CopyToByteSliceList(false),
                    100, SegmentInfo(), 0);

    return segPosting;
}

void NormalIndexReaderTest::TestLoadSegments()
{
    //prepare index config
    IndexPartitionSchemaPtr partitionSchema = SchemaAdapter::LoadSchema(
            TEST_DATA_PATH, "default_index_engine_example.json");
    IndexSchemaPtr indexSchema = partitionSchema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("bitwords");

    string str = "{\"segments\":[1,3],\"versionid\":10, \"format_version\": 1}";
    Version version;
    version.FromString(str);
    version.Store(mRootDir, false);

    //prepare files for directory
    NormalIndexReaderHelper::PrepareSegmentFiles(
            mRootDir + "segment_1_level_0", "bitwords", indexConfig, 0);
    NormalIndexReaderHelper::PrepareSegmentFiles(
            mRootDir + "segment_3_level_0", "bitwords", indexConfig);

    //prepare indexReader
    NormalIndexReader indexReader;
    indexReader.SetIndexConfig(indexConfig);

    //prepare partitiondata
    index_base::PartitionDataPtr partitionData =
        test::PartitionDataMaker::CreateSimplePartitionData(
                GET_FILE_SYSTEM(), mRootDir);

    vector<NormalIndexSegmentReaderPtr> segmentReaders;
    map<string, size_t> segmentPathToIdx;

    indexReader.LoadSegments(partitionData, segmentReaders);

    ASSERT_EQ(segmentReaders.size(), (size_t)1);
}

void NormalIndexReaderTest::TestCompressedPostingHeader()
{
    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);

    string field = "string1:string;";
    string index = "index:string:string1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            field, index, "", "");
    ASSERT_TRUE(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetDictInlineCompress(true);
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE);

    string docString = "cmd=add,string1=A;"
                       "cmd=add,string1=B;"
                       "cmd=add,string1=A;"
                       "cmd=add,string1=B;"
                       "cmd=add,string1=A;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    partition::IndexPartitionPtr indexPart = psm.GetIndexPartition();
    partition::OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);
    PartitionDataPtr partitionData = onlinePart->GetPartitionData();

    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);
    {
        Term term("A", "index");
        PostingIterator* iter = reader.Lookup(term);
        ASSERT_TRUE(iter);
        ASSERT_EQ((df_t)3, iter->GetTermMeta()->GetDocFreq());
        ASSERT_EQ((termpayload_t)0, iter->GetTermMeta()->GetPayload());
        docid_t expectDocId[] = {0, 2, 4, INVALID_DOCID};
        docid_t docId = INVALID_DOCID;
        for (size_t i = 0; i < 4; ++i)
        {
            docId = iter->SeekDoc(docId);
            ASSERT_EQ(docId, expectDocId[i]);
        }
        delete iter;
    }
    {
        Term term("B", "index");
        PostingIterator* iter = reader.Lookup(term);
        ASSERT_TRUE(iter);
        ASSERT_EQ((df_t)2, iter->GetTermMeta()->GetDocFreq());
        ASSERT_EQ((termpayload_t)0, iter->GetTermMeta()->GetPayload());
        docid_t expectDocId[] = {1, 3, INVALID_DOCID};
        docid_t docId = INVALID_DOCID;
        for (size_t i = 0; i < 3; ++i)
        {
            docId = iter->SeekDoc(docId);
            ASSERT_EQ(docId, expectDocId[i]);
        }
        delete iter;
    }
}

void NormalIndexReaderTest::TestCompressedPostingHeaderForCompatible()
{
    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);

    string indexDir = string(TEST_DATA_PATH) + "/compatible_index_for_posting_header_compress";
    string rootDir = GET_TEST_DATA_PATH() + "/part_dir";
    fslib::fs::FileSystem::copy(indexDir, rootDir);

    IndexPartitionSchemaPtr schema =
        SchemaAdapter::LoadAndRewritePartitionSchema(rootDir, options);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, rootDir));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index:A", "docid=0;docid=2;docid=4;"));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "cmd=add,string1=A;cmd=add,string1=A;",
                             "index:A", "docid=0;docid=2;docid=4;docid=5;docid=6;"));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "",
                             "index:A", "docid=0;docid=2;docid=4;docid=5;docid=6;"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,string1=A,ts=10;",
                             "index:A", "docid=0;docid=2;docid=4;docid=5;docid=6;docid=7"));
}

void NormalIndexReaderTest::TestIndexWithReferenceCompressWithMerge()
{

    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    SingleFieldPartitionDataProvider provider(options);
    provider.Init(GET_TEST_DATA_PATH(), "text", SFP_INDEX);
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetDictInlineCompress(true);
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE);//TODO: delete
    indexConfig->SetIsReferenceCompress(true);

    string shortListDocStr = "A B,A B,A B"; // 3
    string compressedDocStr;
    for (int i = 0; i < 128; ++i)
    {
        compressedDocStr += "A B,"; // 128
    }
    string dictinlineDocStr = "A C"; // 1
    string skipListDocStr;
    for (int i = 0; i < 128 * 5; ++i)
    {
        skipListDocStr += "A D,"; // 128 * 5
    }
    string compressedSkipListDocStr;
    for (int i = 0; i < 128 * 40; ++i)
    {
        compressedSkipListDocStr += "A B,"; // 128 * 40
    }

    string docStr = shortListDocStr + "#"
        + compressedDocStr + "#"
        + dictinlineDocStr + "#"
        + skipListDocStr + "#"
        + compressedSkipListDocStr;

    docid_t docSize = 3 + 128 + 1 + 128 * 5 + 128 * 40;

    provider.Build(docStr, SFP_OFFLINE);

    provider.Merge("0,1,2,3,4");
    PartitionDataPtr partitionData = provider.GetPartitionData();
    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);
    {
        Term term("A", "index");
        PostingIterator* iter = reader.Lookup(term);
        ASSERT_TRUE(iter);
        for (docid_t i = 0; i < docSize; i++)
        {
            EXPECT_EQ((docid_t)i, iter->SeekDoc(i));
        }
        ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(docSize));
        delete iter;
        iter = NULL;
    }
    {
        Term term("C", "index");
        PostingIterator* iter = reader.Lookup(term);
        ASSERT_TRUE(iter);
        EXPECT_EQ((docid_t)(3 + 128), iter->SeekDoc(0));
        ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(3 + 128 + 1));
        delete iter;
        iter = NULL;
    }
}

void NormalIndexReaderTest::TestIndexWithReferenceCompressForBuildingSegment()
{
    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    SingleFieldPartitionDataProvider provider(options);
    provider.Init(GET_TEST_DATA_PATH(), "text", SFP_INDEX);
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetDictInlineCompress(true);
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE);//TODO: delete
    indexConfig->SetIsReferenceCompress(true);
    provider.Build("A B,A B,A B,A B,A B,A C,D", SFP_OFFLINE);
    stringstream ss;
    for (size_t i = 0; i < 129; i++)
    {
        ss << "a b,";
    }

    provider.Build(ss.str(), SFP_REALTIME);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);
    Term term("a", "index");
    PostingIterator* iter = reader.Lookup(term);
    ASSERT_TRUE(iter);
    //test flush
    docid_t rtBaseDocid = 7;
    for (docid_t i = 0; i < 129; i++)
    {
        EXPECT_EQ((docid_t)i + rtBaseDocid, iter->SeekDoc(i));
    }
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(7 + rtBaseDocid));
    delete iter;
    iter = NULL;
}

void NormalIndexReaderTest::TestIndexWithReferenceCompress()
{
    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    SingleFieldPartitionDataProvider provider(options);
    provider.Init(GET_TEST_DATA_PATH(), "text", SFP_INDEX);
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetDictInlineCompress(true);
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE);//TODO: delete
    indexConfig->SetIsReferenceCompress(true);
    provider.Build("A B,A B,A B,A B,A B,A C,D", SFP_OFFLINE);
    provider.Build("a b,a b,a b,a b,a b,a c,d", SFP_REALTIME);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);
    Term term("A", "index");
    PostingIterator* iter = reader.Lookup(term);
    ASSERT_TRUE(iter);
    //test compress
    for (docid_t i = 0; i < 6; i++)
    {
        EXPECT_EQ((docid_t)i, iter->SeekDoc(i));
    }
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(7));
    delete iter;
    iter = NULL;
    Term term1("B", "index");
    iter = reader.Lookup(term1);
    ASSERT_TRUE(iter);
    //test short list
    for (docid_t i = 0; i < 5; i++)
    {
        EXPECT_EQ((docid_t)i, iter->SeekDoc(i));
    }
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(6));
    delete iter;
    iter = NULL;
    //test dict inline
    Term term2("C", "index");
    iter = reader.Lookup(term2);
    ASSERT_TRUE(iter);
    ASSERT_EQ((docid_t)5, iter->SeekDoc(0));
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(6));
    delete iter;
    iter = NULL;
    //test realtime
    docid_t rtBaseDocid = 7;
    Term term3("a", "index");
    iter = reader.Lookup(term3);
    ASSERT_TRUE(iter);
    //test short buffer
    for (docid_t i = 0; i < 6; i++)
    {
        EXPECT_EQ((docid_t)i + rtBaseDocid, iter->SeekDoc(i + rtBaseDocid));
    }
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(7 + rtBaseDocid));
    delete iter;

    Term term4("b", "index");
    iter = reader.Lookup(term4);
    ASSERT_TRUE(iter);
    for (docid_t i = 0; i < 5; i++)
    {
        EXPECT_EQ((docid_t)i + rtBaseDocid, iter->SeekDoc(i + rtBaseDocid));
    }
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(6 + rtBaseDocid));
    delete iter;

    Term term5("c", "index");
    iter = reader.Lookup(term5);
    ASSERT_TRUE(iter);
    EXPECT_EQ((docid_t)5 + rtBaseDocid, iter->SeekDoc(0 + rtBaseDocid));
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(6 + rtBaseDocid));
    delete iter;

}

void NormalIndexReaderTest::TestIndexWithReferenceCompressForSkiplist()
{
    innerTestIndexWithReferenceCompress(128);
    innerTestIndexWithReferenceCompress(128 * 10);
    innerTestIndexWithReferenceCompress(128 * 32);
    innerTestIndexWithReferenceCompress(128 * 40);

    innerTestIndexWithReferenceCompress(10, 128);
    innerTestIndexWithReferenceCompress(128, 128 * 10);
    innerTestIndexWithReferenceCompress(128 * 10, 128 * 32);
    innerTestIndexWithReferenceCompress(128 * 40, 128 * 40);
}

void NormalIndexReaderTest::TestMmapLoadConfig()
{
    IndexPartitionOptions options;
    string jsonStr = R"(
    {"load_config" : [{
         "file_patterns" : ["_INDEX_"],
         "load_strategy" : "mmap",
         "load_strategy_param" : { "partial_lock" : true }}]}
    )";
    FromJsonString(options.GetOnlineConfig().loadConfigList, jsonStr);
    options.SetEnablePackageFile(false);

    string field = "pk:uint64:pk;string1:string;text1:text;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    config::IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, "long1", "");

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE,
                             "cmd=add,pk=10,string1=A,long1=1;"
                             "cmd=add,pk=20,string1=A,long1=2;",
                             "index1:A", "docid=0,long1=1;docid=1,long1=2"));
}

void NormalIndexReaderTest::TestCacheLoadConfig()
{
    IndexPartitionOptions options;
    string jsonStr = R"(
    {"load_config" : [{
         "file_patterns" : ["_INDEX_"],
         "load_strategy" : "cache",
         "load_strategy_param" : { "globa_cache" : false }}]}
    )";
    FromJsonString(options.GetOnlineConfig().loadConfigList, jsonStr);
    options.SetEnablePackageFile(false);

    string field = "pk:uint64:pk;string1:string;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    config::IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, "long1", "");

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    string fullDocs;
    string expectedResult;
    for (size_t i = 1; i <= 5000; ++i)
    {
        string id = StringUtil::toString(i);
        string docId = StringUtil::toString(i-1);
        string singleDocStr= string("cmd=add,pk=") +
                             id + string(",string1=A,long1=") + id + string(";");
        fullDocs += singleDocStr;
        expectedResult += (string("docid=") + docId + string(",long1=") + id + string(";"));
    }
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "index1:A", expectedResult));
}

void NormalIndexReaderTest::TestLookupWithMultiInMemSegments()
{
    IndexPartitionOptions options;
    string field = "pk:uint64:pk;string1:string;text1:text;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    config::IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, "long1", "");
    DictionaryConfigPtr dictConfig = schema->AddDictionaryConfig("hf_dict", "A;B;C");

    // set bitmap
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");
    assert(indexConfig);
    indexConfig->SetDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(hp_both);

    FakePartitionDataCreator pdc;
    pdc.Init(schema, options, GET_TEST_DATA_PATH(), false);

    string fullDocs = "cmd=add,pk=1,string1=A,long1=1;"
                      "cmd=add,pk=2,string1=B,long1=2;";
    pdc.AddFullDocs(fullDocs);

    string builtRtDocs = "cmd=add,pk=3,string1=B,long1=3,ts=0;"
                         "cmd=add,pk=4,string1=A,long1=4,ts=0;";
    pdc.AddBuiltSegment(builtRtDocs);

    string inMemSegment1 = "cmd=add,pk=5,string1=B,long1=5,ts=0;"
                           "cmd=add,pk=6,string1=A,long1=6,ts=0;";
    pdc.AddInMemSegment(inMemSegment1);

    string inMemSegment2 = "cmd=add,pk=7,string1=B,long1=7,ts=0;"
                           "cmd=add,pk=8,string1=A,long1=8,ts=0;"
                           "cmd=add,pk=9,string1=C,long1=9,ts=0;";
    pdc.AddInMemSegment(inMemSegment2);

    string buildingDocs = "cmd=add,pk=10,string1=A,long1=10,ts=0;"
                          "cmd=add,pk=11,string1=C,long1=11,ts=0;";
    pdc.AddBuildingData(buildingDocs);

    PartitionDataPtr partData = pdc.CreatePartitionData(false);
    NormalIndexReaderPtr indexReader(new NormalIndexReader);
    indexReader->Open(indexConfig, partData);

    // check normal
    CheckPostingIterator(indexReader, pt_normal, "index1:A", "0,3,5,7,9");
    CheckPostingIterator(indexReader, pt_normal, "index1:B", "1,2,4,6");
    CheckPostingIterator(indexReader, pt_normal, "index1:C", "8,10");

    // check bitmap
    CheckPostingIterator(indexReader, pt_bitmap, "index1:A", "0,3,5,7,9");
    CheckPostingIterator(indexReader, pt_bitmap, "index1:B", "1,2,4,6");
    CheckPostingIterator(indexReader, pt_bitmap, "index1:C", "8,10");
}

void NormalIndexReaderTest::TestPartialLookup()
{
    IndexPartitionOptions options;
    string field = "pk:uint64:pk;string1:string;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "long1", "");
    DictionaryConfigPtr dictConfig = schema->AddDictionaryConfig("hf_dict", "A;B;C");

    // set bitmap
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");
    assert(indexConfig);
    indexConfig->SetDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(hp_both);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    string fullDocs0 = "cmd=add,pk=0,string1=A,long1=0;"
                      "cmd=add,pk=1,string1=B,long1=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs0, "", ""));
    string incDocs1 = "cmd=add,pk=2,string1=B,long1=2;"
                      "cmd=add,pk=3,string1=A,long1=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs1, "", ""));
    string incDocs2 = "cmd=add,pk=4,string1=A,long1=4;"
                      "cmd=add,pk=5,string1=B,long1=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs2, "", ""));
    string rtDocs3 = "cmd=add,pk=6,string1=A,long1=6,ts=6;"
                     "cmd=add,pk=7,string1=B,long1=7,ts=7;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs3, "", ""));
    string rtDocs4 = "cmd=add,pk=8,string1=A,long1=8,ts=8;"
                     "cmd=add,pk=9,string1=B,long1=9,ts=9;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs4, "", ""));

    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);
    PartitionDataPtr partitionData = onlinePart->GetPartitionData();

    // NormalIndexReaderPtr reader(new NormalIndexReader);
    // reader->Open(indexConfig, partitionData);

    auto partReader = onlinePart->GetReader();
    DocIdRangeVector ranges;
    ASSERT_TRUE(partReader->GetPartedDocIdRanges(
        { { 0, 1 }, { 1, 2 }, { 2, 3 }, { 4, 6 }, { 6, 8 } }, 3, 0, ranges));
    ASSERT_EQ(3u, ranges.size());
    EXPECT_EQ(0, ranges[0].first);
    EXPECT_EQ(1, ranges[0].second);
    EXPECT_EQ(1, ranges[1].first);
    EXPECT_EQ(2, ranges[1].second);
    EXPECT_EQ(2, ranges[2].first);
    EXPECT_EQ(3, ranges[2].second);
    auto reader = partReader->GetIndexReader();
    CheckPostingIterator(reader, pt_normal, "index1:A", "0,3", ranges);

    CheckPostingIterator(reader, pt_normal, "index1:A", "3,6,8", { { 2, 3 }, {6, 9}});
    CheckPostingIterator(reader, pt_normal, "index1:A", "0,3,4,", { { 0, 1 }, {3, 6}});
    CheckPostingIterator(reader, pt_normal, "index1:B", "1,2,5", { { 0, 1 }, {3, 6}});
    CheckPostingIterator(reader, pt_normal, "index1:A", "0,8,", { { 0, 1 }, {8, 9}});
    CheckPostingIterator(reader, pt_normal, "index1:B", "1,9,", { { 0, 1 }, {8, 9}});

    CheckPostingIterator(reader, pt_normal, "index1:B", "1,2,5", { { 0, 1 }, { 3, 4 }, { 5, 6 } });
}

void NormalIndexReaderTest::CheckPostingIterator(const IndexReaderPtr& indexReader,
    PostingType postingType, const string& termInfoStr, const string& docIdListStr,
    const DocIdRangeVector& ranges)
{
    vector<string> termInfo;
    StringUtil::fromString(termInfoStr, termInfo, ":");

    Term term(termInfo[1], termInfo[0]);
    Pool pool;
    PostingIterator* postingIter = nullptr;
    if (ranges.empty())
    {
        postingIter = indexReader->Lookup(term, 1000, postingType, &pool);
    }
    else
    {
        postingIter = indexReader->PartialLookup(term, ranges, 1000, postingType, &pool);
    }

    ASSERT_TRUE(postingIter);

    vector<docid_t> docIds;
    StringUtil::fromString(docIdListStr, docIds, ",");
    docid_t curDocId = INVALID_DOCID;
    for (size_t i = 0; i < docIds.size(); i++)
    {
        curDocId = postingIter->SeekDoc(curDocId);
        ASSERT_EQ(docIds[i], curDocId);
    }

    ASSERT_EQ(INVALID_DOCID, postingIter->SeekDoc(curDocId));
    IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, postingIter);
}

IE_NAMESPACE_END(index);
