#include "autil/StringUtil.h"
#include "autil/TimeoutTerminator.h"
#include "autil/mem_pool/Pool.h"
#include "fslib/fs/ErrorGenerator.h"
#include "fslib/fs/FileSystem.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/InterimFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/test/mock_index_reader.h"
#include "indexlib/index/normal/inverted_index/test/normal_index_reader_helper.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/fake_partition_data_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::test;

namespace indexlib { namespace index {
class NormalIndexReaderTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    NormalIndexReaderTest();
    ~NormalIndexReaderTest();

    DECLARE_CLASS_NAME(NormalIndexReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFillTruncSegmentPostingWithTruncAndMain();
    void TestFillTruncSegmentPostingWithTruncAndNoMain();
    void TestFillTruncSegmentPostingWithNoTruncAndMain();
    void TestFillTruncSegmentPostingWithNoTruncAndNoMain();
    void TestLoadSegments();
    void TestDocListDictInline();
    void TestShortDocListVbyteCompress();
    void TestP4DeltaEqualOptimize();
    void TestCompressedPostingFile();
    void TestCompressedPostingHeader();
    void TestCompressedPostingHeaderForCompatible();
    void TestIndexWithReferenceCompress();
    void TestIndexWithReferenceCompressForBuildingSegment();
    void TestIndexWithReferenceCompressWithMerge();
    void TestIndexWithReferenceCompressForSkiplist();
    void TestMmapLoadConfig();
    void TestCacheLoadConfig();
    void TestLookupWithMultiInMemSegments();
    void TestPartialLookup();
    void TestOpenWithHintReader();
    void TestMergeRangeByHintValue();
    void TestLookupAsync();
    void TestLookupWithIoException();
    void TestDictWithHashFunc();

private:
    void PrepareSegmentPosting(MockNormalIndexReader& indexReader, const TermMeta& termMeta);
    SegmentPosting CreateSegmentPosting(const TermMeta& termMeta);
    void CheckLoadSegment(config::IndexConfigPtr indexConfig, std::string& segmentPath, bool hasDictReader,
                          bool hasPostingReader, bool hasException);
    void innerTestIndexWithReferenceCompress(docid_t docCount, docid_t realtimeDocCount = 0);

    // termInfoStr: "index1:A"; docIdListStr: "0,1,..."
    void CheckPostingIterator(const std::shared_ptr<index::InvertedIndexReader>& indexReader, PostingType postingType,
                              const std::string& termInfoStr, const std::string& docIdListStr,
                              const DocIdRangeVector& ranges = {});

private:
    std::string mRootDir;
    MockNormalIndexReader mIndexReader;
    MockMultiFieldIndexReader mMultiFieldIndexReader;
    MockNormalIndexReaderPtr mTruncateIndexReader;
    std::shared_ptr<index::InvertedIndexReader> mIndexReaderPtr;
    MockBitmapIndexReader* mBitmapIndexReader;
    // all test used mPostingFormatOption should equal
    PostingFormatOption mPostingFormatOption;
    config::IndexPartitionOptions _options;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestFillTruncSegmentPostingWithTruncAndMain);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestFillTruncSegmentPostingWithTruncAndNoMain);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestFillTruncSegmentPostingWithNoTruncAndMain);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestFillTruncSegmentPostingWithNoTruncAndNoMain);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestLoadSegments);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestDocListDictInline);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestShortDocListVbyteCompress);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestP4DeltaEqualOptimize);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestCompressedPostingHeader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestCompressedPostingHeaderForCompatible);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestIndexWithReferenceCompress);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestIndexWithReferenceCompressForBuildingSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestIndexWithReferenceCompressWithMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestIndexWithReferenceCompressForSkiplist);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestMmapLoadConfig);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestCacheLoadConfig);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestLookupWithMultiInMemSegments);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestPartialLookup);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestCompressedPostingFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestOpenWithHintReader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestMergeRangeByHintValue);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestLookupAsync);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestLookupWithIoException);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NormalIndexReaderTest, TestDictWithHashFunc);

INSTANTIATE_TEST_CASE_P(BuildMode, NormalIndexReaderTest, testing::Values(0, 1, 2));

IE_LOG_SETUP(index, NormalIndexReaderTest);

NormalIndexReaderTest::NormalIndexReaderTest() { mBitmapIndexReader = NULL; }

NormalIndexReaderTest::~NormalIndexReaderTest() {}

void NormalIndexReaderTest::CaseSetUp()
{
    mIndexReader.SetMultiFieldIndexReader(&mMultiFieldIndexReader);
    EXPECT_CALL(mIndexReader, DoGetSegmentPosting(_, _, _)).WillRepeatedly(Return(false));

    mTruncateIndexReader.reset(new MockNormalIndexReader);
    mIndexReaderPtr = mTruncateIndexReader;
    EXPECT_CALL(*mTruncateIndexReader, DoGetSegmentPosting(_, _, _)).WillRepeatedly(Return(false));
    EXPECT_CALL(mMultiFieldIndexReader, GetInvertedIndexReader(_)).WillRepeatedly(ReturnRef(mIndexReaderPtr));

    DELETE_AND_SET_NULL(mBitmapIndexReader);
    mBitmapIndexReader = new MockBitmapIndexReader;
    EXPECT_CALL(*mBitmapIndexReader, DoGetSegmentPosting(_, _, _)).WillRepeatedly(Return(false));
    mIndexReader.SetBitmapIndexReader(mBitmapIndexReader);

    mRootDir = GET_TEMP_DATA_PATH();

    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &_options);
}

void NormalIndexReaderTest::CaseTearDown() {}

void NormalIndexReaderTest::innerTestIndexWithReferenceCompress(docid_t docCount, docid_t realtimeDocCount)
{
    tearDown();
    setUp();

    _options.SetEnablePackageFile(false);
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "text", SFP_INDEX);
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE); // TODO: delete
    indexConfig->SetIsReferenceCompress(true);

    string docStr;
    for (docid_t i = 0; i < docCount; ++i) {
        docStr += "A B,";
    }
    provider.Build(docStr, SFP_OFFLINE);

    if (realtimeDocCount) {
        string docStr;
        for (docid_t i = 0; i < realtimeDocCount; ++i) {
            docStr += "a b,";
        }
        provider.Build(docStr, SFP_REALTIME);
    }

    PartitionDataPtr partitionData = provider.GetPartitionData();
    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);

    if (docCount) {
        Term term("A", "index");
        PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
        ASSERT_TRUE(iter);
        for (docid_t i = 0; i < docCount; i++) {
            EXPECT_EQ((docid_t)i, iter->SeekDoc(i));
        }
        ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(docCount));
        delete iter;
        iter = NULL;
    }
    if (realtimeDocCount) {
        docid_t rtBaseDocId = docCount;
        Term term("a", "index");
        PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
        ASSERT_TRUE(iter);
        for (docid_t i = 0; i < realtimeDocCount; i++) {
            EXPECT_EQ((docid_t)i + rtBaseDocId, iter->SeekDoc(i + rtBaseDocId));
        }
        ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(realtimeDocCount + rtBaseDocId));
        delete iter;
        iter = NULL;
    }
}

void NormalIndexReaderTest::TestMergeRangeByHintValue()
{
    NormalIndexReader indexReader;
    std::map<TemperatureProperty, DocIdRangeVector> temperatureMeta;
    DocIdRange expectRange(0, 4);
    // only has hot
    temperatureMeta[TemperatureProperty::HOT] = {expectRange};
    TemperatureDocInfoPtr info(new TemperatureDocInfo(temperatureMeta));
    indexReader.mTemperatureDocInfo = info;
    auto ranges = indexReader.MergeDocIdRanges(1, {});
    DocIdRangeVector expectRanges {expectRange};
    ASSERT_EQ(expectRanges, ranges);

    ranges = indexReader.MergeDocIdRanges(1, {{2, 3}});
    expectRanges.clear();
    expectRanges.push_back({2, 3});
    ASSERT_EQ(expectRanges, ranges);

    ranges = indexReader.MergeDocIdRanges(1, {{3, 100}});
    expectRanges.clear();
    expectRanges.push_back({3, 4});
    ASSERT_EQ(expectRanges, ranges);

    ranges = indexReader.MergeDocIdRanges(3, {{3, 100}});
    ASSERT_EQ(expectRanges, ranges);

    temperatureMeta.clear();
    temperatureMeta[TemperatureProperty::HOT] = {{0, 4}, {9, 12}};
    temperatureMeta[TemperatureProperty::WARM] = {{4, 9}, {12, 14}};
    temperatureMeta[TemperatureProperty::COLD] = {{14, 100}};
    info.reset(new TemperatureDocInfo(temperatureMeta));
    indexReader.mTemperatureDocInfo = info;

    ranges = indexReader.MergeDocIdRanges(3, {{6, 100}});
    expectRanges.clear();
    expectRanges.push_back({6, 14});
    ASSERT_EQ(expectRanges, ranges);

    ranges = indexReader.MergeDocIdRanges(4, {{0, 6}});
    expectRanges.clear();
    ASSERT_EQ(expectRanges, ranges);

    ranges = indexReader.MergeDocIdRanges(2, {{3, 4}, {8, 13}});
    expectRanges.push_back({8, 9});
    expectRanges.push_back({12, 13});
    ASSERT_EQ(expectRanges, ranges);

    ranges = indexReader.MergeDocIdRanges(4, {{14, 16}, {18, 20}});
    expectRanges.clear();
    expectRanges.push_back({14, 16});
    expectRanges.push_back({18, 20});
    ASSERT_EQ(expectRanges, ranges);
}

void NormalIndexReaderTest::TestFillTruncSegmentPostingWithTruncAndMain()
{
    PrepareSegmentPosting(*mTruncateIndexReader, TermMeta(1, 1, 1));
    TermMeta termMeta(100, 100, 100);
    PrepareSegmentPosting(mIndexReader, termMeta);

    SegmentPosting resultSegPosting(mPostingFormatOption);
    bool ret = future_lite::coro::syncAwait(mIndexReader.FillTruncSegmentPosting(Term("", "index", "trunc"),
                                                                                 index::DictKeyInfo(1), 1,
                                                                                 resultSegPosting, nullptr))
                   .ValueOrThrow();
    ASSERT_TRUE(ret);

    SegmentPosting truncSegPosting(mPostingFormatOption);
    [[maybe_unused]] auto result =
        future_lite::coro::syncAwait(
            mTruncateIndexReader->GetSegmentPostingAsync(index::DictKeyInfo(1), 1, truncSegPosting, nullptr, nullptr))
            .ValueOrThrow();
    truncSegPosting.SetMainChainTermMeta(termMeta);
    ASSERT_EQ(truncSegPosting, resultSegPosting);
}

void NormalIndexReaderTest::TestFillTruncSegmentPostingWithTruncAndNoMain()
{
    PrepareSegmentPosting(*mTruncateIndexReader, TermMeta(1, 1, 1));

    // TODO: bitmap should exist
    SegmentPosting resultSegPosting(mPostingFormatOption);
    bool ret = future_lite::coro::syncAwait(mIndexReader.FillTruncSegmentPosting(Term("", "index", "trunc"),
                                                                                 index::DictKeyInfo(1), 1,
                                                                                 resultSegPosting, nullptr))
                   .ValueOrThrow();
    ASSERT_TRUE(!ret);

    TermMeta termMeta(100, 100, 100);
    SegmentPosting bitmapSegPosting = CreateSegmentPosting(termMeta);
    EXPECT_CALL(*mBitmapIndexReader, DoGetSegmentPosting(_, _, _))
        .WillRepeatedly(DoAll(SetArgReferee<2>(bitmapSegPosting), Return(true)));

    ret = future_lite::coro::syncAwait(mIndexReader.FillTruncSegmentPosting(Term("", "index", "trunc"),
                                                                            index::DictKeyInfo(1), 1, resultSegPosting,
                                                                            nullptr))
              .ValueOrThrow();
    ASSERT_TRUE(ret);

    SegmentPosting truncSegPosting(mPostingFormatOption);
    [[maybe_unused]] auto result =
        future_lite::coro::syncAwait(
            mTruncateIndexReader->GetSegmentPostingAsync(index::DictKeyInfo(1), 1, truncSegPosting, nullptr, nullptr))
            .ValueOrThrow();
    truncSegPosting.SetMainChainTermMeta(termMeta);
    ASSERT_EQ(truncSegPosting, resultSegPosting);
}

void NormalIndexReaderTest::TestFillTruncSegmentPostingWithNoTruncAndMain()
{
    TermMeta termMeta(100, 100, 100);
    PrepareSegmentPosting(mIndexReader, termMeta);

    SegmentPosting mainSegPosting(mPostingFormatOption);
    [[maybe_unused]] auto result =
        future_lite::coro::syncAwait(
            mIndexReader.GetSegmentPostingAsync(index::DictKeyInfo(1), 1, mainSegPosting, nullptr, nullptr))
            .ValueOrThrow();

    SegmentPosting resultSegPosting(mPostingFormatOption);
    bool ret = future_lite::coro::syncAwait(mIndexReader.FillTruncSegmentPosting(Term("", "index", "trunc"),
                                                                                 index::DictKeyInfo(1), 1,
                                                                                 resultSegPosting, nullptr))
                   .ValueOrThrow();
    ASSERT_TRUE(ret);

    mainSegPosting.SetMainChainTermMeta(termMeta);
    ASSERT_EQ(mainSegPosting, resultSegPosting);
}

void NormalIndexReaderTest::TestFillTruncSegmentPostingWithNoTruncAndNoMain()
{
    SegmentPosting resultSegPosting(mPostingFormatOption);
    bool ret = future_lite::coro::syncAwait(mIndexReader.FillTruncSegmentPosting(Term("", "index", "trunc"),
                                                                                 index::DictKeyInfo(1), 1,
                                                                                 resultSegPosting, nullptr))
                   .ValueOrThrow();
    ASSERT_FALSE(ret);
}

void NormalIndexReaderTest::PrepareSegmentPosting(MockNormalIndexReader& indexReader, const TermMeta& termMeta)
{
    SegmentPosting segPosting = CreateSegmentPosting(termMeta);
    EXPECT_CALL(indexReader, DoGetSegmentPosting(_, _, _))
        .WillRepeatedly(DoAll(SetArgReferee<2>(segPosting), Return(true)));
}

SegmentPosting NormalIndexReaderTest::CreateSegmentPosting(const TermMeta& termMeta)
{
    file_system::InterimFileWriterPtr fileWriter(new file_system::InterimFileWriter);
    fileWriter->Init(100);

    // TODO: at first, we must write 4 bytes to fileWrite.
    // GetByteSliceList in fileWrite will skip 4 bytes
    int32_t len;
    fileWriter->Write((const void*)&len, sizeof(int32_t)).GetOrThrow();
    TermMetaDumper tmDumper;
    tmDumper.Dump(fileWriter, termMeta);

    SegmentPosting segPosting(mPostingFormatOption);
    segPosting.Init(ShortListOptimizeUtil::GetCompressMode(0), fileWriter->CopyToByteSliceList(false), 100, 0);

    return segPosting;
}

void NormalIndexReaderTest::TestLoadSegments()
{
    // prepare index config
    IndexPartitionSchemaPtr partitionSchema =
        SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "default_index_engine_example.json");
    IndexSchemaPtr indexSchema = partitionSchema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("bitwords");

    string str = "{\"segments\":[1,3],\"versionid\":10, \"format_version\": 1}";
    Version version;
    version.FromString(str);
    version.Store(GET_PARTITION_DIRECTORY(), false);

    // prepare files for directory
    NormalIndexReaderHelper::PrepareSegmentFiles(GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1_level_0"),
                                                 "bitwords", indexConfig, 0);
    NormalIndexReaderHelper::PrepareSegmentFiles(GET_PARTITION_DIRECTORY()->MakeDirectory("segment_3_level_0"),
                                                 "bitwords", indexConfig);

    // prepare indexReader
    NormalIndexReader indexReader;
    indexReader.SetIndexConfig(indexConfig);

    // prepare partitiondata
    index_base::PartitionDataPtr partitionData =
        test::PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM(), "");

    vector<NormalIndexSegmentReaderPtr> segmentReaders;
    map<string, size_t> segmentPathToIdx;

    indexReader.LoadSegments(partitionData, segmentReaders, nullptr);

    ASSERT_EQ(segmentReaders.size(), (size_t)1);
}

void NormalIndexReaderTest::TestDictWithHashFunc()
{
    auto InnerTest = [this](const vector<size_t>& docIdToGroup) {
        IndexPartitionSchemaPtr schema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/schema_with_hash.json");
        ASSERT_TRUE(schema);

        string docString;
        for (docid_t docId = 0; docId < docIdToGroup.size(); ++docId) {
            // "cmd=add,pk=pk3,string1=index2#value3,ts=3;"
            string id = std::to_string(docId);
            string group = std::to_string(docIdToGroup[docId]);
            docString += "cmd=add,pk=pk" + id + ",string1=index" + group + "#value" + id + ",ts=1;";
        }
        PartitionStateMachine psm;
        IndexPartitionOptions options = _options;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        partition::IndexPartitionPtr indexPart = psm.GetIndexPartition();
        partition::OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);
        PartitionDataPtr partitionData = onlinePart->GetPartitionData();
        NormalIndexReader reader;
        IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("string1");
        reader.Open(indexConfig, partitionData);
        std::shared_ptr<KeyIterator> iterator = reader.CreateKeyIterator("");
        DYNAMIC_POINTER_CAST(KeyIteratorTyped<NormalIndexReader>, iterator)->Init();
        // expected dict key is first-layer-order
        vector<docid_t> result;
        while (iterator->HasNext()) {
            string strKey;
            PostingIteratorPtr pIter(iterator->NextPosting(strKey));
            for (docid_t docId = pIter->SeekDoc(INVALID_DOCID); docId != INVALID_DOCID; docId = pIter->SeekDoc(docId)) {
                result.push_back(docId);
            }
        }
        AUTIL_LOG(ERROR, "ut result is [%s]", autil::StringUtil::toString(result).c_str());

        // check docid grouped by first layer
#define NOT_VISIT 0
#define VISITING 1
#define FINISHED 2
        map<size_t, size_t> groupState;
        size_t lastGroup = -1;
        for (docid_t docId : result) {
            size_t group = docIdToGroup[docId];
            if (lastGroup != group && lastGroup != -1) {
                groupState[lastGroup] = FINISHED;
            }
            lastGroup = group;

            switch (groupState[group]) {
            case NOT_VISIT:
            case VISITING: {
                groupState[group] = VISITING;
                break;
            }
            case FINISHED: {
                ASSERT_TRUE(false) << docId;
            }
            }
        }
    };

    InnerTest({1, 2, 2, 1, 1});
    InnerTest({1, 3, 3, 1, 2, 1, 3, 2, 4, 5, 4, 5, 1});
}

void NormalIndexReaderTest::TestLookupAsync()
{
    string loadConfig = R"(
        {
            "load_config":[
                {
                    "file_patterns":[
                        ".*"
                    ],
                    "load_strategy":"cache",
                    "load_strategy_param":{
                        "direct_io":false,
                        "global_cache":false,
                        "memory_size_in_mb":0
                    }
                }
            ]
        }
    )";
    FromJsonString(_options.GetOnlineConfig().loadConfigList, loadConfig);
    _options.GetOfflineConfig().buildConfig.maxDocCount = 1;
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(schema,
                                     // Field schema
                                     "string0:string;string1:string;long1:uint32;string2:string:true",
                                     // Index schema
                                     "string2:string:string2;"
                                     // Primary key index schema
                                     "pk:primarykey64:string1",
                                     // Attribute schema
                                     "long1;string0;string1;string2",
                                     // Summary schema
                                     "string1");
    ASSERT_TRUE(schema);

    string docString = "cmd=add,string1=pk1,string2=1,ts=1;"
                       "cmd=add,string1=pk2,string2=2,ts=2;"
                       "cmd=add,string1=pk3,string2=1,ts=3;"
                       "cmd=add,string1=pk4,string2=2,ts=4;";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    partition::IndexPartitionPtr indexPart = psm.GetIndexPartition();
    partition::OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);
    PartitionDataPtr partitionData = onlinePart->GetPartitionData();
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("string2");
    shared_ptr<future_lite::Executor> executor(new future_lite::executors::SimpleExecutor(2));

    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);
    reader.mExecutor = executor.get();

    {
        Term term("1", "string2");
        auto iter = future_lite::coro::syncAwait(
                        reader.LookupAsync(&term, (uint32_t)1000, pt_default, nullptr, nullptr).via(executor.get()))
                        .ValueOrThrow();
        ASSERT_TRUE(iter);
        ASSERT_EQ((df_t)2, iter->GetTermMeta()->GetDocFreq());
        docid_t expectDocId[] = {0, 2, INVALID_DOCID};
        docid_t docId = INVALID_DOCID;
        for (size_t i = 0; i < 3; ++i) {
            docId = iter->SeekDoc(docId);
            ASSERT_EQ(docId, expectDocId[i]);
        }
        delete iter;
    }
    {
        Term term("2", "string2");
        auto iter = future_lite::coro::syncAwait(
                        reader.LookupAsync(&term, (uint32_t)1000, pt_default, nullptr, nullptr).via(executor.get()))
                        .ValueOrThrow();
        ASSERT_TRUE(iter);
        ASSERT_EQ((df_t)2, iter->GetTermMeta()->GetDocFreq());
        docid_t expectDocId[] = {1, 3, INVALID_DOCID};
        docid_t docId = INVALID_DOCID;
        for (size_t i = 0; i < 3; ++i) {
            docId = iter->SeekDoc(docId);
            ASSERT_EQ(docId, expectDocId[i]);
        }
        delete iter;
    }
    {
        // test timeout
        autil::TimeoutTerminator terminator(1, 1);
        Term term("1", "string2");
        auto result = future_lite::coro::syncAwait(
            reader.LookupAsync(&term, (uint32_t)1000, pt_default, nullptr, &terminator).via(executor.get()));
        ASSERT_FALSE(result.Ok());
        ASSERT_EQ(index::ErrorCode::Timeout, result.GetErrorCode());
    }
    executor.reset();
}

void NormalIndexReaderTest::TestShortDocListVbyteCompress()
{
    string field = "string1:string";
    string index = "index:string:string1";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    ASSERT_TRUE(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE);
    indexConfig->SetShortListVbyteCompress(true);

    string docString = "cmd=add,string1=A;"
                       "cmd=add,string1=A;"
                       "cmd=add,string1=B;"
                       "cmd=add,string1=A;"
                       "cmd=add,string1=B;";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    partition::IndexPartitionPtr indexPart = psm.GetIndexPartition();
    partition::OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);
    PartitionDataPtr partitionData = onlinePart->GetPartitionData();

    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);
    {
        Term term("A", "index");
        PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
        ASSERT_TRUE(iter);
        ASSERT_EQ((df_t)3, iter->GetTermMeta()->GetDocFreq());
        docid_t expectDocId[] = {0, 1, 3, INVALID_DOCID};
        docid_t docId = INVALID_DOCID;
        for (size_t i = 0; i < 4; ++i) {
            docId = iter->SeekDoc(docId);
            ASSERT_EQ(docId, expectDocId[i]);
        }
        delete iter;
    }
    {
        Term term("B", "index");
        PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
        ASSERT_TRUE(iter);
        ASSERT_EQ((df_t)2, iter->GetTermMeta()->GetDocFreq());
        docid_t expectDocId[] = {2, 4, INVALID_DOCID};
        docid_t docId = INVALID_DOCID;
        for (size_t i = 0; i < 3; ++i) {
            docId = iter->SeekDoc(docId);
            ASSERT_EQ(docId, expectDocId[i]);
        }
        delete iter;
    }
}

void NormalIndexReaderTest::TestP4DeltaEqualOptimize()
{
    string field = "string1:string";
    string index = "index:string:string1";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    ASSERT_TRUE(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE);

    string docString = "cmd=add,string1=A;"
                       "cmd=add,string1=B;";
    for (int i = 0; i < 512; i++) {
        docString += string("cmd=add,string1=A;");
    }
    auto checkIterator = [](PostingIterator* iter) {
        docid_t docId = INVALID_DOCID;
        for (size_t i = 0; i < 514; ++i) {
            if (i == 1) {
                continue;
            }
            docId = iter->SeekDoc(docId);
            ASSERT_EQ((docid_t)i, docId);
        }
        docId = iter->SeekDoc(docId);
        ASSERT_EQ(INVALID_DOCID, docId);
    };

    Term term("A", "index");
    size_t orgPostingSize = 0;
    size_t optPostingLen = 0;
    {
        ASSERT_TRUE(indexConfig->SetIndexFormatVersionId(0).IsOK()); // legacy p4delta compressor
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, _options, GET_TEMP_DATA_PATH() + "/orgin"));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        partition::IndexPartitionPtr indexPart = psm.GetIndexPartition();
        partition::OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);
        PartitionDataPtr partitionData = onlinePart->GetPartitionData();

        NormalIndexReader reader;
        reader.Open(indexConfig, partitionData);
        PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
        ASSERT_TRUE(iter);
        orgPostingSize = iter->GetPostingLength();
        checkIterator(iter);
        delete iter;
    }
    {
        ASSERT_TRUE(indexConfig->SetIndexFormatVersionId(1).IsOK()); // opt p4delta compressor
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, _options, GET_TEMP_DATA_PATH() + "/optimize"));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        partition::IndexPartitionPtr indexPart = psm.GetIndexPartition();
        partition::OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);
        PartitionDataPtr partitionData = onlinePart->GetPartitionData();

        NormalIndexReader reader;
        reader.Open(indexConfig, partitionData);
        PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
        ASSERT_TRUE(iter);
        optPostingLen = iter->GetPostingLength();
        checkIterator(iter);
        delete iter;
    }
    ASSERT_GT(orgPostingSize, optPostingLen);
}

void NormalIndexReaderTest::TestCompressedPostingFile()
{
    string str = R"(
    {
        "fields": [
           {
               "field_name" : "string1",
               "field_type" : "STRING"
           }
        ],
       "indexs": [
           {
               "index_fields": "string1",
               "index_name": "index",
               "index_type": "STRING",
               "file_compress": "compressor",
               "format_version_id" : 1
           }
       ],
       "file_compress": [
           {
               "name" : "compressor",
               "type" : "zstd"
           }
       ],
       "table_name" : "test_table"
    })";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJsonString(*schema, str);
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index");

    string docString = "cmd=add,string1=A;"
                       "cmd=add,string1=B;";
    for (int i = 0; i < 512; i++) {
        docString += string("cmd=add,string1=A;");
    }
    auto checkIterator = [](PostingIterator* iter) {
        docid_t docId = INVALID_DOCID;
        for (size_t i = 0; i < 514; ++i) {
            if (i == 1) {
                continue;
            }
            docId = iter->SeekDoc(docId);
            ASSERT_EQ((docid_t)i, docId);
        }
        docId = iter->SeekDoc(docId);
        ASSERT_EQ(INVALID_DOCID, docId);
    };

    Term term("A", "index");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, GET_TEMP_DATA_PATH() + "/orgin"));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    partition::IndexPartitionPtr indexPart = psm.GetIndexPartition();
    partition::OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);

    // TODO: check compress file & check empty posting
    PartitionDataPtr partitionData = onlinePart->GetPartitionData();
    auto version = partitionData->GetVersion();
    SegmentData segData = partitionData->GetSegmentData(version[0]);
    file_system::DirectoryPtr indexDir = segData.GetIndexDirectory("index", true);
    auto compressInfo = indexDir->GetCompressFileInfo(POSTING_FILE_NAME);
    ASSERT_TRUE(compressInfo);
    ASSERT_EQ(string("zstd"), compressInfo->compressorName);

    compressInfo = indexDir->GetCompressFileInfo(DICTIONARY_FILE_NAME);
    ASSERT_TRUE(compressInfo);
    ASSERT_EQ(string("zstd"), compressInfo->compressorName);

    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);
    PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
    ASSERT_TRUE(iter);
    checkIterator(iter);
    delete iter;
}

void NormalIndexReaderTest::TestDocListDictInline()
{
    // string field = "string1:string;pk:string";
    // string index = "pk:primarykey64:pk;index:string:string1";
    string field = "string1:string";
    string index = "index:string:string1";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    ASSERT_TRUE(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE);
    ASSERT_TRUE(indexConfig->SetIndexFormatVersionId(1).IsOK()); // enable doclist dictinline

    map<string, int32_t> termMap;
    termMap["A"] = 10;
    termMap["B"] = 128;
    termMap["C"] = 1000;

    string docString;
    for (auto iter = termMap.begin(); iter != termMap.end(); iter++) {
        for (int i = 0; i < iter->second; i++) {
            docString += string("cmd=add,string1=") + iter->first + ";";
        }
    }

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    partition::IndexPartitionPtr indexPart = psm.GetIndexPartition();
    partition::OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);
    PartitionDataPtr partitionData = onlinePart->GetPartitionData();

    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);
    docid_t beginDocId = 0;
    for (auto it = termMap.begin(); it != termMap.end(); it++) {
        Term term(it->first, "index");
        PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
        ASSERT_EQ(0, iter->GetPostingLength());
        ASSERT_TRUE(iter);
        ASSERT_EQ((df_t)it->second, iter->GetTermMeta()->GetDocFreq());
        ASSERT_EQ((termpayload_t)0, iter->GetTermMeta()->GetPayload());
        docid_t docId = INVALID_DOCID;
        for (int i = 0; i < it->second; i++) {
            docId = iter->SeekDoc(docId);
            ASSERT_EQ(docId, beginDocId + i);
        }
        docId = iter->SeekDoc(docId);
        ASSERT_EQ(docId, INVALID_DOCID);
        delete iter;
        beginDocId += it->second;
    }
}

void NormalIndexReaderTest::TestCompressedPostingHeader()
{
    _options.SetEnablePackageFile(false);

    string field = "string1:string;";
    string index = "index:string:string1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    ASSERT_TRUE(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE);

    string docString = "cmd=add,string1=A;"
                       "cmd=add,string1=B;"
                       "cmd=add,string1=A;"
                       "cmd=add,string1=B;"
                       "cmd=add,string1=A;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    partition::IndexPartitionPtr indexPart = psm.GetIndexPartition();
    partition::OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);
    PartitionDataPtr partitionData = onlinePart->GetPartitionData();

    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);
    {
        Term term("A", "index");
        PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
        ASSERT_TRUE(iter);
        ASSERT_EQ((df_t)3, iter->GetTermMeta()->GetDocFreq());
        ASSERT_EQ((termpayload_t)0, iter->GetTermMeta()->GetPayload());
        docid_t expectDocId[] = {0, 2, 4, INVALID_DOCID};
        docid_t docId = INVALID_DOCID;
        for (size_t i = 0; i < 4; ++i) {
            docId = iter->SeekDoc(docId);
            ASSERT_EQ(docId, expectDocId[i]);
        }
        delete iter;
    }
    {
        Term term("B", "index");
        PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
        ASSERT_TRUE(iter);
        ASSERT_EQ((df_t)2, iter->GetTermMeta()->GetDocFreq());
        ASSERT_EQ((termpayload_t)0, iter->GetTermMeta()->GetPayload());
        docid_t expectDocId[] = {1, 3, INVALID_DOCID};
        docid_t docId = INVALID_DOCID;
        for (size_t i = 0; i < 3; ++i) {
            docId = iter->SeekDoc(docId);
            ASSERT_EQ(docId, expectDocId[i]);
        }
        delete iter;
    }
}

void NormalIndexReaderTest::TestCompressedPostingHeaderForCompatible()
{
    _options.SetEnablePackageFile(false);

    string indexDir = GET_PRIVATE_TEST_DATA_PATH() + "/compatible_index_for_posting_header_compress/";
    string rootDir = GET_TEMP_DATA_PATH();

    std::vector<std::string> fileList;
    FslibWrapper::ListDirE(indexDir, fileList);
    for (const string& name : fileList) {
        ASSERT_EQ(FSEC_OK, FslibWrapper::Copy(indexDir + name, rootDir + name).Code());
    }

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadAndRewritePartitionSchema(GET_PARTITION_DIRECTORY(), _options);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, rootDir));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index:A", "docid=0;docid=2;docid=4;"));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "cmd=add,string1=A;cmd=add,string1=A;", "index:A",
                             "docid=0;docid=2;docid=4;docid=5;docid=6;"));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "index:A", "docid=0;docid=2;docid=4;docid=5;docid=6;"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,string1=A,ts=10;", "index:A",
                             "docid=0;docid=2;docid=4;docid=5;docid=6;docid=7"));
}

void NormalIndexReaderTest::TestIndexWithReferenceCompressWithMerge()
{
    _options.SetEnablePackageFile(false);
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "text", SFP_INDEX);
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE); // TODO: delete
    indexConfig->SetIsReferenceCompress(true);

    string shortListDocStr = "A B,A B,A B"; // 3
    string compressedDocStr;
    for (int i = 0; i < 128; ++i) {
        compressedDocStr += "A B,"; // 128
    }
    string dictinlineDocStr = "A C"; // 1
    string skipListDocStr;
    for (int i = 0; i < 128 * 5; ++i) {
        skipListDocStr += "A D,"; // 128 * 5
    }
    string compressedSkipListDocStr;
    for (int i = 0; i < 128 * 40; ++i) {
        compressedSkipListDocStr += "A B,"; // 128 * 40
    }

    string docStr = shortListDocStr + "#" + compressedDocStr + "#" + dictinlineDocStr + "#" + skipListDocStr + "#" +
                    compressedSkipListDocStr;

    docid_t docSize = 3 + 128 + 1 + 128 * 5 + 128 * 40;

    provider.Build(docStr, SFP_OFFLINE);

    provider.Merge("0,1,2,3,4");
    PartitionDataPtr partitionData = provider.GetPartitionData();
    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);
    {
        Term term("A", "index");
        PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
        ASSERT_TRUE(iter);
        for (docid_t i = 0; i < docSize; i++) {
            EXPECT_EQ((docid_t)i, iter->SeekDoc(i));
        }
        ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(docSize));
        delete iter;
        iter = NULL;
    }
    {
        Term term("C", "index");
        PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
        ASSERT_TRUE(iter);
        EXPECT_EQ((docid_t)(3 + 128), iter->SeekDoc(0));
        ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(3 + 128 + 1));
        delete iter;
        iter = NULL;
    }
}

void NormalIndexReaderTest::TestIndexWithReferenceCompressForBuildingSegment()
{
    _options.SetEnablePackageFile(false);
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "text", SFP_INDEX);
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE); // TODO: delete
    indexConfig->SetIsReferenceCompress(true);
    provider.Build("A B,A B,A B,A B,A B,A C,D", SFP_OFFLINE);
    stringstream ss;
    for (size_t i = 0; i < 129; i++) {
        ss << "a b,";
    }

    provider.Build(ss.str(), SFP_REALTIME);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);
    Term term("a", "index");
    PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
    ASSERT_TRUE(iter);
    // test flush
    docid_t rtBaseDocid = 7;
    for (docid_t i = 0; i < 129; i++) {
        EXPECT_EQ((docid_t)i + rtBaseDocid, iter->SeekDoc(i));
    }
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(7 + rtBaseDocid));
    delete iter;
    iter = NULL;
}

void NormalIndexReaderTest::TestIndexWithReferenceCompress()
{
    _options.SetEnablePackageFile(false);
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "text", SFP_INDEX);
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("index");
    indexConfig->SetOptionFlag(OPTION_FLAG_NONE); // TODO: delete
    indexConfig->SetIsReferenceCompress(true);
    provider.Build("A B,A B,A B,A B,A B,A C,D", SFP_OFFLINE);
    provider.Build("a b,a b,a b,a b,a b,a c,d", SFP_REALTIME);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    NormalIndexReader reader;
    reader.Open(indexConfig, partitionData);
    Term term("A", "index");
    PostingIterator* iter = reader.Lookup(term).ValueOrThrow();
    ASSERT_TRUE(iter);
    // test compress
    for (docid_t i = 0; i < 6; i++) {
        EXPECT_EQ((docid_t)i, iter->SeekDoc(i));
    }
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(7));
    delete iter;
    iter = NULL;
    Term term1("B", "index");
    iter = reader.Lookup(term1).ValueOrThrow();
    ASSERT_TRUE(iter);
    // test short list
    for (docid_t i = 0; i < 5; i++) {
        EXPECT_EQ((docid_t)i, iter->SeekDoc(i));
    }
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(6));
    delete iter;
    iter = NULL;
    // test dict inline
    Term term2("C", "index");
    iter = reader.Lookup(term2).ValueOrThrow();
    ASSERT_TRUE(iter);
    ASSERT_EQ((docid_t)5, iter->SeekDoc(0));
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(6));
    delete iter;
    iter = NULL;
    // test realtime
    docid_t rtBaseDocid = 7;
    Term term3("a", "index");
    iter = reader.Lookup(term3).ValueOrThrow();
    ASSERT_TRUE(iter);
    // test short buffer
    for (docid_t i = 0; i < 6; i++) {
        EXPECT_EQ((docid_t)i + rtBaseDocid, iter->SeekDoc(i + rtBaseDocid));
    }
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(7 + rtBaseDocid));
    delete iter;

    Term term4("b", "index");
    iter = reader.Lookup(term4).ValueOrThrow();
    ASSERT_TRUE(iter);
    for (docid_t i = 0; i < 5; i++) {
        EXPECT_EQ((docid_t)i + rtBaseDocid, iter->SeekDoc(i + rtBaseDocid));
    }
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(6 + rtBaseDocid));
    delete iter;

    Term term5("c", "index");
    iter = reader.Lookup(term5).ValueOrThrow();
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
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);

    string field = "pk:uint64:pk;string1:string;text1:text;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "long1", "");

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
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
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);

    string field = "pk:uint64;string1:string;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "long1", "");

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    string fullDocs;
    string expectedResult;
    for (size_t i = 1; i <= 5000; ++i) {
        string id = StringUtil::toString(i);
        string docId = StringUtil::toString(i - 1);
        string singleDocStr = string("cmd=add,pk=") + id + string(",string1=A,long1=") + id + string(";");
        fullDocs += singleDocStr;
        expectedResult += (string("docid=") + docId + string(",long1=") + id + string(";"));
    }
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "index1:A", expectedResult));
}

void NormalIndexReaderTest::TestLookupWithMultiInMemSegments()
{
    // TODO: @qignran disable FakePartitionDataCreator ut
    return;
    string field = "pk:uint64;string1:string:true;text1:text;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "long1", "");
    SchemaMaker::EnableNullFields(schema, "string1");

    std::shared_ptr<DictionaryConfig> dictConfig = schema->AddDictionaryConfig("hf_dict", "A;B;C;__NULL__");

    // set bitmap
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");
    assert(indexConfig);
    indexConfig->SetDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(hp_both);

    FakePartitionDataCreator pdc;
    pdc.Init(schema, _options, GET_TEMP_DATA_PATH());

    string fullDocs = "cmd=add,pk=1,string1=A,long1=1;"
                      "cmd=add,pk=2,string1=B __NULL__,long1=2;";
    pdc.AddFullDocs(fullDocs);

    string builtRtDocs = "cmd=add,pk=3,string1=B,long1=3,ts=0;"
                         "cmd=add,pk=4,string1=A,long1=4,ts=0;"
                         "cmd=add,pk=44,string1=__NULL__,long1=44,ts=0;";
    pdc.AddBuiltSegment(builtRtDocs);

    string inMemSegment1 = "cmd=add,pk=5,string1=B,long1=5,ts=0;"
                           "cmd=add,pk=6,string1=A,long1=6,ts=0;";
    pdc.AddInMemSegment(inMemSegment1);

    string inMemSegment2 = "cmd=add,pk=7,string1=B,long1=7,ts=0;"
                           "cmd=add,pk=8,string1=A __NULL__,long1=8,ts=0;"
                           "cmd=add,pk=9,string1=C,long1=9,ts=0;"
                           "cmd=add,pk=99,string1=__NULL__,long1=99,ts=0;";
    pdc.AddInMemSegment(inMemSegment2);

    string buildingDocs = "cmd=add,pk=10,string1=A,long1=10,ts=0;"
                          "cmd=add,pk=11,string1=C,long1=11,ts=0;"
                          "cmd=add,pk=12,string1=__NULL__,long1=11,ts=0;";
    pdc.AddBuildingData(buildingDocs);

    PartitionDataPtr partData = pdc.CreatePartitionData(false);
    NormalIndexReaderPtr indexReader(new NormalIndexReader);
    indexReader->Open(indexConfig, partData);

    // check normal
    CheckPostingIterator(indexReader, pt_normal, "index1:A", "0,3,6,8,11");
    CheckPostingIterator(indexReader, pt_normal, "index1:B", "1,2,5,7");
    CheckPostingIterator(indexReader, pt_normal, "index1:C", "9,12");
    CheckPostingIterator(indexReader, pt_normal, "index1", "4,10,13");      // check null term
    CheckPostingIterator(indexReader, pt_normal, "index1:__NULL__", "1,8"); // check normal term

    // check bitmap
    CheckPostingIterator(indexReader, pt_bitmap, "index1:A", "0,3,6,8,11");
    CheckPostingIterator(indexReader, pt_bitmap, "index1:B", "1,2,5,7");
    CheckPostingIterator(indexReader, pt_bitmap, "index1:C", "9,12");
    CheckPostingIterator(indexReader, pt_bitmap, "index1", "4,10,13");      // check null term
    CheckPostingIterator(indexReader, pt_bitmap, "index1:__NULL__", "1,8"); // check normal term
}

void NormalIndexReaderTest::TestPartialLookup()
{
    string field = "pk:uint64;string1:string;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "long1", "");
    SchemaMaker::EnableNullFields(schema, "string1");
    std::shared_ptr<DictionaryConfig> dictConfig = schema->AddDictionaryConfig("hf_dict", "A;B;C;__NULL__");

    // set bitmap
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");
    assert(indexConfig);
    indexConfig->SetDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(hp_both);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, GET_TEMP_DATA_PATH()));

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
                     "cmd=add,pk=9,string1=B,long1=9,ts=9;"
                     "cmd=add,pk=10,string1=__NULL__,long1=10,ts=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs4, "", ""));

    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);
    PartitionDataPtr partitionData = onlinePart->GetPartitionData();

    // NormalIndexReaderPtr reader(new NormalIndexReader);
    // reader->Open(indexConfig, partitionData);

    auto partReader = onlinePart->GetReader();
    DocIdRangeVector ranges;
    ASSERT_TRUE(partReader->GetPartedDocIdRanges({{0, 1}, {1, 2}, {2, 3}, {4, 6}, {6, 8}}, 3, 0, ranges));
    ASSERT_EQ(3u, ranges.size());
    EXPECT_EQ(0, ranges[0].first);
    EXPECT_EQ(1, ranges[0].second);
    EXPECT_EQ(1, ranges[1].first);
    EXPECT_EQ(2, ranges[1].second);
    EXPECT_EQ(2, ranges[2].first);
    EXPECT_EQ(3, ranges[2].second);
    auto reader = partReader->GetInvertedIndexReader();
    CheckPostingIterator(reader, pt_normal, "index1:A", "0,3", ranges);

    CheckPostingIterator(reader, pt_normal, "index1:A", "3,6,8", {{2, 3}, {6, 9}});
    CheckPostingIterator(reader, pt_normal, "index1:A", "0,3,4,", {{0, 1}, {3, 6}});
    CheckPostingIterator(reader, pt_normal, "index1:B", "1,2,5", {{0, 1}, {3, 6}});
    CheckPostingIterator(reader, pt_normal, "index1:A", "0,8,", {{0, 1}, {8, 9}});
    CheckPostingIterator(reader, pt_normal, "index1:B", "1,9,", {{0, 1}, {8, 9}});

    CheckPostingIterator(reader, pt_normal, "index1:B", "1,2,5", {{0, 1}, {3, 4}, {5, 6}});
    CheckPostingIterator(reader, pt_normal, "index1", "10", {{9, 10}});
}

void NormalIndexReaderTest::TestOpenWithHintReader()
{
    string field = "pk:uint64;string1:string;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "long1", "");
    SchemaMaker::EnableNullFields(schema, "string1");
    std::shared_ptr<DictionaryConfig> dictConfig = schema->AddDictionaryConfig("hf_dict", "A;B;C;__NULL__");

    // set bitmap
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");
    assert(indexConfig);
    indexConfig->SetDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(hp_both);

    _options.GetBuildConfig(false).maxDocCount = 2;
    _options.GetBuildConfig(true).maxDocCount = 2;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, GET_TEMP_DATA_PATH()));

    string fullDocs0 = "cmd=add,pk=0,string1=A,long1=0;"
                       "cmd=add,pk=1,string1=B,long1=1;"
                       "cmd=add,pk=2,string1=C,long1=2;"
                       "cmd=add,pk=3,string1=D,long1=3;"
                       "cmd=add,pk=4,string1=E,long1=4;";
    string rtDocs = "cmd=add,pk=0,string1=A,long1=0;"
                    "cmd=add,pk=1,string1=B,long1=1;"
                    "cmd=add,pk=2,string1=C,long1=2;"
                    "cmd=add,pk=3,string1=D,long1=3;"
                    "cmd=add,pk=4,string1=E,long1=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs0, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "", ""));

    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);
    PartitionDataPtr partitionData = onlinePart->GetPartitionData();
    NormalIndexReaderPtr hintReader(new NormalIndexReader);
    hintReader->Open(indexConfig, partitionData);
    PartitionDataPtr newPartData(partitionData->Clone());
    size_t segmentCount = newPartData->GetVersion().GetSegmentCount();
    ASSERT_TRUE(segmentCount > 2);
    newPartData->RemoveSegments({newPartData->GetVersion()[segmentCount - 2]});

    NormalIndexReaderPtr targetReader(new NormalIndexReader);
    targetReader->Open(indexConfig, newPartData, hintReader.get());
    auto& targetSegReaders = targetReader->mSegmentReaders;
    docid_t next = 0;
    for (auto segReader : targetSegReaders) {
        const index_base::SegmentData& segData = segReader->GetSegmentData();
        ASSERT_EQ(next, segData.GetBaseDocId());
        next += segData.GetSegmentInfo()->docCount;
    }
}

void NormalIndexReaderTest::TestLookupWithIoException()
{
    using ErrorGenerator = fslib::fs::ErrorGenerator;
    fslib::fs::FileSystem::_useMock = true;
    IndexPartitionOptions options;
    string jsonStr = R"(
    {"load_config" : [{
         "file_patterns" : ["_INDEX_"],
         "load_strategy" : "cache",
         "load_strategy_param" : { "globa_cache" : false, "block_size":4096 , "memory_size_in_mb":0}}]}
    )";
    FromJsonString(options.GetOnlineConfig().loadConfigList, jsonStr);
    options.SetEnablePackageFile(false);
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);

    string field = "pk:uint64;string1:string;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "long1", "");

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

    string fullDocs;
    for (size_t i = 1; i <= 5000; ++i) {
        string id = StringUtil::toString(i);
        string docId = StringUtil::toString(i - 1);
        string singleDocStr = string("cmd=add,pk=") + id + string(",string1=A,long1=") + id + string(";");
        fullDocs += singleDocStr;
    }
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, indexPart);

    auto partReader = onlinePart->GetReader();
    index::Term term("A", "index1");
    auto reader = partReader->GetInvertedIndexReader();
    autil::mem_pool::Pool sessionPool;
    auto errorGenerator = ErrorGenerator::getInstance();
    ErrorGenerator::FileSystemErrorMap errorMap;
    fslib::fs::FileSystemError ec;
    ec.ec = fslib::ErrorCode::EC_BADARGS;
    ec.offset = 0;
    ec.until = 0;
    string fullFilePath = GET_TEMP_DATA_PATH() + "segment_0_level_0/index/index1/dictionary";
    cout << "path is " << fullFilePath << endl;
    errorMap.insert({{fullFilePath, "pread"}, ec});
    errorGenerator->setErrorMap(errorMap);
    auto postingResult =
        future_lite::coro::syncAwait(reader->LookupAsync(&term, 1000, pt_default, &sessionPool, nullptr));
    EXPECT_FALSE(postingResult.Ok());
    fslib::fs::FileSystem::_useMock = false;
}

void NormalIndexReaderTest::CheckPostingIterator(const std::shared_ptr<InvertedIndexReader>& indexReader,
                                                 PostingType postingType, const string& termInfoStr,
                                                 const string& docIdListStr, const DocIdRangeVector& ranges)
{
    vector<string> termInfo;
    StringUtil::fromString(termInfoStr, termInfo, ":");

    Term term(termInfo[0]);
    if (termInfo.size() > 1) {
        term.SetWord(termInfo[1]);
    }

    Pool pool;
    PostingIterator* postingIter = nullptr;
    if (ranges.empty()) {
        postingIter = indexReader->Lookup(term, 1000, postingType, &pool).ValueOrThrow();
    } else {
        postingIter = indexReader->PartialLookup(term, ranges, 1000, postingType, &pool).ValueOrThrow();
    }

    ASSERT_TRUE(postingIter) << termInfoStr;

    vector<docid_t> docIds;
    StringUtil::fromString(docIdListStr, docIds, ",");
    docid_t curDocId = INVALID_DOCID;
    for (size_t i = 0; i < docIds.size(); i++) {
        curDocId = postingIter->SeekDoc(curDocId);
        ASSERT_EQ(docIds[i], curDocId);
    }

    ASSERT_EQ(INVALID_DOCID, postingIter->SeekDoc(curDocId));
    IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, postingIter);
}
}} // namespace indexlib::index
