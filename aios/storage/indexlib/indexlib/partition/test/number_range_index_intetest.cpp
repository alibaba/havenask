#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/index/inverted_index/SeekAndFilterIterator.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/test/searcher.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::merger;
using namespace indexlib::index_base;
using namespace indexlibv2::index;

namespace indexlib { namespace partition {

class NumberRangeIndexInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<std::tuple<bool, int>>
{
public:
    NumberRangeIndexInteTest();
    ~NumberRangeIndexInteTest();

    DECLARE_CLASS_NAME(NumberRangeIndexInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestInterval();
    void TestFieldType();
    void TestRecoverFailed();
    void TestDisableRange();
    void TestMultiValueRangeIndex();

private:
    void InnerTestSeekDocCount(const IndexPartitionReaderPtr& reader);
    void InnerTestInterval(const std::string& fullDocString, const std::string& rtDocString,
                           const std::string& incDcoString, const std::vector<std::string>& queryStrings,
                           const std::vector<std::string>& resultStrings);
    SeekAndFilterIterator* CreateRangeIteratorWithSeek(const IndexPartitionReaderPtr& reader, int64_t left,
                                                       int64_t right);

    void CheckRangeIndexCompressInfo(const file_system::IFileSystemPtr& fileSystem);

    void CheckFileCompressInfo(const index_base::PartitionDataPtr& partitionData, const std::string& filePath,
                               const std::string& compressType);

private:
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(partition, NumberRangeIndexInteTest);

NumberRangeIndexInteTest::NumberRangeIndexInteTest() {}

NumberRangeIndexInteTest::~NumberRangeIndexInteTest() {}

void NumberRangeIndexInteTest::CaseSetUp()
{
    string field = "price:int64;pk:string";
    string index = "price:range:price;pk:primarykey64:pk";
    string attribute = "pk;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GET_PARAM_VALUE(1), &mOptions);
    mOptions.GetOnlineConfig().SetEnableRedoSpeedup(true);
    mOptions.GetOnlineConfig().SetVersionTsAlignment(1);
    mRootDir = GET_TEMP_DATA_PATH();
    bool enableFileCompress = GET_PARAM_VALUE(0);
    if (enableFileCompress) {
        string compressorStr = "compressor:zstd;";
        string indexCompressStr = "price:compressor";
        string attrCompressStr = "price:compressor";
        mSchema = SchemaMaker::EnableFileCompressSchema(mSchema, compressorStr, indexCompressStr, attrCompressStr);
    }
}

void NumberRangeIndexInteTest::CaseTearDown() {}

void NumberRangeIndexInteTest::TestSimpleProcess()
{
    string fullDocString = "cmd=add,price=100,pk=a,ts=1;"
                           "cmd=add,price=15160963460000000,pk=b,ts=1";
    string rtDocString1 = "cmd=add,price=101,pk=c,ts=2;";
    string rtDocString2 = "cmd=add,price=15160000000000001,pk=d,ts=2";
    string rtDocString3 = "cmd=add,price=102,pk=c,ts=2";
    string incDocString = "cmd=add,price=99,pk=e,ts=3;"
                          "cmd=add,price=15100000000000000,pk=f,ts=3";
    string incDocString2 = "cmd=add,price=-1,pk=h,ts=3;"
                           "cmd=add,price=-10,pk=i,ts=4";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "price:[100, 101]", "docid=0,pk=a;"));
    CheckRangeIndexCompressInfo(psm.GetFileSystem());

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString1, "price:[100,101]", "docid=0,pk=a;docid=2,pk=c;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "price:[102,)", "docid=1,pk=b;docid=3,pk=d;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString3, "price:[100,102]", "docid=0,pk=a;docid=4,pk=c"));
    IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    InnerTestSeekDocCount(reader);
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "price:[0,100)", "docid=2,pk=e;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString2, "price:(-10,100)", "docid=2,pk=e;docid=4,pk=h;"));
}

void NumberRangeIndexInteTest::TestMultiValueRangeIndex()
{
    string field = "price:int64:true;pk:string";
    string index = "price:range:price;pk:primarykey64:pk";
    string attribute = "pk;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GET_PARAM_VALUE(1), &mOptions);
    mOptions.GetOnlineConfig().SetEnableRedoSpeedup(true);
    mOptions.GetOnlineConfig().SetVersionTsAlignment(1);
    mRootDir = GET_TEMP_DATA_PATH();
    bool enableFileCompress = GET_PARAM_VALUE(0);
    if (enableFileCompress) {
        string compressorStr = "compressor:zstd;";
        string indexCompressStr = "price:compressor";
        string attrCompressStr = "price:compressor";
        mSchema = SchemaMaker::EnableFileCompressSchema(mSchema, compressorStr, indexCompressStr, attrCompressStr);
    }

    string fullDocString = "cmd=add,price=100 103,pk=a,ts=1;"
                           "cmd=add,price=15160963460000000,pk=b,ts=1";
    string rtDocString1 = "cmd=add,price=101 200,pk=c,ts=2;";
    string rtDocString2 = "cmd=add,price=15160000000000001,pk=d,ts=2";
    string rtDocString3 = "cmd=add,price=102,pk=c,ts=2";
    string incDocString = "cmd=add,price=99,pk=e,ts=3;"
                          "cmd=add,price=15100000000000000,pk=f,ts=3";
    string incDocString2 = "cmd=add,price=-1,pk=h,ts=3;"
                           "cmd=add,price=-10,pk=i,ts=4";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "price:[100, 101]", "docid=0,pk=a;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "price:[102, 103]", "docid=0,pk=a;"));
    CheckRangeIndexCompressInfo(psm.GetFileSystem());

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString1, "price:[100,101]", "docid=0,pk=a;docid=2,pk=c;"));
    INDEXLIB_TEST_TRUE(
        psm.Transfer(BUILD_RT, rtDocString2, "price:[102,)", "docid=0,pk=a;docid=1,pk=b;docid=2,pk=c;docid=3,pk=d;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString3, "price:[100,102]", "docid=0,pk=a;docid=4,pk=c"));
    IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    InnerTestSeekDocCount(reader);
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "price:[0,100)", "docid=2,pk=e;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString2, "price:(-10,100)", "docid=2,pk=e;docid=4,pk=h;"));
}

void NumberRangeIndexInteTest::InnerTestSeekDocCount(const IndexPartitionReaderPtr& reader)
{
    shared_ptr<SeekAndFilterIterator> iter1(CreateRangeIteratorWithSeek(reader, 100, 102));
    RangePostingIterator* rangeIter = dynamic_cast<RangePostingIterator*>(iter1->GetIndexIterator());
    ASSERT_TRUE(rangeIter->GetSeekDocCount() > 1);

    shared_ptr<SeekAndFilterIterator> iter2(CreateRangeIteratorWithSeek(reader, 102, numeric_limits<int64_t>::max()));
    rangeIter = dynamic_cast<RangePostingIterator*>(iter2->GetIndexIterator());
    ASSERT_TRUE(rangeIter->GetSeekDocCount() > 1);
}

SeekAndFilterIterator* NumberRangeIndexInteTest::CreateRangeIteratorWithSeek(const IndexPartitionReaderPtr& reader,
                                                                             int64_t left, int64_t right)
{
    index::Int64Term term(left, true, right, true, "price");
    auto indexReader = reader->GetInvertedIndexReader();
    auto deletionMapReader = reader->GetDeletionMapReader();
    PostingIterator* iter = indexReader->Lookup(term).ValueOrThrow();
    auto rangeIter = dynamic_cast<SeekAndFilterIterator*>(iter);
    assert(rangeIter != nullptr);
    docid_t docid = INVALID_DOCID;
    do {
        docid = iter->SeekDoc(docid);
    } while (docid != INVALID_DOCID);
    return rangeIter;
}

void NumberRangeIndexInteTest::TestRecoverFailed()
{
    string fullDocString = "cmd=add,price=100,pk=a,ts=1;"
                           "cmd=add,price=15160963460000000,pk=b,ts=1;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));
    CheckRangeIndexCompressInfo(psm.GetFileSystem());

    auto mergerCreator = [](const string& path, const IndexPartitionOptions& options) -> IndexPartitionMergerPtr {
        ParallelBuildInfo parallelBuildInfo;
        parallelBuildInfo.parallelNum = 1;
        return IndexPartitionMergerPtr(
            (IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateIncParallelPartitionMerger(
                path, parallelBuildInfo, options, NULL, ""));
    };

    IndexPartitionMergerPtr merger = mergerCreator(mRootDir, mOptions);
    merger->PrepareMerge(0);
    auto mergeMeta = merger->CreateMergeMeta(true, 1, 0);

    string path = PathUtil::JoinPath(mRootDir, "/instance_0/segment_1_level_0/index/price/");
    string rangeInfo = PathUtil::JoinPath(path, "range_info");
    FslibWrapper::AtomicStoreE(rangeInfo, "");
    string targetVersionFile = PathUtil::JoinPath(mRootDir, "instance_0/target_version");
    ASSERT_EQ(FSEC_OK,
              FslibWrapper::Copy(PathUtil::JoinPath(GET_PRIVATE_TEST_DATA_PATH(), "number_range_target_version"),
                                 targetVersionFile)
                  .Code());
    merger->DoMerge(false, mergeMeta, 0);
    merger->EndMerge(mergeMeta, mergeMeta->GetTargetVersion().GetVersionId());
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "price:[100, 101]", "docid=0,pk=a;"));
}

void NumberRangeIndexInteTest::TestInterval()
{
    string docStrings1 = "cmd=add,price=-9223372036854775808,pk=aaa,ts=1;"
                         "cmd=add,price=-1,pk=aa,ts=1;"
                         "cmd=add,price=0,pk=a,ts=1;"
                         "cmd=add,price=1,pk=b,ts=1;";
    string docStrings2 = "cmd=add,price=2,pk=c,ts=2;"
                         "cmd=add,price=3,pk=d,ts=2;"
                         "cmd=add,price=9223372036854775807,pk=e,ts=2;";
    string docStrings = docStrings1 + docStrings2;
    vector<string> queryStrings = {"price:[-9223372036854775808, -1]",
                                   "price:(-9223372036854775808, -1]",
                                   "price:(, 0]",
                                   "price:(0, 1)",
                                   "price:(0, 3)",
                                   "price:[0, 3]",
                                   "price:[0, 3)",
                                   "price:[0, 9223372036854775807)",
                                   "price:[9223372036854775807,)",
                                   "price:[-9223372036854775808,)"};
    vector<string> resultStrings = {
        "docid=0,pk=aaa;docid=1,pk=aa;",
        "docid=1,pk=aa;",
        "docid=0,pk=aaa;docid=1,pk=aa;docid=2,pk=a",
        "",
        "docid=3,pk=b;docid=4,pk=c;",
        "docid=2,pk=a;docid=3,pk=b;docid=4,pk=c;docid=5,pk=d;",
        "docid=2,pk=a;docid=3,pk=b;docid=4,pk=c;",
        "docid=2,pk=a;docid=3,pk=b;docid=4,pk=c;docid=5,pk=d;",
        "docid=6,pk=e;",
        "docid=0,pk=aaa;docid=1,pk=aa;docid=2,pk=a;"
        "docid=3,pk=b;docid=4,pk=c;docid=5,pk=d;docid=6,pk=e;",
    };
    InnerTestInterval(docStrings, "", "", queryStrings, resultStrings);
    InnerTestInterval("", docStrings, docStrings + "##stopTs=3;", queryStrings, resultStrings);
    InnerTestInterval(docStrings1, "", docStrings2, queryStrings, resultStrings);
    InnerTestInterval(docStrings1, docStrings2, docStrings2 + "##stopTs=3;", queryStrings, resultStrings);
}

void NumberRangeIndexInteTest::TestFieldType()
{
    string docStrings1 = "cmd=add,price=-1,pk=aa,ts=1;"
                         "cmd=add,price=0,pk=a,ts=1;"
                         "cmd=add,price=1,pk=b,ts=1;";
    string docStrings2 = "cmd=add,price=2,pk=aa,ts=2;"
                         "cmd=add,price=3,pk=a,ts=2;"
                         "cmd=add,price=4,pk=b,ts=2;";
    vector<string> queryStrings1 = {
        "price:(, -1]",
        "price:(, 1)",
        "price:(-1, 1]",
        "price:[-1, 1]",
    };
    vector<string> queryStrings2 = {
        "price:(, 2]",
        "price:(, 4)",
        "price:(2, 4]",
        "price:[2, 4]",
    };
    vector<string> resultStrings = {
        "docid=0,pk=aa;",
        "docid=0,pk=aa;docid=1,pk=a;",
        "docid=1,pk=a;docid=2,pk=b",
        "docid=0,pk=aa;docid=1,pk=a;docid=2,pk=b",
    };

    string index = "price:range:price;pk:primarykey64:pk";
    string attribute = "pk;price";
    vector<string> signedFieldTypes = {"int64", "int16", "int8", "int32"};
    vector<string> unsignedFieldTypes = {"uint16", "uint8", "uint32"};
    for (string& s : signedFieldTypes) {
        string field = "price:" + s + ";pk:string";
        mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
        if (GET_PARAM_VALUE(0)) {
            string compressorStr = "compressor:zstd;";
            string indexCompressStr = "price:compressor";
            string attrCompressStr = "price:compressor";
            mSchema = SchemaMaker::EnableFileCompressSchema(mSchema, compressorStr, indexCompressStr, attrCompressStr);
        }

        InnerTestInterval(docStrings1, "", "", queryStrings1, resultStrings);
        InnerTestInterval("", docStrings1, docStrings1 + "##stopTs=2;", queryStrings1, resultStrings);
    }

    for (string& s : unsignedFieldTypes) {
        string field = "price:" + s + ";pk:string";
        mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
        if (GET_PARAM_VALUE(0)) {
            string compressorStr = "compressor:zstd;";
            string indexCompressStr = "price:compressor";
            string attrCompressStr = "price:compressor";
            mSchema = SchemaMaker::EnableFileCompressSchema(mSchema, compressorStr, indexCompressStr, attrCompressStr);
        }

        InnerTestInterval(docStrings2, "", "", queryStrings2, resultStrings);
        InnerTestInterval("", docStrings2, docStrings2 + "##stopTs=3;", queryStrings2, resultStrings);
    }
}

void NumberRangeIndexInteTest::InnerTestInterval(const string& fullDocString, const string& rtDocString,
                                                 const string& incDocString, const vector<string>& queryStrings,
                                                 const vector<string>& resultStrings)
{
    static int generation = 0;
    generation++;
    ASSERT_EQ(queryStrings.size(), resultStrings.size());
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir + "generation_" + StringUtil::toString(generation)));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
    CheckRangeIndexCompressInfo(psm.GetFileSystem());

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    if (incDocString.empty()) {
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, rtDocString, "", ""));
    } else {
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "", ""));
    }

    for (size_t i = 0; i < queryStrings.size(); i++) {
        ASSERT_TRUE(psm.Transfer(QUERY, "", queryStrings[i], resultStrings[i]))
            << "=========\nquery: " << queryStrings[i] << "\nresult: " << resultStrings[i] << endl;
    }
}

void NumberRangeIndexInteTest::TestDisableRange()
{
    string fullDocString = "cmd=add,price=100,pk=a,ts=1;"
                           "cmd=add,price=15160963460000000,pk=b,ts=1";
    string rtDocString1 = "cmd=add,price=101,pk=c,ts=2;";
    string incDocString = "cmd=add,price=99,pk=e,ts=3;"
                          "cmd=add,price=15100000000000000,pk=f,ts=3";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "price:[100, 101]", "docid=0,pk=a;"));
    CheckRangeIndexCompressInfo(psm.GetFileSystem());

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString1, "price:[100,101]", "docid=0,pk=a;docid=2,pk=c;"));
    // disable price
    psm.GetSchema()->GetIndexSchema()->DisableIndex("price");
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "price:[100, 101]", ""));
}

void NumberRangeIndexInteTest::CheckRangeIndexCompressInfo(const file_system::IFileSystemPtr& fileSystem)
{
    auto partData = OnDiskPartitionData::CreateOnDiskPartitionData(fileSystem);
    if (GET_PARAM_VALUE(0)) {
        CheckFileCompressInfo(partData, "index/price/price_@_bottom/posting", "zstd");
        CheckFileCompressInfo(partData, "index/price/price_@_high/posting", "zstd");
        CheckFileCompressInfo(partData, "attribute/price/data", "zstd");
        CheckFileCompressInfo(partData, "index/price/price_@_bottom/dictionary", "zstd");
        CheckFileCompressInfo(partData, "index/price/price_@_high/dictionary", "zstd");
    } else {
        CheckFileCompressInfo(partData, "index/price/price_@_bottom/posting", "null");
        CheckFileCompressInfo(partData, "index/price/price_@_high/posting", "null");
        CheckFileCompressInfo(partData, "attribute/price/data", "null");
        CheckFileCompressInfo(partData, "index/price/price_@_bottom/dictionary", "null");
        CheckFileCompressInfo(partData, "index/price/price_@_high/dictionary", "null");
    }
}

void NumberRangeIndexInteTest::CheckFileCompressInfo(const PartitionDataPtr& partitionData, const string& filePath,
                                                     const string& compressType)
{
    auto partitionDataIter = partitionData->CreateSegmentIterator();
    auto iter = partitionDataIter->CreateIterator(SIT_BUILT);
    while (iter->IsValid()) {
        SegmentData segData = iter->GetSegmentData();
        DirectoryPtr segDir = segData.GetDirectory();
        auto compressInfo = segDir->GetCompressFileInfo(filePath);
        if (compressType == "null") {
            EXPECT_TRUE(compressInfo.get() == nullptr);
        } else {
            EXPECT_TRUE(compressInfo.get() != nullptr) << segDir->DebugString() << filePath;
            EXPECT_EQ(compressType, compressInfo->compressorName);
        }
        iter->MoveToNext();
    }
}

INSTANTIATE_TEST_CASE_P(NumberRangeIndexInteTestModeAndBuildMode, NumberRangeIndexInteTest,
                        testing::Combine(testing::Bool(), testing::Values(0, 1, 2)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NumberRangeIndexInteTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NumberRangeIndexInteTest, TestInterval);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NumberRangeIndexInteTest, TestFieldType);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NumberRangeIndexInteTest, TestRecoverFailed);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NumberRangeIndexInteTest, TestDisableRange);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(NumberRangeIndexInteTest, TestMultiValueRangeIndex);

}} // namespace indexlib::partition
