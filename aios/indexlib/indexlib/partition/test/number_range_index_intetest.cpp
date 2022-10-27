#include "indexlib/partition/test/number_range_index_intetest.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/searcher.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/common/number_term.h"
#include "indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, NumberRangeIndexInteTest);

NumberRangeIndexInteTest::NumberRangeIndexInteTest()
{
}

NumberRangeIndexInteTest::~NumberRangeIndexInteTest()
{
}

void NumberRangeIndexInteTest::CaseSetUp()
{
    string field = "price:int64;pk:string";
    string index = "price:range:price;pk:primarykey64:pk";
    string attribute = "pk;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mOptions.GetOnlineConfig().SetEnableRedoSpeedup(true);
    mOptions.GetOnlineConfig().SetVersionTsAlignment(1);
    mRootDir = GET_TEST_DATA_PATH();
}

void NumberRangeIndexInteTest::CaseTearDown()
{
}

void NumberRangeIndexInteTest::TestSimpleProcess()
{
    string fullDocString = "cmd=add,price=100,pk=a,ts=1;"
                           "cmd=add,price=15160963460000000,pk=b,ts=1";
    string rtDocString1 = "cmd=add,price=101,pk=c,ts=2;";
    string rtDocString2 ="cmd=add,price=15160000000000001,pk=d,ts=2";
    string rtDocString3 = "cmd=add,price=102,pk=c,ts=2";
    string incDocString = "cmd=add,price=99,pk=e,ts=3;"
                          "cmd=add,price=15100000000000000,pk=f,ts=3";
    string incDocString2 = "cmd=add,price=-1,pk=h,ts=3;"
                           "cmd=add,price=-10,pk=i,ts=4";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString,
                                    "price:[100, 101]",
                                    "docid=0,pk=a;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString1,
                                    "price:[100,101]",
                                    "docid=0,pk=a;docid=2,pk=c;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2,
                                    "price:[102,)",
                                    "docid=1,pk=b;docid=3,pk=d;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString3,
                                    "price:[100,102]",
                                    "docid=0,pk=a;docid=4,pk=c"));
    IndexPartitionReaderPtr reader = psm.mIndexPartition->GetReader();
    InnerTestSeekDocCount(reader);
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString,
                                    "price:[0,100)",
                                    "docid=2,pk=e;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString2,
                                    "price:(-10,100)",
                                    "docid=2,pk=e;docid=4,pk=h;"));
}


void NumberRangeIndexInteTest::InnerTestSeekDocCount(const IndexPartitionReaderPtr& reader)
{
    SeekAndFilterIteratorPtr iter1(
            CreateRangeIteratorWithSeek(reader, 100, 102));
    RangePostingIterator* rangeIter =
        dynamic_cast<RangePostingIterator*>(iter1->GetIndexIterator());
    ASSERT_TRUE(rangeIter->GetSeekDocCount() > 1);
    
    SeekAndFilterIteratorPtr iter2(
            CreateRangeIteratorWithSeek(reader, 102, numeric_limits<int64_t>::max()));
    rangeIter =
        dynamic_cast<RangePostingIterator*>(iter2->GetIndexIterator());
    ASSERT_TRUE(rangeIter->GetSeekDocCount() > 1);
}

SeekAndFilterIterator* NumberRangeIndexInteTest::CreateRangeIteratorWithSeek(const IndexPartitionReaderPtr& reader,
                                                                            int64_t left, int64_t right)
{
    common::Int64Term term(left, true, right, true, "price");
    auto indexReader = reader->GetIndexReader();
    auto deletionMapReader = reader->GetDeletionMapReader();
    PostingIterator* iter = indexReader->Lookup(term);
    auto rangeIter = dynamic_cast<SeekAndFilterIterator*>(iter);
    assert(rangeIter);
    docid_t docid = INVALID_DOCID;
    do
    {
        docid = rangeIter->SeekDoc(docid);
    }
    while (docid != INVALID_DOCID);   
    return rangeIter;
}

void NumberRangeIndexInteTest::TestRecoverFailed()
{
    string fullDocString = "cmd=add,price=100,pk=a,ts=1;"
                           "cmd=add,price=15160963460000000,pk=b,ts=1;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString,"", ""));

    auto mergerCreator =
        [] (const string& path, const IndexPartitionOptions& options)
        -> IndexPartitionMergerPtr {
        ParallelBuildInfo parallelBuildInfo;
        parallelBuildInfo.parallelNum = 1;
            return IndexPartitionMergerPtr(
                (IndexPartitionMerger*)PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    path, parallelBuildInfo, options, NULL, ""));
    };

    IndexPartitionMergerPtr merger = mergerCreator(mRootDir, mOptions);
    merger->PrepareMerge(0);
    auto mergeMeta = merger->CreateMergeMeta(true, 1, 0);

    string path = PathUtil::JoinPath(mRootDir, "/instance_0/segment_1_level_0/index/price/");
    string rangeInfo = PathUtil::JoinPath(path, "range_info");
    FileSystemWrapper::AtomicStore(rangeInfo, "");
    string targetVersionFile = PathUtil::JoinPath(mRootDir, "instance_0/target_version");
    FileSystemWrapper::Copy(PathUtil::JoinPath(TEST_DATA_PATH, 
                    "number_range_target_version"), targetVersionFile);
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
    vector<string> queryStrings = {
        "price:[-9223372036854775808, -1]",
        "price:(-9223372036854775808, -1]",
        "price:(, 0]",
        "price:(0, 1)",
        "price:(0, 3)",
        "price:[0, 3]",
        "price:[0, 3)",
        "price:[0, 9223372036854775807)",
        "price:[9223372036854775807,)",
        "price:[-9223372036854775808,)"
    };
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
    for (string& s: signedFieldTypes)
    {
        string field = "price:" + s + ";pk:string";
        mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
        InnerTestInterval(docStrings1, "", "", queryStrings1, resultStrings);
        InnerTestInterval("", docStrings1, docStrings1 + "##stopTs=2;", queryStrings1, resultStrings);
    }
    
    for (string& s: unsignedFieldTypes)
    {
        string field = "price:" + s + ";pk:string";
        mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
        InnerTestInterval(docStrings2, "", "", queryStrings2, resultStrings);
        InnerTestInterval("", docStrings2, docStrings2 + "##stopTs=3;", queryStrings2, resultStrings);
    }

}

void NumberRangeIndexInteTest::InnerTestInterval(
        const string& fullDocString,
        const string& rtDocString,
        const string& incDocString,
        const vector<string>& queryStrings,
        const vector<string>& resultStrings)
{
    static int generation = 0;
    generation ++;    
    ASSERT_EQ(queryStrings.size(), resultStrings.size());
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir + "generation_" +
                                StringUtil::toString(generation)));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    if (incDocString.empty())
    {
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, rtDocString, "", ""));
    }
    else
    {
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocString, "", ""));
    }
    
    for (size_t i = 0; i < queryStrings.size(); i ++)
    {
        ASSERT_TRUE(psm.Transfer(QUERY, "", queryStrings[i], resultStrings[i]))
            << "=========\nquery: " << queryStrings[i] << "\nresult: "
            << resultStrings[i] << endl;
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
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString,
                                    "price:[100, 101]",
                                    "docid=0,pk=a;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString1,
                                    "price:[100,101]",
                                    "docid=0,pk=a;docid=2,pk=c;"));
    // disable price
    psm.GetSchema()->GetIndexSchema()->DisableIndex("price");
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "price:[100, 101]", ""));
}

IE_NAMESPACE_END(partition);

