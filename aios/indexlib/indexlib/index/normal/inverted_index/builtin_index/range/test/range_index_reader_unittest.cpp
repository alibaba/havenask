#include "indexlib/index/normal/inverted_index/builtin_index/range/test/range_index_reader_unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/searcher.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/common/number_term.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, RangeIndexReaderTest);

RangeIndexReaderTest::RangeIndexReaderTest()
{
}

RangeIndexReaderTest::~RangeIndexReaderTest()
{
}

void RangeIndexReaderTest::CaseSetUp()
{
    string field = "price:int64;pk:string";
    string index = "price:range:price;pk:primarykey64:pk";
    string attribute = "pk;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mRootDir = GET_TEST_DATA_PATH();
}

void RangeIndexReaderTest::CaseTearDown()
{
}

void RangeIndexReaderTest::TestRangeIndexReaderTermIllegal()
{
    string fullDocString = "cmd=add,price=100,pk=a,ts=1;"
                           "cmd=add,price=15160963460000000,pk=b,ts=1";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString,
                                    "price:[100, 101]",
                                    "docid=0,pk=a;"));
    IndexPartitionPtr part = psm.GetIndexPartition();
    ASSERT_TRUE(part);
    auto partReader = part->GetReader();
    auto indexReader = partReader->GetIndexReader();
    Term term("128", "price");
    PostingIterator* iter = indexReader->Lookup(term);
    ASSERT_FALSE(iter);
}

void RangeIndexReaderTest::TestSimpleProcess()
{
    string fullDocString = "cmd=add,price=100,pk=a,ts=1;"
                           "cmd=add,price=15160963460000000,pk=b,ts=1";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, fullDocString,
                                    "price:[100, 101]",
                                    "docid=0,pk=a;"));
    partition::IndexPartitionPtr part = psm.GetIndexPartition();
    ASSERT_TRUE(part);
    auto partReader = part->GetReader();
    auto indexReader = partReader->GetIndexReader();
    Int64Term term(100, true, 101, true, "price");
    PostingIterator* iter = indexReader->Lookup(term);
    SeekAndFilterIterator* stIter = dynamic_cast<SeekAndFilterIterator*>(iter);
    ASSERT_TRUE(stIter);
    DocValueFilter* dvIter = stIter->GetDocValueFilter();
    ASSERT_TRUE(dvIter->Test(0));
    ASSERT_FALSE(dvIter->Test(1));
    delete iter;

    iter = indexReader->PartialLookup(term, {}, 1000, pt_default, nullptr);
    stIter = dynamic_cast<SeekAndFilterIterator*>(iter);
    ASSERT_TRUE(stIter);
    dvIter = stIter->GetDocValueFilter();
    ASSERT_TRUE(dvIter->Test(0));
    ASSERT_FALSE(dvIter->Test(1));
    delete iter;
}

IE_NAMESPACE_END(index);

