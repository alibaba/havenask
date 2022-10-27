#include "indexlib/index/normal/inverted_index/builtin_index/spatial/test/spatial_posting_iterator_unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h"
#include "indexlib/common/term.h"
#include "indexlib/test/single_field_partition_data_provider.h"


using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SpatialPostingIteratorTest);

SpatialPostingIteratorTest::SpatialPostingIteratorTest()
{
}

SpatialPostingIteratorTest::~SpatialPostingIteratorTest()
{
}

void SpatialPostingIteratorTest::CaseSetUp()
{
}

void SpatialPostingIteratorTest::CaseTearDown()
{
}

void SpatialPostingIteratorTest::TestSimpleProcess()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "location", SFP_INDEX);
    string docsStr = "100 10";
    provider.Build(docsStr, SFP_OFFLINE);

    SpatialIndexReaderPtr reader(new SpatialIndexReader);
    reader->Open(provider.GetIndexConfig(), provider.GetPartitionData());
    PostingIterator* iter = reader->Lookup(Term("point(100 10)", "spatial"));
    ASSERT_TRUE(iter);
    ASSERT_EQ((docid_t)0, iter->SeekDoc(INVALID_DOCID));
    ASSERT_EQ((docid_t)INVALID_DOCID, iter->SeekDoc(0));
    autil::mem_pool::Pool* pool = iter->GetSessionPool();
    IE_POOL_COMPATIBLE_DELETE_CLASS(pool, iter);
}

void SpatialPostingIteratorTest::TestMember()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "location", SFP_INDEX);
    string docsStr = "1 1,5 5,3 3,1 1,10 10";
    provider.Build(docsStr, SFP_OFFLINE);

    SpatialIndexReaderPtr reader(new SpatialIndexReader);
    reader->Open(provider.GetIndexConfig(), provider.GetPartitionData());
    PostingIterator* iter = reader->Lookup(Term("rectangle(1 1,3 3)", "spatial"));
    iter->GetSessionPool();

    SeekAndFilterIterator* seekAndFilterIter = dynamic_cast<SeekAndFilterIterator*>(iter);
    SpatialPostingIterator* spatialIter = 
        dynamic_cast<SpatialPostingIterator*>(seekAndFilterIter->GetIndexIterator());
    ASSERT_TRUE(spatialIter);
    ASSERT_EQ((docid_t)INVALID_DOCID, spatialIter->mCurDocid);
    ASSERT_EQ((size_t)2, spatialIter->mPostingIterators.size());
    ASSERT_EQ((size_t)2, spatialIter->mHeap.size());

    ASSERT_EQ((docid_t)0, iter->SeekDoc(INVALID_DOCID));
    ASSERT_EQ((size_t)2, spatialIter->mHeap.size());
    ASSERT_EQ((docid_t)0, spatialIter->mCurDocid);
    /* 0, 2, 3 */
    ASSERT_EQ((docid_t)2, iter->SeekDoc(INVALID_DOCID));
    //ASSERT_EQ((size_t)1, spatialIter->mHeap.size());
    ASSERT_EQ((docid_t)2, spatialIter->mCurDocid);

    ASSERT_EQ((docid_t)3, iter->SeekDoc(INVALID_DOCID));
    ASSERT_EQ((size_t)1, spatialIter->mHeap.size());
    ASSERT_EQ((docid_t)3, spatialIter->mCurDocid);
    autil::mem_pool::Pool* pool = iter->GetSessionPool();
    IE_POOL_COMPATIBLE_DELETE_CLASS(pool, iter);
}

void SpatialPostingIteratorTest::TestClone()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "location", SFP_INDEX);
    string docsStr = "1 1,5 5,3 3,1 1,10 10";
    provider.Build(docsStr, SFP_OFFLINE);

    SpatialIndexReaderPtr reader(new SpatialIndexReader);
    reader->Open(provider.GetIndexConfig(), provider.GetPartitionData());
    PostingIterator* iter = reader->Lookup(Term("rectangle(1 1,3 3)", "spatial"));
    ASSERT_EQ((docid_t)0, iter->SeekDoc(INVALID_DOCID));

    PostingIterator* iter2 = iter->Clone();
    ASSERT_EQ((docid_t)2, iter->SeekDoc(INVALID_DOCID));
    ASSERT_EQ((docid_t)0, iter2->SeekDoc(INVALID_DOCID));
    ASSERT_EQ((docid_t)3, iter->SeekDoc(INVALID_DOCID));
    ASSERT_EQ((docid_t)2, iter2->SeekDoc(INVALID_DOCID));
    ASSERT_EQ((docid_t)3, iter2->SeekDoc(INVALID_DOCID));
    autil::mem_pool::Pool* pool = iter->GetSessionPool();
    IE_POOL_COMPATIBLE_DELETE_CLASS(pool, iter);
    IE_POOL_COMPATIBLE_DELETE_CLASS(pool, iter2);
}

void SpatialPostingIteratorTest::TestUnpack()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "location", SFP_INDEX);
    string docsStr = "1 1,1 1,5 5,1 1";
    provider.Build("", SFP_OFFLINE);
    provider.Build(docsStr, SFP_REALTIME);

    SpatialIndexReaderPtr reader(new SpatialIndexReader);
    reader->Open(provider.GetIndexConfig(), provider.GetPartitionData());
    PostingIterator* iter = reader->Lookup(Term("point(1 1)", "spatial"));
    TermMatchData tmd;
    iter->Unpack(tmd);
    ASSERT_EQ((tf_t)1, tmd.GetTermFreq());
    ASSERT_EQ((fieldmap_t)0, tmd.GetFieldMap());
    ASSERT_EQ((docpayload_t)0, tmd.GetDocPayload());
    ASSERT_FALSE(tmd.GetInDocPositionState());
    ASSERT_FALSE(tmd.HasFieldMap());
    ASSERT_FALSE(tmd.HasDocPayload());
    ASSERT_TRUE(tmd.IsMatched());
    autil::mem_pool::Pool* pool = iter->GetSessionPool();
    IE_POOL_COMPATIBLE_DELETE_CLASS(pool, iter);
}

void SpatialPostingIteratorTest::TestSeekDocCount()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "location", SFP_INDEX);
    string docsStr = "1 1,5 5,3 3,1 1,10 10";
    provider.Build(docsStr, SFP_OFFLINE);

    SpatialIndexReaderPtr reader(new SpatialIndexReader);
    reader->Open(provider.GetIndexConfig(), provider.GetPartitionData());
    PostingIterator* iter = reader->Lookup(Term("rectangle(1 1,3 3)", "spatial"));
    SeekAndFilterIterator* seekAndFilterIter =
        dynamic_cast<SeekAndFilterIterator*>(iter);
    SpatialPostingIterator* spatialIter =
        dynamic_cast<SpatialPostingIterator*>(
                seekAndFilterIter->GetIndexIterator());
    ASSERT_TRUE(spatialIter);
    //seek count 1
    ASSERT_EQ(0, spatialIter->SeekDoc(-1));
    //seek count 3
    ASSERT_EQ(2, spatialIter->SeekDoc(0));
    //seek count 5
    ASSERT_EQ(3, spatialIter->SeekDoc(2));
    //seek count 6
    ASSERT_EQ(-1, spatialIter->SeekDoc(3));
    ASSERT_EQ(6, spatialIter->GetSeekDocCount());
    autil::mem_pool::Pool* pool = iter->GetSessionPool();
    IE_POOL_COMPATIBLE_DELETE_CLASS(pool, iter);
}

void SpatialPostingIteratorTest::TestTermMeta()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "location", SFP_INDEX);
    string docsStr = "1 1,5 5,3 3,1 1,10 10";
    provider.Build(docsStr, SFP_OFFLINE);

    SpatialIndexReaderPtr reader(new SpatialIndexReader);
    reader->Open(provider.GetIndexConfig(), provider.GetPartitionData());
    PostingIterator* iter = reader->Lookup(Term("rectangle(1 1,3 3)", "spatial"));
    TermMeta* tm = iter->GetTermMeta();
    ASSERT_TRUE(tm);
    ASSERT_EQ((df_t)3, tm->GetDocFreq());
    ASSERT_EQ((tf_t)3, tm->GetTotalTermFreq());
    ASSERT_EQ((termpayload_t)0, tm->GetPayload());
    autil::mem_pool::Pool* pool = iter->GetSessionPool();
    IE_POOL_COMPATIBLE_DELETE_CLASS(pool, iter);
}

void SpatialPostingIteratorTest::TestReset()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "location", SFP_INDEX);
    string docsStr = "1 1,5 5,3 3,1 1,10 10";
    provider.Build(docsStr, SFP_OFFLINE);

    SpatialIndexReaderPtr reader(new SpatialIndexReader);
    reader->Open(provider.GetIndexConfig(), provider.GetPartitionData());
    PostingIterator* iter = reader->Lookup(Term("rectangle(1 1,3 3)", "spatial"));
    ASSERT_EQ((docid_t)0, iter->SeekDoc(INVALID_DOCID));
    ASSERT_EQ((docid_t)2, iter->SeekDoc(INVALID_DOCID));
    iter->Reset();
    ASSERT_EQ((docid_t)0, iter->SeekDoc(INVALID_DOCID));
    ASSERT_EQ((docid_t)2, iter->SeekDoc(INVALID_DOCID));
    ASSERT_EQ((docid_t)3, iter->SeekDoc(INVALID_DOCID));
    ASSERT_EQ((docid_t)INVALID_DOCID, iter->SeekDoc(INVALID_DOCID));

    iter->Reset();
    ASSERT_EQ((docid_t)0, iter->SeekDoc(INVALID_DOCID));
    autil::mem_pool::Pool* pool = iter->GetSessionPool();
    IE_POOL_COMPATIBLE_DELETE_CLASS(pool, iter);
}

IE_NAMESPACE_END(index);

