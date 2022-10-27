#include "indexlib/index/normal/inverted_index/customized_index/test/match_info_posting_iterator_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MatchInfoPostingIteratorTest);

MatchInfoPostingIteratorTest::MatchInfoPostingIteratorTest()
{
}

MatchInfoPostingIteratorTest::~MatchInfoPostingIteratorTest()
{
}

void MatchInfoPostingIteratorTest::CaseSetUp()
{
}

void MatchInfoPostingIteratorTest::CaseTearDown()
{
}

vector<SegmentMatchInfo> MatchInfoPostingIteratorTest::MakeSegMatchInfos(
        const vector<MatchInfoTuple> &matchInfos, autil::mem_pool::Pool *pool)
{
    vector<SegmentMatchInfo> segMatchInfos;
    for (const MatchInfoTuple& item : matchInfos)
    {
        const auto& docIds = item.getDocIds();
        const auto& matchValues = item.getMatchValues();

        MatchInfo matchInfo;
        matchInfo.pool = pool;
        matchInfo.matchCount = docIds.size();
        matchInfo.docIds = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, docid_t, matchInfo.matchCount);
        matchInfo.matchValues = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, matchvalue_t, matchInfo.matchCount);

        std::copy(docIds.begin(), docIds.end(), matchInfo.docIds);
        std::copy(matchValues.begin(), matchValues.end(), matchInfo.matchValues);
        SegmentMatchInfo segMatchInfo;
        segMatchInfo.baseDocId = item.getBaseDocId();
        segMatchInfo.matchInfo.reset(new MatchInfo(std::move(matchInfo)));
        segMatchInfos.push_back(segMatchInfo);
    }
    return segMatchInfos;
}

void MatchInfoPostingIteratorTest::TestSeekOneSegment()
{
    vector<SegmentMatchInfo> segMatchInfos = MakeSegMatchInfos(
            {
                MatchInfoTuple(0, {0, 1, 4, 99}, {0, 1, 2, 3}),
            },
            &mPool);
    PostingIteratorPtr iter(new MatchInfoPostingIterator(segMatchInfos, &mPool));
    ASSERT_EQ(0, iter->SeekDoc(-1));

    ASSERT_EQ(1, iter->SeekDoc(0));

    ASSERT_EQ(4, iter->SeekDoc(1));

    ASSERT_EQ(99, iter->SeekDoc(4));

    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(99));

    // test reset
    iter->Reset();
    ASSERT_EQ(0, iter->SeekDoc(-1)); 
    ASSERT_EQ(1, iter->SeekDoc(0));
    ASSERT_EQ(4, iter->SeekDoc(1));
    ASSERT_EQ(99, iter->SeekDoc(4));
    ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(99));

    // test clone
    PostingIterator* iterNew(iter->Clone());
    ASSERT_EQ(0, iterNew->SeekDoc(-1)); 
    ASSERT_EQ(1, iterNew->SeekDoc(0));
    ASSERT_EQ(4, iterNew->SeekDoc(1));
    ASSERT_EQ(99, iterNew->SeekDoc(4));
    ASSERT_EQ(INVALID_DOCID, iterNew->SeekDoc(99));
    POOL_COMPATIBLE_DELETE_CLASS(iter->GetSessionPool(), iterNew);
}

void MatchInfoPostingIteratorTest::TestSeekMultiSegments()
{
    vector<SegmentMatchInfo> segMatchInfos = MakeSegMatchInfos(
            {
                MatchInfoTuple(0, {0, 1, 4, 99}, {0, 1, 2, 3}),
                MatchInfoTuple(100, {0, 1, 4, 99}, {4, 5, 6, 7}), 
                MatchInfoTuple(200, {0, 1, 4, 99}, {8, 9, 10, 100}), 
            },
            &mPool);
    PostingIteratorPtr iter(new MatchInfoPostingIterator(segMatchInfos, &mPool));
    InnerTestSeekDoc(iter, INVALID_DOCID,
                     {0, 1, 4, 99, 100, 101, 104, 199, 200, 201, 204, 299},
                     {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100}); 

    // test boundary conditions
    iter->Reset();
    EXPECT_EQ(99, iter->SeekDoc(99));
    EXPECT_EQ(100, iter->SeekDoc(100));
    EXPECT_EQ(199, iter->SeekDoc(199));
    EXPECT_EQ(200, iter->SeekDoc(199));
    EXPECT_EQ(299, iter->SeekDoc(298));

    // test out of boundary
    iter->Reset();
    EXPECT_EQ(0, iter->SeekDoc(INVALID_DOCID));
    EXPECT_EQ(299, iter->SeekDoc(299));
    EXPECT_EQ(INVALID_DOCID, iter->SeekDoc(300));    
}

void MatchInfoPostingIteratorTest::InnerTestSeekDoc(
        const PostingIteratorPtr& iter,
        docid_t startDocId,
        const vector<docid_t>& expectDocIds,
        const vector<int32_t>& expectValues)
{
    ASSERT_EQ(expectDocIds.size(), expectValues.size());
    docid_t curDocId = startDocId;
    for (size_t i = 0; i < expectDocIds.size(); ++i)
    {
        matchvalue_t value;
        value.SetUint32(expectValues[i]);
        docid_t actualDocId = iter->SeekDoc(curDocId);
        EXPECT_EQ(expectDocIds[i], actualDocId);
        matchvalue_t actualMatchValue = iter->GetMatchValue();
        EXPECT_EQ(value.GetInt32(), actualMatchValue.GetInt32());
        curDocId = actualDocId;
    }
    EXPECT_EQ(INVALID_DOCID, iter->SeekDoc(curDocId));
}

IE_NAMESPACE_END(index);

