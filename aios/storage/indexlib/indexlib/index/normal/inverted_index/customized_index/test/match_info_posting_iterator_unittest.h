#pragma once

#include <tuple>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/match_info_posting_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(index, PostingIterator);
namespace indexlib { namespace index {

class MatchInfoTuple
{
public:
    MatchInfoTuple(docid_t baseDocId, const std::vector<docid_t>& docids, const std::vector<int32_t>& values)
        : mBaseDocId(baseDocId)
        , mDocIds(docids)
    {
        for_each(values.begin(), values.end(), [=](int i) {
            matchvalue_t value;
            value.SetInt32(i);
            mValues.push_back(value);
        });
    };
    ~MatchInfoTuple() {};

public:
    docid_t getBaseDocId() const { return mBaseDocId; }
    const std::vector<docid_t>& getDocIds() const { return mDocIds; }
    const std::vector<matchvalue_t>& getMatchValues() const { return mValues; }

private:
    docid_t mBaseDocId;
    std::vector<docid_t> mDocIds;
    std::vector<matchvalue_t> mValues;
};

class MatchInfoPostingIteratorTest : public INDEXLIB_TESTBASE
{
public:
    MatchInfoPostingIteratorTest();
    ~MatchInfoPostingIteratorTest();

    DECLARE_CLASS_NAME(MatchInfoPostingIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSeekOneSegment();
    void TestSeekMultiSegments();

private:
    void InnerTestSeekDoc(const PostingIteratorPtr& iter, docid_t startDocId, const std::vector<docid_t>& expectDocIds,
                          const std::vector<int32_t>& expectValues);

private:
    std::vector<SegmentMatchInfo> MakeSegMatchInfos(const std::vector<MatchInfoTuple>& matchInfos,
                                                    autil::mem_pool::Pool* pool);

private:
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MatchInfoPostingIteratorTest, TestSeekOneSegment);
INDEXLIB_UNIT_TEST_CASE(MatchInfoPostingIteratorTest, TestSeekMultiSegments);
}} // namespace indexlib::index
