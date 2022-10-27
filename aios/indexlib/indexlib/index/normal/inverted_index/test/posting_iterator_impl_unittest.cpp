
#include "indexlib/index/normal/inverted_index/test/posting_iterator_impl_unittest.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingIteratorImplTest);

class PostingIteratorImplMock : public PostingIteratorImpl
{
public:
    PostingIteratorImplMock(Pool *sessionPool)
        : PostingIteratorImpl(sessionPool)
    {}

    bool Init(const SegmentPostingVectorPtr& segPostings, 
              TermMeta* termMeta,
              TermMeta* currentChainTermMeta)
    {
        mSegmentPostings = segPostings;
        mTermMeta = *termMeta;
        mCurrentChainTermMeta = *currentChainTermMeta;
        return true;
    }

    docid_t SeekDoc(docid_t docId) override { return INVALID_DOCID; }
    common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override {
        result = INVALID_DOCID;
        return common::ErrorCode::OK;
    }
    void Unpack(TermMatchData& termMatchData) override {}
    void Reset() override {}
    bool HasPosition() const override{ assert(false); return false; }
    PostingIterator* Clone() const override { assert(false); return NULL; }
};

PostingIteratorImplTest::PostingIteratorImplTest()
{
}

PostingIteratorImplTest::~PostingIteratorImplTest()
{
}

void PostingIteratorImplTest::CaseSetUp()
{
}

void PostingIteratorImplTest::CaseTearDown()
{
}

void PostingIteratorImplTest::TestCopy()
{
    PostingFormatOption option;
    std::tr1::shared_ptr<autil::mem_pool::Pool> memPool(new autil::mem_pool::Pool());

    SegmentPostingVectorPtr segPostings(new SegmentPostingVector);
    SegmentPosting segPosting(option);
    segPostings->push_back(segPosting);

    TermMeta *termMeta = IE_POOL_COMPATIBLE_NEW_CLASS(memPool.get(), TermMeta,
            2, 2, true);
    TermMeta *currentChainTermMeta = IE_POOL_COMPATIBLE_NEW_CLASS(memPool.get(), TermMeta,
            1, 1, true);

    PostingIteratorImplMock it(memPool.get());
    it.Init(segPostings, termMeta, currentChainTermMeta);
    
    // PostingIteratorImplMock clonedIt(it);
    // EXPECT_TRUE(it == clonedIt);
}

IE_NAMESPACE_END(index);

