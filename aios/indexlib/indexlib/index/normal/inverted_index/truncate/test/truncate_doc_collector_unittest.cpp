#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_doc_collector.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);

class FakeDocFilterProcessor : public DocFilterProcessor
{
public:
    FakeDocFilterProcessor() {};
    ~FakeDocFilterProcessor() {};

public:
    bool BeginFilter(dictkey_t key, const PostingIteratorPtr& postingIt) override
    {
        return true;
    }
    bool IsFiltered(docid_t docId) override
    {
        return false;
    }
    void SetTruncateMetaReader(const TruncateMetaReaderPtr &metaReader) override
    {
    }
};
DEFINE_SHARED_PTR(FakeDocFilterProcessor);

class FakePostingTruncatorForCollector : public PostingTruncator
{
public:
    FakePostingTruncatorForCollector()
        : PostingTruncator(0) {};
    ~FakePostingTruncatorForCollector() {};

public:
    void AddDoc(docid_t docId) override
    {
        mDocIds.push_back(docId);
    }

    void Truncate() override
    {
        assert(mReserveDocCount < mDocIds.size());
        mDocIds.erase(mDocIds.begin() + mReserveDocCount, mDocIds.end());
    }

    void SetDocCountToReserve(uint32_t reserveDocCount)
    {
        mReserveDocCount = reserveDocCount;
    }

private:
    uint32_t mReserveDocCount;
    
};
DEFINE_SHARED_PTR(FakePostingTruncatorForCollector);

class TruncateDocCollectorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TruncateDocCollectorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForSimple()
    {
        FakeDocFilterProcessorPtr docFilterProcessor(new FakeDocFilterProcessor());
        FakePostingTruncatorForCollectorPtr postingTruncator(
                new FakePostingTruncatorForCollector());
        TruncateDocCollectorPtr docCollector(
                new TruncateDocCollector(postingTruncator, docFilterProcessor));

        // collect
        postingTruncator->SetDocCountToReserve(3);
        docCollector->BeginCollect((dictkey_t)0, PostingIteratorPtr());
        for (size_t i = 0; i < 10; ++i)
        {
            docCollector->CollectDocId((docid_t)i);
        }
        docCollector->EndCollect();
        
        // check
        vector<docid_t> truncatedDocIds;
        while (!docCollector->Empty())
        {
            truncatedDocIds.push_back(docCollector->Pop());
        }
        INDEXLIB_TEST_EQUAL((size_t)3, truncatedDocIds.size());
        for (size_t i = 0; i < truncatedDocIds.size(); ++i)
        {
            INDEXLIB_TEST_EQUAL((docid_t)i, truncatedDocIds[i]);
        }
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, TruncateDocCollectorTest);

INDEXLIB_UNIT_TEST_CASE(TruncateDocCollectorTest, TestCaseForSimple);

IE_NAMESPACE_END(index);
