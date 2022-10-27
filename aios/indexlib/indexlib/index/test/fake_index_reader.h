#ifndef __INDEXLIB_FAKE_INDEX_READER_H
#define __INDEXLIB_FAKE_INDEX_READER_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include <vector>
#include <map>
#include "indexlib/index/test/fake_index_reader_base.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

IE_NAMESPACE_BEGIN(index);

// to distinguish with FakePostingIterator and FakeIndexReader in testlib
namespace {

class FakePostingIterator : public PostingIterator
{
public:
    FakePostingIterator(const std::vector<docid_t>& docIdVect)
        : mDocIdVect(docIdVect)
        , mCursor(-1)
        , mTermMeta(NULL)
    {
        mTermMeta = new index::TermMeta;
        mTermMeta->SetDocFreq(mDocIdVect.size());
    }
    ~FakePostingIterator()
    {
        DELETE_AND_SET_NULL(mTermMeta);
    }


    index::TermMeta *GetTermMeta() const override { 
        return mTermMeta;
    }

    PostingIterator* Clone() const override
    {
        return new FakePostingIterator(mDocIdVect);
    }

    docid_t SeekDoc(docid_t docId) override
    {
        if (mCursor >= (int32_t)mDocIdVect.size() - 1)
        {
            return INVALID_DOCID;
        }
        mCursor++;
        return mDocIdVect[mCursor];
    }
    common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override
    {
        result = SeekDoc(docId);
        return common::ErrorCode::OK;
    }
    void Unpack(TermMatchData& termMatchData) override {}

    bool HasPosition() const override { return false;}
    void Reset() override { mCursor = 0;}

private:
    std::vector<docid_t> mDocIdVect;
    int32_t mCursor;
    index::TermMeta *mTermMeta;
};

class FakeIndexReader : public FakeIndexReaderBase
{
public:
    FakeIndexReader(const std::vector<docid_t>& docIdVect)
        : mDocIdVect(docIdVect)
    { }

    FakeIndexReader()
    { }

    void SetIndexConfig(const config::IndexConfigPtr& indexConfig)
    { mIndexConfig = indexConfig; }

    PostingIterator* Lookup(const common::Term& term,
                              uint32_t statePoolSize = 1000,
                              PostingType type = pt_default,
                              autil::mem_pool::Pool *sessionPool = NULL) override
    { return new FakePostingIterator(mDocIdVect); }

    std::string GetIdentifier() const  { return "fake";}

    std::string GetIndexName() const
    { return mIndexConfig->GetIndexName(); }

    void SetDocCount(uint32_t docCount)
    { mDocCount = docCount; }
    
protected:
    typedef std::vector<std::pair<std::string, std::string> > TermVector;

    config::IndexConfigPtr mIndexConfig;
    std::vector<docid_t> mDocIdVect;

    uint32_t mDocCount;
};

typedef std::tr1::shared_ptr<FakeIndexReader> FakeIndexReaderPtr;

}
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FAKE_INDEX_READER_H
