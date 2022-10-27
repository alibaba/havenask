#ifndef IINDEX_INDEX_FAKENUMBERPOSTINGITERATOR_H_
#define IINDEX_INDEX_FAKENUMBERPOSTINGITERATOR_H_
#include <ha3/index/index.h>
#include <ha3/util/Log.h>
#include <ha3_sdk/testlib/index/FakeNumberIndexReader.h>
#include <indexlib/index/normal/inverted_index/accessor/term_meta.h>
#include <indexlib/index/normal/inverted_index/accessor/posting_iterator.h>

IE_NAMESPACE_BEGIN(index);

class FakeNumberPostingIterator : public PostingIterator
{
public: 
    FakeNumberPostingIterator(FakeNumberIndexReader::NumberValuesPtr numberValuePtr,
                              int32_t leftNumber, int32_t rightNumber,
                              autil::mem_pool::Pool *sessionPool = NULL) 
        : _numberValuePtr(numberValuePtr)
        , _leftNumber(leftNumber)
        , _rightNumber(rightNumber) 
        , _sessionPool(sessionPool)
        , _termMeta(NULL)
        {
            _termMeta = new index::TermMeta();
        }
    
    virtual ~FakeNumberPostingIterator() {
        DELETE_AND_SET_NULL(_termMeta);
    }

    virtual IE_NAMESPACE(index)::TermMeta* GetTermMeta() const override;

    docid_t SeekDoc(docid_t id) override;
    IE_NAMESPACE(common)::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override
    {
        result = SeekDoc(docId);
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }

    virtual void Unpack(TermMatchData& termMatchData) override;
    bool HasPosition() const override {
        return true;
    }
    autil::mem_pool::Pool *GetSessionPool() const override { return _sessionPool; }
    void Reset() override {}
    virtual PostingIterator* Clone() const override { assert(false); return NULL; }
private:
    bool valueInRange(int32_t value);
private:
    FakeNumberIndexReader::NumberValuesPtr _numberValuePtr;
    int32_t _leftNumber;
    int32_t _rightNumber;
    autil::mem_pool::Pool *_sessionPool;
    index::TermMeta* _termMeta;
private:
    HA3_LOG_DECLARE();
};

IE_NAMESPACE_END(index);
#endif
