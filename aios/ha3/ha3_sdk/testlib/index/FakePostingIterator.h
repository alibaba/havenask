#ifndef IINDEX_INDEX_FAKEPOSTINGITERATOR_H_
#define IINDEX_INDEX_FAKEPOSTINGITERATOR_H_
#include <ha3/index/index.h>
#include <ha3/util/Log.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <ha3_sdk/testlib/index/FakeInDocPositionState.h>
#include <indexlib/util/object_pool.h>
#include <indexlib/common/error_code.h>
#include <indexlib/common_define.h>
#include <indexlib/index/normal/inverted_index/accessor/posting_iterator.h>
#include <indexlib/index/normal/inverted_index/accessor/term_meta.h>


IE_NAMESPACE_BEGIN(index);

class FakePostingIterator : virtual public index::PostingIterator
{
public: 
    FakePostingIterator(const FakeTextIndexReader::RichPostings &p,
                        uint32_t statePoolSize,
                        autil::mem_pool::Pool *sessionPool = NULL)
        : _richPostings(p)
        , _sessionPool(sessionPool)
        , _seekRet(IE_NAMESPACE(common)::ErrorCode::OK)
    {
        _pos = -1;
        _statePoolSize = statePoolSize;
        _objectPool.Init(_statePoolSize);
        HA3_LOG(TRACE3, "statePoolSize:%u", _statePoolSize);
        _type = pi_unknown;
        _hasPostion = true;

        tf_t totalTermFreq = 0;
        for (size_t postingIndex = 0; postingIndex < _richPostings.second.size(); postingIndex++) {
            totalTermFreq += _richPostings.second[postingIndex].occArray.size();
        }
        _truncateTermMeta = POOL_COMPATIBLE_NEW_CLASS(_sessionPool, IE_NAMESPACE(index)::TermMeta,
                _richPostings.second.size(), totalTermFreq, _richPostings.first);
        _termMeta = POOL_COMPATIBLE_NEW_CLASS(_sessionPool, IE_NAMESPACE(index)::TermMeta,
                _richPostings.second.size(), totalTermFreq, _richPostings.first);
    }
    
    virtual ~FakePostingIterator() {
        POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _termMeta);
        POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _truncateTermMeta);
        _termMeta = NULL;
        _truncateTermMeta = NULL;

    }

    virtual IE_NAMESPACE(index)::TermMeta* GetTermMeta() const override {
        return _termMeta;
    }
    virtual IE_NAMESPACE(index)::TermMeta* GetTruncateTermMeta() const override {
        return _truncateTermMeta;
    }
    void setTermMeta(IE_NAMESPACE(index)::TermMeta *termMeta) {
        *_termMeta = *termMeta;
    }

    docid_t SeekDoc(docid_t id) override;
    IE_NAMESPACE(common)::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override
    {
        if (_seekRet != IE_NAMESPACE(common)::ErrorCode::OK) {
            return _seekRet;
        }
        result = SeekDoc(docId);
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }
    virtual void Unpack(index::TermMatchData &tmd) override;
    virtual matchvalue_t GetMatchValue() const override;
    pos_t SeekPosition(pos_t occ);
    IE_NAMESPACE(common)::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t &nextPos) override
    {
        nextPos = SeekPosition(pos);
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }
    bool HasPosition() const override {
        return _hasPostion;
    }
    void setPosition(bool pos) {
        _hasPostion = pos;
    }
    PostingIteratorType GetType() const override { return _type; }
    void setType(PostingIteratorType type) {
        _type = type;
    }
    void setSeekRet(IE_NAMESPACE(common)::ErrorCode seekRet) {
        _seekRet = seekRet;
    }

    /* override */ PostingIterator* Clone() const override {
        FakePostingIterator * iter = POOL_COMPATIBLE_NEW_CLASS(_sessionPool, FakePostingIterator,
                                _richPostings, _statePoolSize, _sessionPool);
        iter->setType(_type);
        iter->setPosition(_pos);
        iter->setSeekRet(_seekRet);
        return iter;
    }
    /* override */
    autil::mem_pool::Pool *GetSessionPool() const override { return _sessionPool; }

    /* override*/ void Reset() override {_pos = -1;}
protected:
    FakeTextIndexReader::RichPostings _richPostings;
    IE_NAMESPACE(util)::ObjectPool<FakeInDocPositionState> _objectPool;
    uint32_t _statePoolSize;
    PostingIteratorType _type;
    int32_t _pos;
    int32_t _occPos;
    bool _hasPostion;
    IE_NAMESPACE(index)::TermMeta *_termMeta;
    IE_NAMESPACE(index)::TermMeta *_truncateTermMeta;
    autil::mem_pool::Pool *_sessionPool;
    IE_NAMESPACE(common)::ErrorCode _seekRet;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakePostingIterator);

IE_NAMESPACE_END(index);
#endif
