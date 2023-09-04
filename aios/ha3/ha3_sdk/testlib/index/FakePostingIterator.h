#pragma once
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3_sdk/testlib/index/FakeInDocPositionState.h"
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/util/ObjectPool.h"

namespace indexlib {
namespace index {
class TermMatchData;
} // namespace index
} // namespace indexlib

namespace indexlib {
namespace index {

class FakePostingIterator : virtual public indexlib::index::PostingIterator {
public:
    FakePostingIterator(const FakeTextIndexReader::RichPostings &p,
                        uint32_t statePoolSize,
                        autil::mem_pool::Pool *sessionPool = NULL)
        : _richPostings(p)
        , _sessionPool(sessionPool)
        , _seekRet(indexlib::index::ErrorCode::OK) {
        _pos = -1;
        _statePoolSize = statePoolSize;
        _objectPool.Init(_statePoolSize);
        AUTIL_LOG(TRACE3, "statePoolSize:%u", _statePoolSize);
        _type = pi_unknown;
        _hasPostion = true;

        tf_t totalTermFreq = 0;
        for (size_t postingIndex = 0; postingIndex < _richPostings.second.size(); postingIndex++) {
            totalTermFreq += _richPostings.second[postingIndex].occArray.size();
        }
        _truncateTermMeta = POOL_COMPATIBLE_NEW_CLASS(_sessionPool,
                                                      indexlib::index::TermMeta,
                                                      _richPostings.second.size(),
                                                      totalTermFreq,
                                                      _richPostings.first);
        _termMeta = POOL_COMPATIBLE_NEW_CLASS(_sessionPool,
                                              indexlib::index::TermMeta,
                                              _richPostings.second.size(),
                                              totalTermFreq,
                                              _richPostings.first);
    }

    virtual ~FakePostingIterator() {
        POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _termMeta);
        POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _truncateTermMeta);
        _termMeta = NULL;
        _truncateTermMeta = NULL;
    }

    virtual indexlib::index::TermMeta *GetTermMeta() const override {
        return _termMeta;
    }
    virtual indexlib::index::TermMeta *GetTruncateTermMeta() const override {
        return _truncateTermMeta;
    }
    void setTermMeta(indexlib::index::TermMeta *termMeta) {
        *_termMeta = *termMeta;
    }

    docid_t SeekDoc(docid_t id) override;
    indexlib::index::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t &result) override {
        if (_seekRet != indexlib::index::ErrorCode::OK) {
            return _seekRet;
        }
        result = SeekDoc(docId);
        return indexlib::index::ErrorCode::OK;
    }
    virtual void Unpack(indexlib::index::TermMatchData &tmd) override;
    virtual matchvalue_t GetMatchValue() const override;
    pos_t SeekPosition(pos_t occ);
    indexlib::index::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t &nextPos) override {
        nextPos = SeekPosition(pos);
        return indexlib::index::ErrorCode::OK;
    }
    bool HasPosition() const override {
        return _hasPostion;
    }
    void setPosition(bool pos) {
        _hasPostion = pos;
    }
    PostingIteratorType GetType() const override {
        return _type;
    }
    void setType(PostingIteratorType type) {
        _type = type;
    }
    void setSeekRet(indexlib::index::ErrorCode seekRet) {
        _seekRet = seekRet;
    }

    /* override */ PostingIterator *Clone() const override {
        FakePostingIterator *iter = POOL_COMPATIBLE_NEW_CLASS(
            _sessionPool, FakePostingIterator, _richPostings, _statePoolSize, _sessionPool);
        iter->setType(_type);
        iter->setPosition(_pos);
        iter->setSeekRet(_seekRet);
        return iter;
    }
    /* override */
    autil::mem_pool::Pool *GetSessionPool() const override {
        return _sessionPool;
    }

    /* override*/ void Reset() override {
        _pos = -1;
    }

protected:
    FakeTextIndexReader::RichPostings _richPostings;
    indexlib::util::ObjectPool<FakeInDocPositionState> _objectPool;
    uint32_t _statePoolSize;
    PostingIteratorType _type;
    int32_t _pos;
    int32_t _occPos;
    bool _hasPostion;
    indexlib::index::TermMeta *_termMeta;
    indexlib::index::TermMeta *_truncateTermMeta;
    autil::mem_pool::Pool *_sessionPool;
    indexlib::index::ErrorCode _seekRet;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FakePostingIterator> FakePostingIteratorPtr;

} // namespace index
} // namespace indexlib
