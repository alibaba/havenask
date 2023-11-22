#include "indexlib/testlib/fake_posting_iterator.h"

#include "indexlib/testlib/fake_in_doc_position_state.h"

using namespace std;
using namespace indexlib::index;

namespace indexlib { namespace testlib {
IE_LOG_SETUP(testlib, FakePostingIterator);

FakePostingIterator::FakePostingIterator(const RichPostings& p, uint32_t statePoolSize,
                                         autil::mem_pool::Pool* sessionPool)
    : _richPostings(p)
    , _sessionPool(sessionPool)
{
    _pos = -1;
    _statePoolSize = statePoolSize;
    _objectPool.Init(_statePoolSize);
    IE_LOG(TRACE3, "statePoolSize:%u", _statePoolSize);
    _type = pi_unknown;
    _hasPostion = true;

    tf_t totalTermFreq = 0;
    for (size_t postingIndex = 0; postingIndex < _richPostings.second.size(); postingIndex++) {
        totalTermFreq += _richPostings.second[postingIndex].occArray.size();
    }
    _truncateTermMeta = POOL_COMPATIBLE_NEW_CLASS(_sessionPool, TermMeta, _richPostings.second.size(), totalTermFreq,
                                                  _richPostings.first);
    _termMeta = POOL_COMPATIBLE_NEW_CLASS(_sessionPool, TermMeta, _richPostings.second.size(), totalTermFreq,
                                          _richPostings.first);
}

FakePostingIterator::~FakePostingIterator()
{
    POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _termMeta);
    POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _truncateTermMeta);
    _termMeta = NULL;
    _truncateTermMeta = NULL;
}

docid64_t FakePostingIterator::SeekDoc(docid64_t id)
{
    while (++_pos < (int32_t)_richPostings.second.size()) {
        IE_LOG(TRACE3, "id: %ld, _pos: %d]", id, _pos);
        if (_richPostings.second[_pos].docid >= id) {
            _occPos = -1;
            return _richPostings.second[_pos].docid;
        }
    }
    return INVALID_DOCID;
}

index::ErrorCode FakePostingIterator::SeekPositionWithErrorCode(pos_t occ, pos_t& nextPos)
{
    assert(_occPos <= (int32_t)_richPostings.second[_pos].occArray.size());
    while (++_occPos < (int32_t)_richPostings.second[_pos].occArray.size()) {
        IE_LOG(TRACE3, "occ: %d, _occPos: %d]", occ, _occPos);
        if (_richPostings.second[_pos].occArray[_occPos] >= occ) {
            nextPos = _richPostings.second[_pos].occArray[_occPos];
            return index::ErrorCode::OK;
        }
    }
    nextPos = INVALID_POSITION;
    return index::ErrorCode::OK;
}

void FakePostingIterator::Unpack(TermMatchData& tmd)
{
    FakeInDocPositionState* state = _objectPool.Alloc();
    state->init(_richPostings.second[_pos]);
    uint32_t tf = _richPostings.second[_pos].occArray.size();
    state->SetTermFreq((tf_t)tf);
    tmd.SetInDocPositionState(state);

    if (_richPostings.second[_pos].occArray.size() > 0) {
        tmd.SetFirstOcc(_richPostings.second[_pos].occArray[0]);
    }
    uint32_t fieldBitMap = 0;
    uint32_t fieldBitArraySize = _richPostings.second[_pos].fieldBitArray.size();
    for (uint32_t i = 0; i < tf && i < fieldBitArraySize; i++) {
        fieldBitMap |= (1 << _richPostings.second[_pos].fieldBitArray[i]);
    }
    tmd.SetFieldMap(fieldBitMap);
    tmd.SetDocPayload((docpayload_t)_richPostings.second[_pos].docPayload);
}
}} // namespace indexlib::testlib
