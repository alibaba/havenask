#include <ha3_sdk/testlib/index/FakePostingIterator.h>
#include <ha3_sdk/testlib/index/FakeInDocPositionIterator.h>
#include <ha3_sdk/testlib/index/FakeInDocPositionState.h>
#include <indexlib/index/normal/inverted_index/accessor/term_match_data.h>
#include <assert.h>

IE_NAMESPACE_BEGIN(index);
IE_NAMESPACE_USE(common);

HA3_LOG_SETUP(index, FakePostingIterator);

docid_t FakePostingIterator::SeekDoc(docid_t id) {
    while (++_pos < (int32_t)_richPostings.second.size()) {
        HA3_LOG(TRACE3, "id: %d, _pos: %d]", id, _pos);
        if (_richPostings.second[_pos].docid >= id) {
            _occPos = -1;
            return _richPostings.second[_pos].docid;
        }
    }
    return INVALID_DOCID;
}

pos_t FakePostingIterator::SeekPosition(pos_t occ) {
    assert(_occPos <= (int32_t)_richPostings.second[_pos].occArray.size());
    while (++_occPos < (int32_t)_richPostings.second[_pos].occArray.size()) {
        HA3_LOG(TRACE3, "occ: %d, _occPos: %d]", occ, _occPos);
        if (_richPostings.second[_pos].occArray[_occPos] >= occ) {
            return _richPostings.second[_pos].occArray[_occPos];
        }
    }
    return INVALID_POSITION;
}

void FakePostingIterator::Unpack(TermMatchData &tmd) {
    FakeInDocPositionState *state = _objectPool.Alloc();
    state->init(_richPostings.second[_pos]);
    uint32_t tf = _richPostings.second[_pos].occArray.size();
    state->SetTermFreq((tf_t)tf);
    tmd.SetInDocPositionState(state);

    if (_richPostings.second[_pos].occArray.size() > 0) {
        tmd.SetFirstOcc(_richPostings.second[_pos].occArray[0]);
    }
    uint32_t fieldBitMap = 0;
    uint32_t fieldBitArraySize = _richPostings.second[_pos].fieldBitArray.size();
    for (uint32_t i = 0;i < tf && i < fieldBitArraySize ;i++) {
        fieldBitMap |= (1 << _richPostings.second[_pos].fieldBitArray[i]);
    }
    tmd.SetFieldMap(fieldBitMap);
    tmd.SetDocPayload((docpayload_t)_richPostings.second[_pos].docPayload);
}

matchvalue_t FakePostingIterator::GetMatchValue() const{
    matchvalue_t mRes;
    mRes.SetUint16(_richPostings.second[_pos].docPayload);
    return mRes;
}

IE_NAMESPACE_END(index);
