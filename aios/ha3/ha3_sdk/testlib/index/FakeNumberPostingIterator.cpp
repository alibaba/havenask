#include <ha3_sdk/testlib/index/FakeNumberPostingIterator.h>
#include <assert.h>

IE_NAMESPACE_USE(common);
IE_NAMESPACE_BEGIN(index);

HA3_LOG_SETUP(index, FakeNumberPostingIterator);

TermMeta* FakeNumberPostingIterator::GetTermMeta() const {
    return _termMeta;
}

docid_t FakeNumberPostingIterator::SeekDoc(docid_t id) {
    HA3_LOG(TRACE3, "look for %d, size: %zu", id, _numberValuePtr->size());
    for (size_t pos = (size_t)id; pos < _numberValuePtr->size(); pos++) {
        HA3_LOG(TRACE3, "docid: %zu, value: %d", pos, _numberValuePtr->at(pos));
        if (valueInRange(_numberValuePtr->at(pos))) {
            return (docid_t) pos;
        }
    }
    return INVALID_DOCID;
}

// TermMatchDataPtr FakeNumberPostingIterator::Unpack() {
//     assert(false);
//     TermMatchDataPtr ptr;
//     return ptr;
// }

void FakeNumberPostingIterator::Unpack(TermMatchData &tmd) {
    assert(false);
}

bool FakeNumberPostingIterator::valueInRange(int32_t value) {
    return _leftNumber <= value && value <= _rightNumber;
}

IE_NAMESPACE_END(index);
