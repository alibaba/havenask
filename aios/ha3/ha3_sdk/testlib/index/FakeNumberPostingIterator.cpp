#include "ha3_sdk/testlib/index/FakeNumberPostingIterator.h"

#include <assert.h>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "ha3_sdk/testlib/index/FakeNumberIndexReader.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"

namespace indexlib {
namespace index {
class TermMatchData;
} // namespace index
} // namespace indexlib

namespace indexlib {
namespace index {

AUTIL_LOG_SETUP(ha3, FakeNumberPostingIterator);

indexlib::index::TermMeta *FakeNumberPostingIterator::GetTermMeta() const {
    return _termMeta;
}

indexlib::docid64_t FakeNumberPostingIterator::SeekDoc(indexlib::docid64_t id) {
    AUTIL_LOG(TRACE3, "look for %ld, size: %zu", id, _numberValuePtr->size());
    for (size_t pos = (size_t)id; pos < _numberValuePtr->size(); pos++) {
        AUTIL_LOG(TRACE3, "docid: %zu, value: %d", pos, _numberValuePtr->at(pos));
        if (valueInRange(_numberValuePtr->at(pos))) {
            return (docid_t)pos;
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

} // namespace index
} // namespace indexlib
