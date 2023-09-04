#include "ha3_sdk/testlib/index/FakeBufferedPostingIterator.h"

#include "autil/Log.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"

namespace indexlib {
namespace index {
AUTIL_LOG_SETUP(ha3, FakeBufferedPostingIterator);

FakeBufferedPostingIterator::FakeBufferedPostingIterator(
    const indexlib::index::PostingFormatOption &postingFormatOption,
    autil::mem_pool::Pool *sessionPool)
    : BufferedPostingIterator(postingFormatOption, sessionPool, nullptr) {}

} // namespace index
} // namespace indexlib
