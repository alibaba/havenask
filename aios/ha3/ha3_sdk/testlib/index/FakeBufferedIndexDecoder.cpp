#include "ha3_sdk/testlib/index/FakeBufferedIndexDecoder.h"

#include <algorithm>
#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/index/inverted_index/BufferedIndexDecoder.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/indexlib.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace autil;

namespace indexlib {
namespace index {
AUTIL_LOG_SETUP(ha3, FakeBufferedIndexDecoder);

FakeBufferedIndexDecoder::FakeBufferedIndexDecoder(
    const indexlib::index::PostingFormatOption &formatOption, autil::mem_pool::Pool *pool)
    : BufferedIndexDecoder(NULL, pool)
    , _blockCursor(0) {
    mCurSegPostingFormatOption = formatOption;
}

FakeBufferedIndexDecoder::~FakeBufferedIndexDecoder() {}

void FakeBufferedIndexDecoder::Init(const std::string &docIdStr, const std::string &fieldStr) {
    StringUtil::fromString<docid_t>(docIdStr, _docIds, ",");
    StringUtil::fromString<fieldmap_t>(fieldStr, _fieldMaps, ",");
}

bool FakeBufferedIndexDecoder::DecodeDocBuffer(docid64_t startDocId,
                                               docid32_t *docBuffer,
                                               docid64_t &firstDocId,
                                               docid64_t &lastDocId,
                                               ttf_t &currentTTF) {
    uint32_t totalCount = _docIds.size();
    uint32_t totalBlockCount = (totalCount - 1) / 128 + 1;
    if (_blockCursor >= totalBlockCount) {
        return false;
    }
    uint32_t nextReadCount = std::min((uint32_t)128, totalCount - _blockCursor * 128);
    memcpy(docBuffer, &_docIds[_blockCursor * 128], sizeof(docid_t) * nextReadCount);
    ++_blockCursor;
    firstDocId = docBuffer[0];
    lastDocId = docBuffer[nextReadCount - 1];
    for (uint32_t i = nextReadCount - 1; i > 0; --i) {
        docBuffer[i] -= docBuffer[i - 1];
    }
    return true;
}

bool FakeBufferedIndexDecoder::DecodeCurrentTFBuffer(tf_t *tfBuffer) {
    return false;
}

void FakeBufferedIndexDecoder::DecodeCurrentDocPayloadBuffer(docpayload_t *docPayloadBuffer) {
    return;
}

void FakeBufferedIndexDecoder::DecodeCurrentFieldMapBuffer(fieldmap_t *fieldMapBuffer) {
    assert(_blockCursor >= 1);
    uint32_t totalCount = _docIds.size();
    uint32_t nextReadCount = std::min((uint32_t)128, totalCount - (_blockCursor - 1) * 128);
    memcpy(
        fieldMapBuffer, &_fieldMaps[(_blockCursor - 1) * 128], sizeof(fieldmap_t) * nextReadCount);
}

uint32_t FakeBufferedIndexDecoder::GetSeekedDocCount() const {
    return _blockCursor * 128;
}

} // namespace index
} // namespace indexlib
