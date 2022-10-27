#include <autil/StringTokenizer.h>
#include <ha3_sdk/testlib/index/FakeBufferedIndexDecoder.h>
#include <autil/StringUtil.h>

using namespace autil;

IE_NAMESPACE_BEGIN(index);
HA3_LOG_SETUP(index, FakeBufferedIndexDecoder);

FakeBufferedIndexDecoder::FakeBufferedIndexDecoder(
        const PostingFormatOption& formatOption, autil::mem_pool::Pool *pool)
    : BufferedIndexDecoder(NULL, pool)
    , _blockCursor(0)
{ 
    mCurSegPostingFormatOption = formatOption;
}

FakeBufferedIndexDecoder::~FakeBufferedIndexDecoder() { 
}

void FakeBufferedIndexDecoder::Init(const std::string &docIdStr, const std::string &fieldStr) {
    StringUtil::fromString<docid_t>(docIdStr, _docIds, ",");
    StringUtil::fromString<fieldmap_t>(fieldStr, _fieldMaps, ",");
}

bool FakeBufferedIndexDecoder::DecodeDocBuffer(docid_t startDocId, docid_t *docBuffer,
        docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
{
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
    memcpy(fieldMapBuffer, &_fieldMaps[(_blockCursor - 1) * 128],
           sizeof(fieldmap_t) * nextReadCount);
}

uint32_t FakeBufferedIndexDecoder::GetSeekedDocCount() const {
    return _blockCursor * 128;
}

IE_NAMESPACE_END(index);

