#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_decoder.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);

IE_LOG_SETUP(index, BitmapPostingDecoder);

BitmapPostingDecoder::BitmapPostingDecoder() 
    : mDocIdCursor(INVALID_DOCID)
    , mEndDocId(INVALID_DOCID)
{
}

BitmapPostingDecoder::~BitmapPostingDecoder() 
{
}

void BitmapPostingDecoder::Init(TermMeta *termMeta,
                                uint8_t* data, uint32_t size)
{
    mTermMeta = termMeta;
    mBitmap.reset(new Bitmap);
    mBitmap->Mount(size * Bitmap::BYTE_SLOT_NUM, (uint32_t*)data);
    mDocIdCursor = INVALID_DOCID;
    mEndDocId = size * Bitmap::BYTE_SLOT_NUM;
}

uint32_t BitmapPostingDecoder::DecodeDocList(docid_t* docBuffer, size_t len)
{
    uint32_t retDocCount = 0;
    while (mDocIdCursor < mEndDocId && retDocCount < len)
    {
        uint32_t nextId = mBitmap->Next(mDocIdCursor);
        if (nextId != Bitmap::INVALID_INDEX)
        {
            docBuffer[retDocCount] = (docid_t)nextId;
            retDocCount++;
            mDocIdCursor = nextId;
        }
        else
        {
            mDocIdCursor = mEndDocId;
        }
    }

    return retDocCount;
}

IE_NAMESPACE_END(index);

