#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/single_bitmap_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/in_mem_bitmap_index_decoder.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_loader.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SingleBitmapPostingIterator);

SingleBitmapPostingIterator::SingleBitmapPostingIterator(optionflag_t flag) 
    : mData(NULL)
    , mCurrentLocalId(INVALID_DOCID)
    , mBaseDocId(0)
    , mLastDocId(INVALID_DOCID)
    , mStatePool(NULL)
{
}

SingleBitmapPostingIterator::~SingleBitmapPostingIterator() 
{
}

void SingleBitmapPostingIterator::Init(const util::ByteSliceListPtr& sliceListPtr,
                                       ObjectPool<InDocPositionStateType>* statePool)
{
    SetStatePool(statePool);

    ByteSliceReader reader(sliceListPtr.get());
    uint32_t pos = reader.Tell();
    TermMetaLoader tmLoader;
    tmLoader.Load(&reader, mTermMeta);

    uint32_t bmSize = reader.ReadUInt32();

    ByteSlice* slice = sliceListPtr->GetHead();
    uint8_t* dataCursor = slice->data + (reader.Tell() - pos);
    mBitmap.MountWithoutRefreshSetCount(bmSize * Bitmap::BYTE_SLOT_NUM,
            (uint32_t*)(dataCursor));
    mCurrentLocalId = INVALID_DOCID;
    mLastDocId = mBaseDocId + mBitmap.GetItemCount();
    mData = mBitmap.GetData();
}

void SingleBitmapPostingIterator::Init(
        const PostingWriter* postingWriter,
        ObjectPool<InDocPositionStateType>* statePool)
{
    const BitmapPostingWriter* bitmapPostingWriter = 
        dynamic_cast<const BitmapPostingWriter*>(postingWriter);

    assert(bitmapPostingWriter);
    InMemBitmapIndexDecoder bitmapDecoder;
    bitmapDecoder.Init(bitmapPostingWriter);

    SetStatePool(statePool);
    mTermMeta = *(bitmapDecoder.GetTermMeta());
    mBitmap.MountWithoutRefreshSetCount(bitmapDecoder.GetBitmapItemCount(), 
            bitmapDecoder.GetBitmapData());
    mCurrentLocalId = INVALID_DOCID;
    mLastDocId = mBaseDocId + mBitmap.GetItemCount();
    mData = mBitmap.GetData();
}

void SingleBitmapPostingIterator::Init(const Bitmap& bitmap, 
                                       TermMeta *termMeta,
                                       ObjectPool<InDocPositionStateType>* statePool)
{
    mTermMeta = *termMeta;
    SetStatePool(statePool);
    mBitmap = bitmap;
    mCurrentLocalId = INVALID_DOCID;
    mLastDocId = mBaseDocId + mBitmap.GetItemCount();
    mData = mBitmap.GetData();
}

IE_NAMESPACE_END(index);

