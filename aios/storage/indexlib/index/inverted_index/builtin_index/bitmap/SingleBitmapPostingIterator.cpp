/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index/inverted_index/builtin_index/bitmap/SingleBitmapPostingIterator.h"

#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingExpandData.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/InMemBitmapIndexDecoder.h"
#include "indexlib/index/inverted_index/format/TermMetaLoader.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {
namespace {
using util::Bitmap;
using util::ObjectPool;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, SingleBitmapPostingIterator);

SingleBitmapPostingIterator::SingleBitmapPostingIterator(optionflag_t flag)
    : _currentLocalId(INVALID_DOCID)
    , _baseDocId(0)
    , _lastDocId(INVALID_DOCID)
    , _statePool(nullptr)
{
}

SingleBitmapPostingIterator::~SingleBitmapPostingIterator() {}

void SingleBitmapPostingIterator::Init(BitmapPostingExpandData* expandData,
                                       ObjectPool<InDocPositionStateType>* statePool)
{
    _currentLocalId = INVALID_DOCID;
    _lastDocId = _baseDocId;
    InitExpandBitmap(expandData);
}

void SingleBitmapPostingIterator::Init(const util::ByteSliceListPtr& sliceListPtr, BitmapPostingExpandData* expandData,
                                       ObjectPool<InDocPositionStateType>* statePool)
{
    SetStatePool(statePool);

    file_system::ByteSliceReader reader;
    reader.Open(sliceListPtr.get()).GetOrThrow();
    uint32_t pos = reader.Tell();
    TermMetaLoader tmLoader;
    tmLoader.Load(&reader, _termMeta);

    uint32_t bmSize = reader.ReadUInt32().GetOrThrow();

    util::ByteSlice* slice = sliceListPtr->GetHead();
    uint8_t* dataCursor = slice->data + (reader.Tell() - pos);
    _bitmap.MountWithoutRefreshSetCount(bmSize * Bitmap::BYTE_SLOT_NUM, (uint32_t*)(dataCursor));
    _currentLocalId = INVALID_DOCID;
    _lastDocId = _baseDocId + _bitmap.GetItemCount();
    InitExpandBitmap(expandData);
}

void SingleBitmapPostingIterator::Init(util::ByteSlice* singleSlice, BitmapPostingExpandData* expandData,
                                       ObjectPool<InDocPositionStateType>* statePool)
{
    SetStatePool(statePool);

    file_system::ByteSliceReader reader;
    reader.Open(singleSlice).GetOrThrow();
    uint32_t pos = reader.Tell();
    TermMetaLoader tmLoader;
    tmLoader.Load(&reader, _termMeta);

    uint32_t bmSize = reader.ReadUInt32().GetOrThrow();

    util::ByteSlice* slice = singleSlice;
    uint8_t* dataCursor = slice->data + (reader.Tell() - pos);
    _bitmap.MountWithoutRefreshSetCount(bmSize * Bitmap::BYTE_SLOT_NUM, (uint32_t*)(dataCursor));
    _currentLocalId = INVALID_DOCID;
    _lastDocId = _baseDocId + _bitmap.GetItemCount();
    InitExpandBitmap(expandData);
}

void SingleBitmapPostingIterator::Init(const PostingWriter* postingWriter,
                                       ObjectPool<InDocPositionStateType>* statePool)
{
    SetStatePool(statePool);

    const BitmapPostingWriter* bitmapPostingWriter = dynamic_cast<const BitmapPostingWriter*>(postingWriter);
    assert(bitmapPostingWriter);
    InMemBitmapIndexDecoder bitmapDecoder;
    bitmapDecoder.Init(bitmapPostingWriter);

    _termMeta = *(bitmapDecoder.GetTermMeta());
    _bitmap.MountWithoutRefreshSetCount(bitmapDecoder.GetBitmapItemCount(), bitmapDecoder.GetBitmapData());
    _currentLocalId = INVALID_DOCID;
    _lastDocId = _baseDocId + _bitmap.GetItemCount();
}

void SingleBitmapPostingIterator::Init(const Bitmap& bitmap, TermMeta* termMeta,
                                       ObjectPool<InDocPositionStateType>* statePool)
{
    _termMeta = *termMeta;
    SetStatePool(statePool);
    _bitmap = bitmap;
    _currentLocalId = INVALID_DOCID;
    _lastDocId = _baseDocId + _bitmap.GetItemCount();
}

void SingleBitmapPostingIterator::InitExpandBitmap(BitmapPostingExpandData* expandData)
{
    if (!expandData) {
        return;
    }
    InMemBitmapIndexDecoder bitmapDecoder;
    bitmapDecoder.Init(expandData->postingWriter);
    _expandBitmap.MountWithoutRefreshSetCount(bitmapDecoder.GetBitmapItemCount(), bitmapDecoder.GetBitmapData());
    _lastDocId += bitmapDecoder.GetBitmapItemCount();
    _termMeta = expandData->termMeta;
}
} // namespace indexlib::index
