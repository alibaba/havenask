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
#include "indexlib/index/inverted_index/format/skiplist/BufferedSkipListWriter.h"

#include "indexlib/util/byte_slice_list/ByteSliceListIterator.h"

namespace indexlib::index {
namespace {
using util::ByteSliceList;
using util::ByteSliceListIterator;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, BufferedSkipListWriter);

BufferedSkipListWriter::BufferedSkipListWriter(autil::mem_pool::Pool* byteSlicePool,
                                               autil::mem_pool::RecyclePool* bufferPool, CompressMode compressMode)
    : BufferedByteSlice(byteSlicePool, bufferPool)
    , _lastKey(0)
    , _lastValue1(0)
{
    if (compressMode == REFERENCE_COMPRESS_MODE) {
        _lastKey = INVALID_LAST_KEY;
    }
}

void BufferedSkipListWriter::AddItem(uint32_t key, uint32_t value1, uint32_t value2)
{
    assert(GetMultiValue()->GetAtomicValueSize() == 3);

    PushBack(0, key - _lastKey);
    PushBack(1, value1 - _lastValue1);
    _lastKey = key;
    _lastValue1 = value1;
    PushBack(2, value2);
    EndPushBack();

    if (NeedFlush(SKIP_LIST_BUFFER_SIZE)) {
        Flush(PFOR_DELTA_COMPRESS_MODE);
    }
}

void BufferedSkipListWriter::AddItem(uint32_t key, uint32_t value1)
{
    assert(GetMultiValue()->GetAtomicValueSize() == 2);

    PushBack(0, IsReferenceCompress() ? key : key - _lastKey);
    PushBack(1, value1);
    EndPushBack();

    if (!IsReferenceCompress()) {
        _lastKey = key;
    }

    if (NeedFlush(SKIP_LIST_BUFFER_SIZE)) {
        Flush(IsReferenceCompress() ? REFERENCE_COMPRESS_MODE : PFOR_DELTA_COMPRESS_MODE);
    }
}

void BufferedSkipListWriter::AddItem(uint32_t valueDelta)
{
    _lastValue1 += valueDelta;
    PushBack(0, _lastValue1);
    EndPushBack();

    if (NeedFlush(SKIP_LIST_BUFFER_SIZE)) {
        Flush(SHORT_LIST_COMPRESS_MODE);
    }
}

size_t BufferedSkipListWriter::FinishFlush()
{
    uint32_t skipListSize = GetTotalCount();
    uint8_t skipListCompressMode = ShortListOptimizeUtil::GetSkipListCompressMode(skipListSize);

    if (IsReferenceCompress() && skipListCompressMode == PFOR_DELTA_COMPRESS_MODE) {
        skipListCompressMode = REFERENCE_COMPRESS_MODE;
    }
    return Flush(skipListCompressMode);
}

size_t BufferedSkipListWriter::DoFlush(uint8_t compressMode)
{
    assert(GetMultiValue()->GetAtomicValueSize() <= 3);
    assert(compressMode == PFOR_DELTA_COMPRESS_MODE || compressMode == SHORT_LIST_COMPRESS_MODE ||
           compressMode == REFERENCE_COMPRESS_MODE);

    uint32_t flushSize = 0;
    uint8_t size = _buffer.Size();

    const MultiValue* multiValue = GetMultiValue();
    const AtomicValueVector& atomicValues = multiValue->GetAtomicValues();
    for (size_t i = 0; i < atomicValues.size(); ++i) {
        AtomicValue* atomicValue = atomicValues[i];
        uint8_t* buffer = _buffer.GetRow(atomicValue->GetLocation());
        flushSize += atomicValue->Encode(compressMode, _postingListWriter, buffer, size * atomicValue->GetSize());
    }
    return flushSize;
}

void BufferedSkipListWriter::Dump(const std::shared_ptr<file_system::FileWriter>& file)
{
    uint32_t skipListSize = GetTotalCount();
    if (skipListSize == 0) {
        return;
    }
    uint8_t compressMode = ShortListOptimizeUtil::GetSkipListCompressMode(skipListSize);

    if (compressMode != SHORT_LIST_COMPRESS_MODE || GetMultiValue()->GetAtomicValueSize() != 3) {
        _postingListWriter.Dump(file).GetOrThrow();
        return;
    }

    const ByteSliceList* skipList = _postingListWriter.GetByteSliceList();
    ByteSliceListIterator iter(skipList);
    const MultiValue* multiValue = GetMultiValue();
    const AtomicValueVector& atomicValues = multiValue->GetAtomicValues();
    uint32_t start = 0;
    for (size_t i = 0; i < atomicValues.size(); ++i) {
        uint32_t len = atomicValues[i]->GetSize() * skipListSize;
        if (i > 0) {
            // not encode last value after first row
            len -= atomicValues[i]->GetSize();
        }
        if (len == 0) {
            break;
        }
        bool ret = iter.SeekSlice(start);
        assert(ret);
        (void)ret;
        while (iter.HasNext(start + len)) {
            void* data = NULL;
            size_t size = 0;
            iter.Next(data, size);
            assert(data);
            assert(size);
            file->Write(data, size).GetOrThrow();
        }
        start += atomicValues[i]->GetSize() * skipListSize;
    }
}

size_t BufferedSkipListWriter::EstimateDumpSize() const
{
    uint32_t skipListSize = GetTotalCount();
    if (skipListSize == 0) {
        return 0;
    }
    uint8_t compressMode = ShortListOptimizeUtil::GetSkipListCompressMode(skipListSize);

    if (compressMode != SHORT_LIST_COMPRESS_MODE || GetMultiValue()->GetAtomicValueSize() != 3) {
        return _postingListWriter.GetSize();
    }

    const MultiValue* multiValue = GetMultiValue();
    const AtomicValueVector& atomicValues = multiValue->GetAtomicValues();
    assert(atomicValues.size() == 3);

    size_t optSize = atomicValues[1]->GetSize() + atomicValues[2]->GetSize();
    return _postingListWriter.GetSize() - optSize;
}
} // namespace indexlib::index
