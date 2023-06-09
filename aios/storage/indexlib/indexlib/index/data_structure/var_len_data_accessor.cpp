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
#include "indexlib/index/data_structure/var_len_data_accessor.h"

#include "indexlib/util/KeyHasherTyped.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VarLenDataAccessor);

VarLenDataAccessor::VarLenDataAccessor() : mData(NULL), mOffsets(NULL), mAppendDataSize(0) {}

VarLenDataAccessor::~VarLenDataAccessor() {}

void VarLenDataAccessor::Init(Pool* pool, bool uniqEncode)
{
    if (uniqEncode) {
        InitForUniqEncode(pool);
        return;
    }

    char* buffer = (char*)pool->allocate(sizeof(RadixTree));
    mData = new (buffer) RadixTree(SLOT_NUM, SLICE_LEN, pool);
    buffer = (char*)pool->allocate(sizeof(TypedSliceList<uint64_t>));
    mOffsets = new (buffer) TypedSliceList<uint64_t>(SLOT_NUM, OFFSET_SLICE_LEN, pool);
}

void VarLenDataAccessor::InitForUniqEncode(Pool* pool, uint32_t hashMapInitSize)
{
    char* buffer = (char*)pool->allocate(sizeof(RadixTree));
    mData = new (buffer) RadixTree(SLOT_NUM, SLICE_LEN, pool);
    buffer = (char*)pool->allocate(sizeof(TypedSliceList<uint64_t>));
    mOffsets = new (buffer) TypedSliceList<uint64_t>(SLOT_NUM, OFFSET_SLICE_LEN, pool);
    mEncodeMap.reset(new EncodeMap(pool, hashMapInitSize));
}

void VarLenDataAccessor::AppendValue(const StringView& value)
{
    uint64_t hashValue = GetHashValue(value);
    AppendValue(value, hashValue);
}

bool VarLenDataAccessor::UpdateValue(docid_t docId, const StringView& value)
{
    uint64_t hashValue = GetHashValue(value);
    return UpdateValue(docId, value, hashValue);
}

void VarLenDataAccessor::AppendValue(const StringView& value, uint64_t hash)
{
    if (mEncodeMap) {
        uint64_t* offset = mEncodeMap->Find(hash);
        if (offset != NULL) {
            mOffsets->PushBack(*offset);
            return;
        }
    }

    uint64_t currentOffset = AppendData(value);
    mOffsets->PushBack(currentOffset);
    if (mEncodeMap) {
        mEncodeMap->Insert(hash, currentOffset);
    }
}

bool VarLenDataAccessor::UpdateValue(docid_t docid, const StringView& value, uint64_t hash)
{
    if (mOffsets->Size() <= (uint64_t)docid) {
        IE_LOG(ERROR, "update doc not exist, docid is %d", docid);
        return false;
    }

    if (mEncodeMap) {
        uint64_t* offset = mEncodeMap->Find(hash);
        if (offset != NULL) {
            (*mOffsets)[docid] = *offset;
            return true;
        }
    }

    uint64_t currentOffset = AppendData(value);
    (*mOffsets)[docid] = currentOffset;
    if (mEncodeMap) {
        mEncodeMap->Insert(hash, currentOffset);
    }
    return true;
}

VarLenDataIteratorPtr VarLenDataAccessor::CreateDataIterator() const
{
    return VarLenDataIteratorPtr(new VarLenDataIterator(mData, mOffsets));
}

bool VarLenDataAccessor::ReadData(const docid_t docid, uint8_t*& data, uint32_t& dataLength) const
{
    if ((uint64_t)docid >= mOffsets->Size()) {
        IE_LOG(ERROR, "Read data fail, Invalid docid %d", docid);
        data = NULL;
        dataLength = 0;
        return false;
    }

    uint64_t offset = (*mOffsets)[docid];
    uint8_t* buffer = mData->Search(offset);
    dataLength = *(uint32_t*)buffer;
    data = buffer + sizeof(uint32_t);
    return true;
}

uint64_t VarLenDataAccessor::AppendData(const StringView& data)
{
    uint32_t dataSize = data.size();
    uint32_t appendSize = dataSize + sizeof(uint32_t);
    uint8_t* buffer = mData->Allocate(appendSize);
    if (buffer == NULL) {
        IE_LOG(ERROR,
               "data size [%u] too large to allocate"
               "cut to %lu",
               dataSize, SLICE_LEN - sizeof(uint32_t));
        appendSize = sizeof(uint32_t);
        dataSize = 0;
        buffer = mData->Allocate(appendSize);
    }

    memcpy(buffer, (void*)&dataSize, sizeof(uint32_t));
    memcpy(buffer + sizeof(uint32_t), data.data(), dataSize);
    mAppendDataSize += dataSize;
    return mData->GetCurrentOffset() - appendSize;
}

uint64_t VarLenDataAccessor::GetHashValue(const StringView& data)
{
    uint64_t hashValue = 0;
    if (mEncodeMap) {
        MurmurHasher::GetHashKey(data, hashValue);
    }
    return hashValue;
}
}} // namespace indexlib::index
