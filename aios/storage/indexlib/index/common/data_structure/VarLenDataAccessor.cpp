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
#include "indexlib/index/common/data_structure/VarLenDataAccessor.h"

#include "indexlib/util/KeyHasherTyped.h"

using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, VarLenDataAccessor);

VarLenDataAccessor::VarLenDataAccessor() : _data(NULL), _offsets(NULL), _appendDataSize(0) {}

VarLenDataAccessor::~VarLenDataAccessor() {}

void VarLenDataAccessor::Init(Pool* pool, bool uniqEncode)
{
    if (uniqEncode) {
        InitForUniqEncode(pool, HASHMAP_INIT_SIZE);
        return;
    }

    char* buffer = (char*)pool->allocate(sizeof(indexlib::index::RadixTree));
    _data = new (buffer) indexlib::index::RadixTree(SLOT_NUM, SLICE_LEN, pool);
    buffer = (char*)pool->allocate(sizeof(indexlib::index::TypedSliceList<uint64_t>));
    _offsets = new (buffer) indexlib::index::TypedSliceList<uint64_t>(SLOT_NUM, OFFSET_SLICE_LEN, pool);
}

void VarLenDataAccessor::InitForUniqEncode(Pool* pool, uint32_t hashMapInitSize)
{
    char* buffer = (char*)pool->allocate(sizeof(indexlib::index::RadixTree));
    _data = new (buffer) indexlib::index::RadixTree(SLOT_NUM, SLICE_LEN, pool);
    buffer = (char*)pool->allocate(sizeof(indexlib::index::TypedSliceList<uint64_t>));
    _offsets = new (buffer) indexlib::index::TypedSliceList<uint64_t>(SLOT_NUM, OFFSET_SLICE_LEN, pool);
    _encodeMap.reset(new EncodeMap(pool, hashMapInitSize));
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
    if (_encodeMap) {
        uint64_t* offset = _encodeMap->Find(hash);
        if (offset != NULL) {
            _offsets->PushBack(*offset);
            return;
        }
    }

    uint64_t currentOffset = AppendData(value);
    _offsets->PushBack(currentOffset);
    if (_encodeMap) {
        _encodeMap->Insert(hash, currentOffset);
    }
}

bool VarLenDataAccessor::UpdateValue(docid_t docid, const StringView& value, uint64_t hash)
{
    if (_offsets->Size() <= (uint64_t)docid) {
        AUTIL_LOG(ERROR, "update doc not exist, docid is %d", docid);
        return false;
    }

    if (_encodeMap) {
        uint64_t* offset = _encodeMap->Find(hash);
        if (offset != NULL) {
            (*_offsets)[docid] = *offset;
            return true;
        }
    }

    uint64_t currentOffset = AppendData(value);
    (*_offsets)[docid] = currentOffset;
    if (_encodeMap) {
        _encodeMap->Insert(hash, currentOffset);
    }
    return true;
}

std::shared_ptr<VarLenDataIterator> VarLenDataAccessor::CreateDataIterator() const
{
    return std::make_shared<VarLenDataIterator>(_data, _offsets);
}

bool VarLenDataAccessor::ReadData(const docid_t docid, uint8_t*& data, uint32_t& dataLength) const
{
    if ((uint64_t)docid >= _offsets->Size()) {
        AUTIL_LOG(ERROR, "Read data fail, Invalid docid %d", docid);
        data = NULL;
        dataLength = 0;
        return false;
    }

    uint64_t offset = (*_offsets)[docid];
    uint8_t* buffer = _data->Search(offset);
    dataLength = *(uint32_t*)buffer;
    data = buffer + sizeof(uint32_t);
    return true;
}

uint64_t VarLenDataAccessor::AppendData(const StringView& data)
{
    uint32_t dataSize = data.size();
    uint32_t appendSize = dataSize + sizeof(uint32_t);
    uint8_t* buffer = _data->Allocate(appendSize);
    if (buffer == NULL) {
        AUTIL_LOG(ERROR,
                  "data size [%u] too large to allocate"
                  "cut to %lu",
                  dataSize, SLICE_LEN - sizeof(uint32_t));
        appendSize = sizeof(uint32_t);
        dataSize = 0;
        buffer = _data->Allocate(appendSize);
    }

    memcpy(buffer, (void*)&dataSize, sizeof(uint32_t));
    memcpy(buffer + sizeof(uint32_t), data.data(), dataSize);
    _appendDataSize += dataSize;
    return _data->GetCurrentOffset() - appendSize;
}

uint64_t VarLenDataAccessor::GetHashValue(const StringView& data)
{
    uint64_t hashValue = 0;
    if (_encodeMap) {
        MurmurHasher::GetHashKey(data, hashValue);
    }
    return hashValue;
}
} // namespace indexlibv2::index
