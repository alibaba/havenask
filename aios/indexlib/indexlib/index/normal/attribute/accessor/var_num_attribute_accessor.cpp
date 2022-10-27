#include "indexlib/index/normal/attribute/accessor/var_num_attribute_accessor.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, VarNumAttributeAccessor);

void VarNumAttributeAccessor::Init(Pool *pool)
{
    char* buffer = (char*)pool->allocate(sizeof(RadixTree));
    mData = new (buffer) RadixTree(SLOT_NUM, SLICE_LEN, pool);
    buffer = (char*)pool->allocate(sizeof(TypedSliceList<uint64_t>));
    mOffsets = new (buffer) TypedSliceList<uint64_t>(
            SLOT_NUM, OFFSET_SLICE_LEN, pool);

}

void VarNumAttributeAccessor::AddEncodedField(
        const AttrValueMeta& meta, EncodeMapPtr& encodeMap)
{
    uint64_t hashValue = meta.hashKey;
    uint64_t *offset = encodeMap->Find(hashValue);
    if (offset != NULL)
    {
        mOffsets->PushBack(*offset);
    }
    else
    {
        uint64_t currentOffset = AppendData(meta.data);
        mOffsets->PushBack(currentOffset);
        encodeMap->Insert(hashValue, currentOffset);
    }
}

bool VarNumAttributeAccessor::UpdateEncodedField(
        const docid_t docid, const AttrValueMeta& meta, EncodeMapPtr& encodeMap)
{
    if (mOffsets->Size() <= (uint64_t)docid)
    {
        IE_LOG(ERROR, "update doc not exist, docid is %d", docid);
        return false;
    }

    uint64_t hashValue = meta.hashKey;
    uint64_t *offset = encodeMap->Find(hashValue);
    if (offset != NULL)
    {
        (*mOffsets)[docid] = *offset;
    }
    else
    {
        uint64_t currentOffset = AppendData(meta.data);
        (*mOffsets)[docid] = currentOffset;
        encodeMap->Insert(hashValue, currentOffset);
    }
    return true;
}

bool VarNumAttributeAccessor::UpdateNormalField(
        const docid_t docid, const AttrValueMeta& meta)
{
    if (mOffsets->Size() <= (uint64_t)docid)
    {
        IE_LOG(ERROR, "update doc not exist, docid is %d", docid);
        return false;
    }
    uint64_t currentOffset = AppendData(meta.data);
    (*mOffsets)[docid] = currentOffset;
    return true;
}

void VarNumAttributeAccessor::AddNormalField(const AttrValueMeta& meta)
{
    Append(meta.data);
}

void VarNumAttributeAccessor::Append(const autil::ConstString& data)
{
    uint64_t currentOffset = AppendData(data);
    mOffsets->PushBack(currentOffset);
}

uint64_t VarNumAttributeAccessor::AppendData(
        const ConstString& data)
{
    uint32_t dataSize = data.size();
    uint32_t appendSize = dataSize + sizeof(uint32_t);
    uint8_t* buffer = mData->Allocate(appendSize);
    if (buffer == NULL)
    {
        IE_LOG(ERROR, "data size [%u] too large to allocate"
               "cut to %lu", dataSize, SLICE_LEN - sizeof(uint32_t));
        appendSize = sizeof(uint32_t);
        dataSize = 0;
        buffer = mData->Allocate(appendSize);
    }

    memcpy(buffer, (void*)&dataSize, sizeof(uint32_t));
    memcpy(buffer + sizeof(uint32_t), data.data(), dataSize);

    mAppendDataSize += dataSize;
    return mData->GetCurrentOffset() - appendSize;
}

VarNumAttributeDataIteratorPtr VarNumAttributeAccessor::CreateVarNumAttributeDataIterator() const
{
    return VarNumAttributeDataIteratorPtr(
            new VarNumAttributeDataIterator(mData, mOffsets));
}

void VarNumAttributeAccessor::ReadData(
            const docid_t docid, uint8_t*& data, uint32_t& dataLength) const
{
    if ((uint64_t)docid >= mOffsets->Size())
    {
        IE_LOG(ERROR, "Read data fail, Invalid docid %d", docid);
        data = NULL;
        dataLength = 0;
        return;
    }

    uint64_t offset = (*mOffsets)[docid];
    uint8_t* buffer = mData->Search(offset);

    dataLength = *(uint32_t*)buffer;
    data = buffer + sizeof(uint32_t);
}

IE_NAMESPACE_END(index);
