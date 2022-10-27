#ifndef __INDEXLIB_DFS_VAR_NUM_ATTRIBUTE_OFFSET_REVERTER_H
#define __INDEXLIB_DFS_VAR_NUM_ATTRIBUTE_OFFSET_REVERTER_H

#include <tr1/memory>
#include <algorithm>
#include <vector>
#include <stdint.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/index/normal/attribute/accessor/attribute_offset_reader.h"

IE_NAMESPACE_BEGIN(index);

class DfsVarNumAttributeOffsetReverter
{
public:
    DfsVarNumAttributeOffsetReverter(
            AttributeOffsetReader* offsetReader,
            uint32_t docCount, uint64_t dataLen);
    ~DfsVarNumAttributeOffsetReverter();

public:
    bool GetEncodeAttrLenVec(std::vector<uint32_t> &vec);
    void GetNormalAttrLenVec(std::vector<uint32_t> &vec);

private:
    template<typename T>
    bool GetTypedLenVec(std::vector<uint32_t> &vec);

private:
    AttributeOffsetReader* mOffsetReader;
    uint32_t  mDocCount;
    uint64_t  mDataLen;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DfsVarNumAttributeOffsetReverter);

//////////////////////////////////////////////////////////////////////

template <typename T>
inline bool DfsVarNumAttributeOffsetReverter::GetTypedLenVec(std::vector<uint32_t> &vec)
{
    typedef std::vector<T>       OffsetVec;
    typedef util::HashMap<T, uint32_t> LengthMap;

    OffsetVec offsetVec;
    LengthMap lengthMap(HASHMAP_INIT_SIZE);
    uint32_t *pLen = NULL;
    uint32_t  uniqSize = 0;

    // uniq offset
    T offset;
    for(uint32_t pos = 0; pos < mDocCount; ++pos)
    {
        offset = (T)mOffsetReader->GetOffset(pos);
        pLen = lengthMap.Find(offset);
        if (pLen != NULL)
        {
            continue;
        }
        offsetVec.push_back(offset);
        lengthMap.Insert(offset, 0);
    }
    uniqSize = offsetVec.size();

    // sort
    sort(offsetVec.begin(), offsetVec.end());
    if (mDataLen < offsetVec[uniqSize - 1])
    {
        return false;
    }
    
    // uniq insert length
    for(uint32_t pos = 0; pos < uniqSize - 1; ++pos)
    {
        lengthMap.FindAndInsert(offsetVec[pos], (uint32_t)(offsetVec[pos + 1] - offsetVec[pos]));
    }
    lengthMap.FindAndInsert(offsetVec[uniqSize - 1], (uint32_t)(mDataLen - offsetVec[uniqSize - 1]));

    // check uniq element number
    if (unlikely(lengthMap.Size() != offsetVec.size()))
    {
        return false;
    }

    // output
    for(uint32_t pos = 0; pos < mDocCount; ++pos)
    {
        pLen = lengthMap.Find((T)mOffsetReader->GetOffset(pos));
        if (unlikely(pLen == NULL))
        {
            return false;
        }
        vec.push_back(*pLen);
    }
    return true;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DFS_VAR_NUM_ATTRIBUTE_OFFSET_REVERTER_H
