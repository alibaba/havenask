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
#ifndef __INDEXLIB_SUFFIX_KEY_WRITER_H
#define __INDEXLIB_SUFFIX_KEY_WRITER_H

#include <cmath>
#include <limits>
#include <memory>
#include <unordered_map>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/MMapVector.h"

namespace indexlib { namespace partition {
class KKVSegmentWriterTest;
}} // namespace indexlib::partition

namespace indexlib { namespace index {

template <typename SKeyType>
class SuffixKeyWriter
{
public:
#pragma pack(push, 4)
    struct SKeyNode {
        uint32_t next;
        uint32_t timestamp;
        uint64_t offset;
        SKeyType skey;
        uint32_t expireTime;
        SKeyNode(SKeyType _skey, uint64_t _offset, uint32_t _ts, uint32_t _expireTime, uint32_t _next)
            : next(_next)
            , timestamp(_ts)
            , offset(_offset)
            , skey(_skey)
            , expireTime(_expireTime)
        {
        }
    };
#pragma pack(pop)

    // ListNode.skey may read & write at same time when update header node
    struct ListNode {
        uint32_t next;
        uint32_t skeyOffset;
        uint32_t count;
        SKeyType skey;
        ListNode(SKeyType _skey, uint32_t _count, uint32_t _skeyOffset, uint32_t _next = INVALID_SKEY_OFFSET)
            : next(_next)
            , skeyOffset(_skeyOffset)
            , count(_count)
            , skey(_skey)
        {
        }
    };

public:
    SuffixKeyWriter()
        : mReplaceNodeCount(0)
        , mLongTailThresHold(0)
        , mMinBlockCapacity(0)
        , mMaxLinkStep(std::numeric_limits<uint32_t>::max())
        , mMaxSkeyCount(0)
    {
    }

    ~SuffixKeyWriter() {}

    void Init(int64_t reserveSize, size_t longTailThreshold = 100,
              uint32_t maxLinkStep = std::numeric_limits<uint32_t>::max())
    {
        mLongTailThresHold = longTailThreshold;
        mMinBlockCapacity = (size_t)sqrt(mLongTailThresHold);

        size_t reserveItemCapacity = reserveSize / sizeof(SKeyNode) + 1;
        size_t reserveSkipListCapacity = reserveItemCapacity / mMinBlockCapacity + 1;

        mSKeyNodes.Init(util::MMapAllocatorPtr(new util::MMapAllocator), reserveItemCapacity);
        mListNodes.Init(util::MMapAllocatorPtr(new util::MMapAllocator), reserveSkipListCapacity);
        assert(sizeof(SKeyNode) % sizeof(uint32_t) == 0);
        mMaxLinkStep = maxLinkStep;
    }

    bool LinkSkeyNode(SKeyListInfo& listInfo, uint32_t skeyOffset);

    size_t GetTotalSkeyCount() const
    {
        assert(mSKeyNodes.Size() >= mReplaceNodeCount);
        return mSKeyNodes.Size() - mReplaceNodeCount;
    }

    size_t GetUsedBytes() const { return mSKeyNodes.GetUsedBytes() + mListNodes.GetUsedBytes(); }

    uint32_t GetMaxSkeyCount() const { return mMaxSkeyCount; }

    bool IsFull() const { return mSKeyNodes.IsFull() || mListNodes.IsFull(); }
    size_t GetLongTailThreshold() const { return mLongTailThresHold; }

public:
    uint32_t Append(const SKeyNode& skeyNode)
    {
        uint32_t ret = mSKeyNodes.Size();
        mSKeyNodes.PushBack(skeyNode);
        return ret;
    }

    const SKeyNode& GetSKeyNode(uint32_t skeyOffset) const { return mSKeyNodes[skeyOffset]; }

    // Seek Skey from start node pos (by start list node)
    // if ret : true, startNodePos.skey = skey;
    // if ret : false, startNodePos.skey = first skey bigger than target skey
    bool SeekTargetSKey(SKeyType skey, SKeyNode& skeyNode, uint32_t& startListNodePos, uint32_t& startNodePos) const;

public:
    // for test
    const util::MMapVector<SKeyNode>& GetSKeyNodes() const { return mSKeyNodes; }

private:
    uint32_t AppendList(const ListNode& listNode)
    {
        uint32_t ret = mListNodes.Size();
        mListNodes.PushBack(listNode);
        return ret;
    }

    // preNodePos : node position before link target skey node
    // listNodePos : refer to the last list node which less than skey
    uint32_t FindLinkPositionForSKey(const SKeyListInfo& listInfo, SKeyType skey, uint32_t& preNodePos,
                                     uint32_t& listNodePos);

    void AddNewSKeyNode(SKeyListInfo& listInfo, uint32_t preNodePos, uint32_t listNodePos, uint32_t skeyOffset);

    void ReplaceSKeyNode(SKeyListInfo& listInfo, uint32_t preNodePos, uint32_t targetPos, uint32_t listNodePos,
                         uint32_t skeyOffset);

    void CreateList(SKeyListInfo& listInfo);
    void SplitListNode(uint32_t cur);

private:
    util::MMapVector<SKeyNode> mSKeyNodes;
    util::MMapVector<ListNode> mListNodes;
    size_t mReplaceNodeCount;
    size_t mLongTailThresHold;
    size_t mMinBlockCapacity;
    uint32_t mMaxLinkStep;
    uint32_t mMaxSkeyCount;

private:
    friend class KKVSegmentWriterTest;
    friend class SuffixKeyWriterTest;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SuffixKeyWriter);

////////////////////////////////////////////////////////////////
template <typename SKeyType>
inline bool SuffixKeyWriter<SKeyType>::LinkSkeyNode(SKeyListInfo& listInfo, uint32_t skeyOffset)
{
    if (mSKeyNodes.IsFull()) {
        IE_LOG(INFO, "skey node vector is full, size is [%lu]", mSKeyNodes.Size());
        return false;
    }

    if (mListNodes.IsFull()) {
        IE_LOG(INFO, "list node vector is full, size is [%lu]", mListNodes.Size());
        return false;
    }

    if (mSKeyNodes[skeyOffset].offset == SKEY_ALL_DELETED_OFFSET) {
        listInfo = SKeyListInfo(skeyOffset, INVALID_SKEY_OFFSET, 1);
        return true;
    }

    SKeyType skey = mSKeyNodes[skeyOffset].skey;

    uint32_t preNodePos = INVALID_SKEY_OFFSET;
    uint32_t listNodePos = INVALID_SKEY_OFFSET;
    uint32_t targetPos = FindLinkPositionForSKey(listInfo, skey, preNodePos, listNodePos);

    if (targetPos == INVALID_SKEY_OFFSET) {
        if (listInfo.count >= mMaxLinkStep) {
            IE_LOG(INFO, "link skey [%s] over max link step [%u]", autil::StringUtil::toString(skey).c_str(),
                   mMaxLinkStep);
            return true;
        }
        AddNewSKeyNode(listInfo, preNodePos, listNodePos, skeyOffset);
    } else {
        ReplaceSKeyNode(listInfo, preNodePos, targetPos, listNodePos, skeyOffset);
    }
    return true;
}

template <typename SKeyType>
inline uint32_t SuffixKeyWriter<SKeyType>::FindLinkPositionForSKey(const SKeyListInfo& listInfo, SKeyType skey,
                                                                   uint32_t& preNodePos, uint32_t& listNodePos)
{
    preNodePos = INVALID_SKEY_OFFSET;
    listNodePos = INVALID_SKEY_OFFSET;

    // locate skey begin header by skip list block
    uint32_t header = listInfo.skeyHeader;
    uint32_t curListPos = listInfo.blockHeader;
    while (curListPos != INVALID_SKEY_OFFSET) {
        if (mListNodes[curListPos].skey >= skey) {
            break;
        }
        header = mListNodes[curListPos].skeyOffset;
        listNodePos = curListPos;
        curListPos = mListNodes[curListPos].next;
    }

    // locate skey node
    uint32_t current = header;
    while (current != INVALID_SKEY_OFFSET) {
        if (mSKeyNodes[current].skey > skey) {
            return INVALID_SKEY_OFFSET;
        }
        if (mSKeyNodes[current].skey == skey && mSKeyNodes[current].offset != SKEY_ALL_DELETED_OFFSET) {
            return current;
        }
        // delete all may ==
        assert(mSKeyNodes[current].skey <= skey);
        preNodePos = current;
        current = mSKeyNodes[current].next;
    }
    return INVALID_SKEY_OFFSET;
}

template <typename SKeyType>
inline void SuffixKeyWriter<SKeyType>::AddNewSKeyNode(SKeyListInfo& listInfo, uint32_t preNodePos, uint32_t listNodePos,
                                                      uint32_t skeyOffset)
{
    // add skey node to skey list
    if (preNodePos == INVALID_SKEY_OFFSET) {
        mSKeyNodes[skeyOffset].next = listInfo.skeyHeader;
        MEMORY_BARRIER();
        listInfo.skeyHeader = skeyOffset;
    } else {
        mSKeyNodes[skeyOffset].next = mSKeyNodes[preNodePos].next;
        MEMORY_BARRIER();
        mSKeyNodes[preNodePos].next = skeyOffset;
    }
    listInfo.count++;
    mMaxSkeyCount = std::max(mMaxSkeyCount, listInfo.count);

    // locate listNode for skey node
    SKeyType skey = mSKeyNodes[skeyOffset].skey;
    uint32_t curListPos = (listNodePos != INVALID_SKEY_OFFSET) ? listNodePos : listInfo.blockHeader;
    if (curListPos != INVALID_SKEY_OFFSET) {
        if (skey < mListNodes[curListPos].skey) // add to header
        {
            mListNodes[curListPos].skeyOffset = skeyOffset;
            MEMORY_BARRIER();
            mListNodes[curListPos].skey = skey;
        }
        mListNodes[curListPos].count++;
        // listNodeCount > 2 * sqrt(listCount), will split
        if (mListNodes[curListPos].count * mListNodes[curListPos].count > 4 * listInfo.count) {
            SplitListNode(curListPos);
        }
        return;
    }

    // no list node, create if over threshold
    assert(listInfo.blockHeader == INVALID_SKEY_OFFSET);
    if (listInfo.count >= mLongTailThresHold) {
        CreateList(listInfo);
    }
}

template <typename SKeyType>
inline void SuffixKeyWriter<SKeyType>::ReplaceSKeyNode(SKeyListInfo& listInfo, uint32_t preNodePos, uint32_t targetPos,
                                                       uint32_t listNodePos, uint32_t skeyOffset)
{
    SKeyType skey = mSKeyNodes[skeyOffset].skey;
    assert(skey == mSKeyNodes[targetPos].skey);

    // link replaced skey node
    mSKeyNodes[skeyOffset].next = mSKeyNodes[targetPos].next;
    MEMORY_BARRIER();
    if (preNodePos == INVALID_SKEY_OFFSET) {
        listInfo.skeyHeader = skeyOffset;
    } else {
        mSKeyNodes[preNodePos].next = skeyOffset;
    }
    ++mReplaceNodeCount;

    // update skip list node which refer to replaced skey node
    uint32_t curListPos = (listNodePos != INVALID_SKEY_OFFSET) ? listNodePos : listInfo.blockHeader;
    while (curListPos != INVALID_SKEY_OFFSET) {
        if (mListNodes[curListPos].skey > skey) {
            break;
        }
        if (mListNodes[curListPos].skeyOffset == targetPos) {
            assert(mListNodes[curListPos].skey == skey);
            mListNodes[curListPos].skeyOffset = skeyOffset;
            break;
        }
        curListPos = mListNodes[curListPos].next;
    }
}

template <typename SKeyType>
inline void SuffixKeyWriter<SKeyType>::CreateList(SKeyListInfo& listInfo)
{
    uint32_t preListNode = INVALID_SKEY_OFFSET;
    uint32_t curListNode = INVALID_SKEY_OFFSET;
    uint32_t listHeader = INVALID_SKEY_OFFSET;

    SKeyType skey;
    uint32_t skeyOffset = INVALID_SKEY_OFFSET;

    uint32_t count = 0;
    uint32_t current = listInfo.skeyHeader;
    while (current != INVALID_SKEY_OFFSET) {
        if (count == 0) {
            skey = mSKeyNodes[current].skey;
            skeyOffset = current;
        }
        ++count;
        if (count == mMinBlockCapacity) {
            curListNode = AppendList(ListNode(skey, count, skeyOffset));
            if (preListNode == INVALID_SKEY_OFFSET) {
                listHeader = curListNode;
            } else {
                mListNodes[preListNode].next = curListNode;
            }
            preListNode = curListNode;
            count = 0;
        }
        current = mSKeyNodes[current].next;
    }

    if (count > 0) {
        curListNode = AppendList(ListNode(skey, count, skeyOffset));
        if (preListNode == INVALID_SKEY_OFFSET) {
            listHeader = curListNode;
        } else {
            mListNodes[preListNode].next = curListNode;
        }
    }
    listInfo.blockHeader = listHeader;
}

template <typename SKeyType>
inline void SuffixKeyWriter<SKeyType>::SplitListNode(uint32_t cur)
{
    uint32_t cnt = mListNodes[cur].count;
    uint32_t count1, count2;
    count1 = cnt / 2;
    count2 = cnt - count1;
    cnt = count1;
    uint32_t prev = mListNodes[cur].skeyOffset, current = prev;
    while (cnt > 0) {
        current = mSKeyNodes[current].next;
        --cnt;
    }
    uint32_t newListNode = AppendList(ListNode(mSKeyNodes[current].skey, count2, current, mListNodes[cur].next));
    mListNodes[cur].count = count1;
    mListNodes[cur].next = newListNode;
}

template <typename SKeyType>
inline bool SuffixKeyWriter<SKeyType>::SeekTargetSKey(SKeyType skey, SKeyNode& skeyNode, uint32_t& startListNodePos,
                                                      uint32_t& startNodePos) const
{
    uint32_t curListPos = startListNodePos;
    while (curListPos != INVALID_SKEY_OFFSET) {
        if (mListNodes[curListPos].skey > skey) {
            break;
        }
        uint32_t header = mListNodes[curListPos].skeyOffset;
        startListNodePos = curListPos;
        curListPos = mListNodes[curListPos].next;
        if (startNodePos == INVALID_SKEY_OFFSET || mSKeyNodes[startNodePos].skey < mSKeyNodes[header].skey) {
            startNodePos = header;
        }
    }

    // locate skey node
    while (startNodePos != INVALID_SKEY_OFFSET) {
        skeyNode = mSKeyNodes[startNodePos];
        if (skeyNode.skey > skey) {
            return false;
        }
        if (skeyNode.skey == skey && skeyNode.offset != SKEY_ALL_DELETED_OFFSET) {
            return true;
        }
        // delete all may ==
        assert(skeyNode.skey <= skey);
        startNodePos = skeyNode.next;
    }
    return false;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_SUFFIX_KEY_WRITER_H
