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
#pragma once

#include <cmath>

#include "autil/StringUtil.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/base/Define.h"
#include "indexlib/index/kkv/common/SKeyListInfo.h"
#include "indexlib/util/MMapVector.h"

namespace indexlibv2::index {

template <typename SKeyType>
class SKeyWriter
{
public:
#pragma pack(push, 4)
    struct SKeyNode {
        uint32_t next;
        uint32_t timestamp;
        uint64_t valueOffset;
        SKeyType skey;
        uint32_t expireTime;
        SKeyNode(SKeyType _skey, uint64_t _offset, uint32_t _ts, uint32_t _expireTime, uint32_t _next)
            : next(_next)
            , timestamp(_ts)
            , valueOffset(_offset)
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
    SKeyWriter()
        : _replaceNodeCount(0)
        , _longTailThresHold(0)
        , _minBlockCapacity(0)
        , _maxLinkStep(std::numeric_limits<uint32_t>::max())
        , _maxSkeyCount(0)
    {
    }

    ~SKeyWriter() {}

    void Init(int64_t reserveSize, size_t longTailThreshold = 100,
              uint32_t maxLinkStep = std::numeric_limits<uint32_t>::max())
    {
        _longTailThresHold = longTailThreshold;
        _minBlockCapacity = (size_t)sqrt(_longTailThresHold);

        size_t reserveItemCapacity = reserveSize / sizeof(SKeyNode) + 1;
        size_t reserveSkipListCapacity = reserveItemCapacity / _minBlockCapacity + 1;

        _skeyNodes.Init(indexlib::util::MMapAllocatorPtr(new indexlib::util::MMapAllocator), reserveItemCapacity);
        _listNodes.Init(indexlib::util::MMapAllocatorPtr(new indexlib::util::MMapAllocator), reserveSkipListCapacity);
        assert(sizeof(SKeyNode) % sizeof(uint32_t) == 0);
        _maxLinkStep = maxLinkStep;
    }

    bool LinkSkeyNode(SKeyListInfo& listInfo, uint32_t skeyOffset);

    size_t GetTotalSkeyCount() const
    {
        assert(_skeyNodes.Size() >= _replaceNodeCount);
        return _skeyNodes.Size() - _replaceNodeCount;
    }

    size_t GetUsedBytes() const { return _skeyNodes.GetUsedBytes() + _listNodes.GetUsedBytes(); }

    uint32_t GetMaxSkeyCount() const { return _maxSkeyCount; }

    bool IsFull() const { return _skeyNodes.IsFull() || _listNodes.IsFull(); }
    size_t GetLongTailThreshold() const { return _longTailThresHold; }

public:
    uint32_t Append(const SKeyNode& skeyNode)
    {
        uint32_t ret = _skeyNodes.Size();
        _skeyNodes.PushBack(skeyNode);
        return ret;
    }

    const SKeyNode& GetSKeyNode(uint32_t skeyOffset) const { return _skeyNodes[skeyOffset]; }

    // Seek Skey from start node pos (by start list node)
    // if ret : true, startNodePos.skey = skey;
    // if ret : false, startNodePos.skey = first skey bigger than target skey
    bool SeekTargetSKey(SKeyType skey, SKeyNode& skeyNode, uint32_t& startListNodePos, uint32_t& startNodePos) const;

public:
    // for test
    const indexlib::util::MMapVector<SKeyNode>& GetSKeyNodes() const { return _skeyNodes; }

private:
    uint32_t AppendList(const ListNode& listNode)
    {
        uint32_t ret = _listNodes.Size();
        _listNodes.PushBack(listNode);
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
    indexlib::util::MMapVector<SKeyNode> _skeyNodes;
    indexlib::util::MMapVector<ListNode> _listNodes;
    size_t _replaceNodeCount;
    size_t _longTailThresHold;
    size_t _minBlockCapacity;
    uint32_t _maxLinkStep;
    uint32_t _maxSkeyCount;

private:
    friend class SuffixKeyWriterTest;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SKeyWriter, SKeyType);

////////////////////////////////////////////////////////////////
template <typename SKeyType>
inline bool SKeyWriter<SKeyType>::LinkSkeyNode(SKeyListInfo& listInfo, uint32_t skeyOffset)
{
    if (_skeyNodes.IsFull()) {
        AUTIL_LOG(INFO, "skey node vector is full, size is [%lu]", _skeyNodes.Size());
        return false;
    }

    if (_listNodes.IsFull()) {
        AUTIL_LOG(INFO, "list node vector is full, size is [%lu]", _listNodes.Size());
        return false;
    }

    if (_skeyNodes[skeyOffset].valueOffset == SKEY_ALL_DELETED_OFFSET) {
        listInfo = SKeyListInfo(skeyOffset, INVALID_SKEY_OFFSET, 1);
        return true;
    }

    SKeyType skey = _skeyNodes[skeyOffset].skey;

    uint32_t preNodePos = INVALID_SKEY_OFFSET;
    uint32_t listNodePos = INVALID_SKEY_OFFSET;
    uint32_t targetPos = FindLinkPositionForSKey(listInfo, skey, preNodePos, listNodePos);

    if (targetPos == INVALID_SKEY_OFFSET) {
        if (listInfo.count >= _maxLinkStep) {
            AUTIL_LOG(INFO, "link skey [%s] over max link step [%u]", autil::StringUtil::toString(skey).c_str(),
                      _maxLinkStep);
            return true;
        }
        AddNewSKeyNode(listInfo, preNodePos, listNodePos, skeyOffset);
    } else {
        ReplaceSKeyNode(listInfo, preNodePos, targetPos, listNodePos, skeyOffset);
    }
    return true;
}

template <typename SKeyType>
inline uint32_t SKeyWriter<SKeyType>::FindLinkPositionForSKey(const SKeyListInfo& listInfo, SKeyType skey,
                                                              uint32_t& preNodePos, uint32_t& listNodePos)
{
    preNodePos = INVALID_SKEY_OFFSET;
    listNodePos = INVALID_SKEY_OFFSET;

    // locate skey begin header by skip list block
    uint32_t header = listInfo.skeyHeader;
    uint32_t curListPos = listInfo.blockHeader;
    while (curListPos != INVALID_SKEY_OFFSET) {
        if (_listNodes[curListPos].skey >= skey) {
            break;
        }
        header = _listNodes[curListPos].skeyOffset;
        listNodePos = curListPos;
        curListPos = _listNodes[curListPos].next;
    }

    // locate skey node
    uint32_t current = header;
    while (current != INVALID_SKEY_OFFSET) {
        if (_skeyNodes[current].skey > skey) {
            return INVALID_SKEY_OFFSET;
        }
        if (_skeyNodes[current].skey == skey && _skeyNodes[current].valueOffset != SKEY_ALL_DELETED_OFFSET) {
            return current;
        }
        // delete all may ==
        assert(_skeyNodes[current].skey <= skey);
        preNodePos = current;
        current = _skeyNodes[current].next;
    }
    return INVALID_SKEY_OFFSET;
}

template <typename SKeyType>
inline void SKeyWriter<SKeyType>::AddNewSKeyNode(SKeyListInfo& listInfo, uint32_t preNodePos, uint32_t listNodePos,
                                                 uint32_t skeyOffset)
{
    // add skey node to skey list
    if (preNodePos == INVALID_SKEY_OFFSET) {
        _skeyNodes[skeyOffset].next = listInfo.skeyHeader;
        MEMORY_BARRIER();
        listInfo.skeyHeader = skeyOffset;
    } else {
        _skeyNodes[skeyOffset].next = _skeyNodes[preNodePos].next;
        MEMORY_BARRIER();
        _skeyNodes[preNodePos].next = skeyOffset;
    }
    listInfo.count++;
    _maxSkeyCount = std::max(_maxSkeyCount, listInfo.count);

    // locate listNode for skey node
    SKeyType skey = _skeyNodes[skeyOffset].skey;
    uint32_t curListPos = (listNodePos != INVALID_SKEY_OFFSET) ? listNodePos : listInfo.blockHeader;
    if (curListPos != INVALID_SKEY_OFFSET) {
        if (skey < _listNodes[curListPos].skey) // add to header
        {
            _listNodes[curListPos].skeyOffset = skeyOffset;
            MEMORY_BARRIER();
            _listNodes[curListPos].skey = skey;
        }
        _listNodes[curListPos].count++;
        // listNodeCount > 2 * sqrt(listCount), will split
        if (_listNodes[curListPos].count * _listNodes[curListPos].count > 4 * listInfo.count) {
            SplitListNode(curListPos);
        }
        return;
    }

    // no list node, create if over threshold
    assert(listInfo.blockHeader == INVALID_SKEY_OFFSET);
    if (listInfo.count >= _longTailThresHold) {
        CreateList(listInfo);
    }
}

template <typename SKeyType>
inline void SKeyWriter<SKeyType>::ReplaceSKeyNode(SKeyListInfo& listInfo, uint32_t preNodePos, uint32_t targetPos,
                                                  uint32_t listNodePos, uint32_t skeyOffset)
{
    SKeyType skey = _skeyNodes[skeyOffset].skey;
    assert(skey == _skeyNodes[targetPos].skey);

    // link replaced skey node
    _skeyNodes[skeyOffset].next = _skeyNodes[targetPos].next;
    MEMORY_BARRIER();
    if (preNodePos == INVALID_SKEY_OFFSET) {
        listInfo.skeyHeader = skeyOffset;
    } else {
        _skeyNodes[preNodePos].next = skeyOffset;
    }
    ++_replaceNodeCount;

    // update skip list node which refer to replaced skey node
    uint32_t curListPos = (listNodePos != INVALID_SKEY_OFFSET) ? listNodePos : listInfo.blockHeader;
    while (curListPos != INVALID_SKEY_OFFSET) {
        if (_listNodes[curListPos].skey > skey) {
            break;
        }
        if (_listNodes[curListPos].skeyOffset == targetPos) {
            assert(_listNodes[curListPos].skey == skey);
            _listNodes[curListPos].skeyOffset = skeyOffset;
            break;
        }
        curListPos = _listNodes[curListPos].next;
    }
}

template <typename SKeyType>
inline void SKeyWriter<SKeyType>::CreateList(SKeyListInfo& listInfo)
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
            skey = _skeyNodes[current].skey;
            skeyOffset = current;
        }
        ++count;
        if (count == _minBlockCapacity) {
            curListNode = AppendList(ListNode(skey, count, skeyOffset));
            if (preListNode == INVALID_SKEY_OFFSET) {
                listHeader = curListNode;
            } else {
                _listNodes[preListNode].next = curListNode;
            }
            preListNode = curListNode;
            count = 0;
        }
        current = _skeyNodes[current].next;
    }

    if (count > 0) {
        curListNode = AppendList(ListNode(skey, count, skeyOffset));
        if (preListNode == INVALID_SKEY_OFFSET) {
            listHeader = curListNode;
        } else {
            _listNodes[preListNode].next = curListNode;
        }
    }
    listInfo.blockHeader = listHeader;
}

template <typename SKeyType>
inline void SKeyWriter<SKeyType>::SplitListNode(uint32_t cur)
{
    uint32_t cnt = _listNodes[cur].count;
    uint32_t count1, count2;
    count1 = cnt / 2;
    count2 = cnt - count1;
    cnt = count1;
    uint32_t prev = _listNodes[cur].skeyOffset, current = prev;
    while (cnt > 0) {
        current = _skeyNodes[current].next;
        --cnt;
    }
    uint32_t newListNode = AppendList(ListNode(_skeyNodes[current].skey, count2, current, _listNodes[cur].next));
    _listNodes[cur].count = count1;
    _listNodes[cur].next = newListNode;
}

template <typename SKeyType>
inline bool SKeyWriter<SKeyType>::SeekTargetSKey(SKeyType skey, SKeyNode& skeyNode, uint32_t& startListNodePos,
                                                 uint32_t& startNodePos) const
{
    uint32_t curListPos = startListNodePos;
    while (curListPos != INVALID_SKEY_OFFSET) {
        if (_listNodes[curListPos].skey > skey) {
            break;
        }
        uint32_t header = _listNodes[curListPos].skeyOffset;
        startListNodePos = curListPos;
        curListPos = _listNodes[curListPos].next;
        if (startNodePos == INVALID_SKEY_OFFSET || _skeyNodes[startNodePos].skey < _skeyNodes[header].skey) {
            startNodePos = header;
        }
    }

    // locate skey node
    while (startNodePos != INVALID_SKEY_OFFSET) {
        skeyNode = _skeyNodes[startNodePos];
        if (skeyNode.skey > skey) {
            return false;
        }
        if (skeyNode.skey == skey && skeyNode.valueOffset != SKEY_ALL_DELETED_OFFSET) {
            return true;
        }
        // delete all may ==
        assert(skeyNode.skey <= skey);
        startNodePos = skeyNode.next;
    }
    return false;
}

} // namespace indexlibv2::index
