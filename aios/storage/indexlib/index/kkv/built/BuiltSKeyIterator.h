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
#include "autil/MultiValueType.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/common/ChunkReader.h"
#include "indexlib/index/kkv/common/KKVMetricsCollector.h"
#include "indexlib/index/kkv/common/OnDiskPKeyOffset.h"
#include "indexlib/index/kkv/common/SKeyIteratorBase.h"
#include "indexlib/index/kkv/common/Trait.h"

namespace indexlibv2::index {
template <typename SKeyType, typename Option>
class BuiltSKeyIterator final : public SKeyIteratorBase<SKeyType>
{
    static constexpr bool valueInline = Option::valueInline;
    static constexpr bool storeTs = Option::storeTs;
    static constexpr bool storeExpireTime = Option::storeExpireTime;
    using Base = SKeyIteratorBase<SKeyType>;
    using SKeyNode = typename SKeyNodeTraits<SKeyType, valueInline>::SKeyNode;

public:
    BuiltSKeyIterator(uint32_t defaultTs, bool isOnline, autil::mem_pool::Pool* sessionPool, int32_t fixedValueLen,
                      const indexlib::file_system::ReadOption& readOption)
        : _chunkReader(readOption, /*chunkOffsetAlignBit*/ 0u, isOnline, sessionPool)
        , _defaultTs(defaultTs)
        , _fixedValueLen(fixedValueLen)
    {
    }
    ~BuiltSKeyIterator() = default;

public:
    FL_LAZY(Status)
    Init(indexlib::file_system::FileReader* fileReader, bool isCompressFile, OnDiskPKeyOffset firstSkeyOffset);
    uint32_t GetSKeyChunkCount() const { return _chunkReader.GetReadCount(); }

public:
    void MoveToNext() override;
    bool IsValid() const override { return _isValid; }
    SKeyType GetSKey() const override { return _skeyNode.skey; }
    bool IsDeleted() const override { return _skeyNode.IsSKeyDeleted(); }
    uint32_t GetTs() const override;
    uint32_t GetExpireTime() const override;
    ValueOffset GetValueOffset() const
    {
        static_assert(!valueInline);
        return _valueOffset;
    }
    autil::StringView GetValue() const
    {
        static_assert(valueInline);
        return _value;
    }

public:
    // for coroutine
    bool NeedSwitchChunk() { return _inChunkOffset >= _chunkData.length; }
    FL_LAZY(bool) SwitchChunk();
    void MoveToNextInChunk();

private:
    void DoMoveToNext();
    uint32_t DoGetValue();
    void DoGetValueOffset();
    FL_LAZY(Status)
    InitChunkReader(indexlib::file_system::FileReader* fileReader, bool isCompressFile,
                    OnDiskPKeyOffset firstSkeyOffset);

private:
    ChunkReader _chunkReader;
    ChunkData _chunkData = ChunkData::Invalid();
    uint32_t _chunkOffset = 0u;
    uint32_t _inChunkOffset = 0u;

private:
    SKeyNode _skeyNode;
    const char* _skeyNodeAddr = nullptr;
    bool _isValid = false;
    uint32_t _defaultTs = 0;
    bool _isLastNode = false;

protected:
    int32_t _fixedValueLen = -1;
    autil::StringView _value;
    ValueOffset _valueOffset = ValueOffset::Invalid();
};

template <typename SKeyType, typename Option>
FL_LAZY(Status)
BuiltSKeyIterator<SKeyType, Option>::Init(indexlib::file_system::FileReader* fileReader, bool isCompressFile,
                                          OnDiskPKeyOffset firstSkeyOffset)
{
    auto status = FL_COAWAIT InitChunkReader(fileReader, isCompressFile, firstSkeyOffset);
    if (!status.IsOK()) {
        FL_CORETURN status;
    }

    _inChunkOffset = firstSkeyOffset.inChunkOffset;
    _chunkOffset = firstSkeyOffset.chunkOffset;
    _chunkData = FL_COAWAIT _chunkReader.Read(_chunkOffset);
    _isValid = _chunkData.IsValid();
    if (!_isValid) {
        FL_CORETURN Status::IOError("init chunk data with offset[%lu] failed", _chunkOffset);
    }

    MoveToNext();
    Base::_hasPKeyDeleted = _skeyNode.IsPKeyDeleted();
    if (Base::_hasPKeyDeleted) {
        Base::_pkeyDeletedTs = GetTs();
        MoveToNext();
    }
    FL_CORETURN Status::OK();
}

template <typename SKeyType, typename Option>
void BuiltSKeyIterator<SKeyType, Option>::MoveToNext()
{
    if (_isLastNode) {
        _isValid = false;
        return;
    }
    if (NeedSwitchChunk()) {
        if (!future_lite::interface::syncAwait(SwitchChunk())) {
            return;
        }
    }
    DoMoveToNext();
}

template <typename SKeyType, typename Option>
void BuiltSKeyIterator<SKeyType, Option>::DoMoveToNext()
{
    _skeyNodeAddr = _chunkData.data + _inChunkOffset;
    _skeyNode = *reinterpret_cast<const SKeyNode*>(_skeyNodeAddr);

    uint32_t valueAreaLen = 0;
    if constexpr (valueInline) {
        valueAreaLen = DoGetValue();
    } else {
        DoGetValueOffset();
    }

    if (_skeyNode.IsLastNode()) {
        _isLastNode = true;
        return;
    }
    _inChunkOffset += sizeof(SKeyNode);
    if constexpr (storeTs) {
        _inChunkOffset += sizeof(uint32_t);
    }
    if constexpr (storeExpireTime) {
        _inChunkOffset += sizeof(uint32_t);
    }
    if constexpr (valueInline) {
        _inChunkOffset += valueAreaLen;
    }
}

template <typename SKeyType, typename Option>
void BuiltSKeyIterator<SKeyType, Option>::MoveToNextInChunk()
{
    if (_isLastNode) {
        _isValid = false;
        return;
    }
    assert(!NeedSwitchChunk());
    DoMoveToNext();
}

template <typename SKeyType, typename Option>
uint32_t BuiltSKeyIterator<SKeyType, Option>::DoGetValue()
{
    static_assert(valueInline);

    uint32_t valueAreaLen = 0u;
    if (!_skeyNode.HasValue()) {
        _value = autil::StringView::empty_instance();
        return valueAreaLen;
    }

    const char* valueAddr = _skeyNodeAddr + sizeof(SKeyNode);
    if constexpr (storeTs) {
        valueAddr += sizeof(uint32_t);
    }
    if constexpr (storeExpireTime) {
        valueAddr += sizeof(uint32_t);
    }

    if (_fixedValueLen > 0) {
        _value = autil::StringView(valueAddr, _fixedValueLen);
        valueAreaLen = _fixedValueLen;
    } else {
        autil::MultiChar mc;
        mc.init(valueAddr);
        _value = autil::StringView(mc.data(), mc.size());
        valueAreaLen = mc.length();
    }
    return valueAreaLen;
}

template <typename SKeyType, typename Option>
void BuiltSKeyIterator<SKeyType, Option>::DoGetValueOffset()
{
    static_assert(!valueInline);
    _valueOffset = _skeyNode.GetValueOffset();
}

template <typename SKeyType, typename Option>
uint32_t BuiltSKeyIterator<SKeyType, Option>::GetTs() const
{
    if constexpr (!storeTs) {
        return _defaultTs;
    }
    const char* tsAddr = _skeyNodeAddr + sizeof(SKeyNode);
    uint32_t ts = 0u;
    ::memcpy(&ts, tsAddr, sizeof(ts));
    return ts;
}

template <typename SKeyType, typename Option>
uint32_t BuiltSKeyIterator<SKeyType, Option>::GetExpireTime() const
{
    if constexpr (!storeExpireTime) {
        return indexlib::UNINITIALIZED_EXPIRE_TIME;
    }
    const char* expireTimeAddr = _skeyNodeAddr + sizeof(SKeyNode);
    if constexpr (storeTs) {
        expireTimeAddr += sizeof(uint32_t);
    }
    uint32_t expireTime = 0u;
    ::memcpy(&expireTime, expireTimeAddr, sizeof(expireTime));
    return expireTime;
}

template <typename SKeyType, typename Option>
FL_LAZY(bool)
BuiltSKeyIterator<SKeyType, Option>::SwitchChunk()
{
    _inChunkOffset = 0;
    _chunkOffset = _chunkReader.CalcNextChunkOffset(_chunkOffset, _chunkData.length);
    _chunkData = FL_COAWAIT _chunkReader.Read(_chunkOffset);
    _isValid = _chunkData.IsValid();
    FL_CORETURN _isValid;
}

template <typename SKeyType, typename Option>
FL_LAZY(Status)
BuiltSKeyIterator<SKeyType, Option>::InitChunkReader(indexlib::file_system::FileReader* fileReader, bool isCompressFile,
                                                     OnDiskPKeyOffset firstSkeyOffset)
{
    _chunkReader.Init(fileReader, isCompressFile);
    uint64_t blockOffset = firstSkeyOffset.GetBlockOffset();
    uint64_t hintSize = firstSkeyOffset.GetHintSize();
    hintSize = std::min(hintSize, fileReader->GetLength() - blockOffset);
    if (hintSize > OnDiskPKeyOffset::HINT_BLOCK_SIZE) {
        FL_CORETURN FL_COAWAIT _chunkReader.Prefetch(hintSize, blockOffset);
    }
    FL_CORETURN Status::OK();
}

} // namespace indexlibv2::index
