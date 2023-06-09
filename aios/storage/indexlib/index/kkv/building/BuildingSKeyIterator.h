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

#include "indexlib/index/kkv/building/SKeyWriter.h"
#include "indexlib/index/kkv/common/SKeyIteratorBase.h"

namespace indexlibv2::index {
template <typename SKeyType>
class BuildingSKeyIterator final : public SKeyIteratorBase<SKeyType>
{
    using Base = SKeyIteratorBase<SKeyType>;
    using SKeyNode = typename SKeyWriter<SKeyType>::SKeyNode;

public:
    BuildingSKeyIterator(const std::shared_ptr<SKeyWriter<SKeyType>>& skeyWriter, const SKeyListInfo& skeyListInfo)
        : _skeyWriter(skeyWriter)
        , _skipListIdx(skeyListInfo.blockHeader)
        , _skeyOffset(skeyListInfo.skeyHeader)
    {
        assert(_skeyWriter);
        _skeyNode = _skeyWriter->GetSKeyNode(_skeyOffset);
        if (_skeyNode.valueOffset != SKEY_ALL_DELETED_OFFSET) {
            return;
        }
        Base::_hasPKeyDeleted = true;
        Base::_pkeyDeletedTs = _skeyNode.timestamp;
        MoveToNext();
    }
    ~BuildingSKeyIterator() {}

public:
    SKeyType GetSKey() const override { return _skeyNode.skey; }
    bool IsDeleted() const override { return _skeyNode.valueOffset == INVALID_VALUE_OFFSET; }
    uint32_t GetTs() const override { return _skeyNode.timestamp; }
    uint32_t GetExpireTime() const override { return _skeyNode.expireTime; }
    bool IsValid() const override { return _skeyOffset != INVALID_SKEY_OFFSET; }
    void MoveToNext() override
    {
        _skeyOffset = _skeyNode.next;
        if (!IsValid()) {
            return;
        }
        _skeyNode = _skeyWriter->GetSKeyNode(_skeyOffset);
    }
    uint64_t GetValueOffset() const { return _skeyNode.valueOffset; }
    bool MoveToSKey(SKeyType skey) { return _skeyWriter->SeekTargetSKey(skey, _skeyNode, _skipListIdx, _skeyOffset); }

private:
    uint32_t _pkeyDeletedTs = 0u;
    bool _hasPKeyDeleted = false;
    std::shared_ptr<SKeyWriter<SKeyType>> _skeyWriter;
    SKeyNode _skeyNode = SKeyNode(0, 0, 0, indexlib::UNINITIALIZED_EXPIRE_TIME, INVALID_SKEY_OFFSET);
    uint32_t _skipListIdx = 0u;
    uint32_t _skeyOffset = 0u;
};

} // namespace indexlibv2::index