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

#include "indexlib/framework/Segment.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentIteratorFactory.h"
#include "indexlib/index/kkv/common/KKVSegmentIteratorBase.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableIteratorBase.h"
namespace indexlibv2::index {

template <typename SKeyType>
class OnDiskKKVSegmentIterator
{
public:
    using SinglePKeyIterator = KKVSegmentIteratorBase<SKeyType>;

public:
    OnDiskKKVSegmentIterator(size_t segIdx);
    ~OnDiskKKVSegmentIterator();

public:
    void Init(const std::shared_ptr<config::KKVIndexConfig>& kkvIndexConfig,
              const std::shared_ptr<framework::Segment>& segment);

    void ResetBufferParam(size_t readerBufferSize, bool asyncRead);
    void Reset() { _pkeyTableIter->Reset(); }
    bool IsValid() const;
    void MoveToNext() const;
    uint64_t GetPrefixKey() const;
    size_t GetPkeyCount() { return _pkeyTableIter->Size(); }
    SinglePKeyIterator* GetIterator(autil::mem_pool::Pool* pool,
                                    const std::shared_ptr<config::KKVIndexConfig>& indexConfig);

    size_t GetSegmentIdx() const { return _segmentIdx; }
    bool KeepSortSequence() const { return _keepSortSequence; }

private:
    void ResetBufferParam(const std::shared_ptr<indexlib::file_system::FileReader>& reader, size_t readerBufferSize,
                          bool asyncRead);

private:
    using PKeyTableIterator = PrefixKeyTableIteratorBase<OnDiskPKeyOffset>;

private:
    std::shared_ptr<PKeyTableIterator> _pkeyTableIter;
    std::shared_ptr<indexlib::file_system::FileReader> _skeyReader;
    std::shared_ptr<indexlib::file_system::FileReader> _valueReader;
    std::unique_ptr<KKVBuiltSegmentIteratorFactory<SKeyType>> _iteratorFactory;
    size_t _segmentIdx;
    std::shared_ptr<config::KKVIndexConfig> _kkvIndexConfig;
    bool _storeTs;
    bool _keepSortSequence;
    uint32_t _defaultTs;

private:
    AUTIL_LOG_DECLARE();
};

MARK_EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(OnDiskKKVSegmentIterator);

} // namespace indexlibv2::index
