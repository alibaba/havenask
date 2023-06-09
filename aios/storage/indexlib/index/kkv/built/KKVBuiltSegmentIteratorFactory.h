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

#include "autil/Log.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentIterator.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentIteratorOption.h"

namespace indexlibv2::index {

template <typename SKeyType>
class KKVBuiltSegmentIteratorFactory
{
public:
    KKVBuiltSegmentIteratorFactory(const config::KKVIndexConfig* indexConfig, bool isOnline, bool storeTs,
                                   uint32_t defaultTs, bool keepSortSeq, bool valueInline,
                                   indexlib::file_system::FileReader* skeyFileReader,
                                   indexlib::file_system::FileReader* valueFileReader)
        : _indexConfig(indexConfig)
        , _isOnline(isOnline)
        , _storeTs(storeTs)
        , _defaultTs(defaultTs)
        , _keepSortSeq(keepSortSeq)
        , _storeExpireTime(indexConfig->StoreExpireTime())
        , _valueInline(valueInline)
        , _skeyFileReader(skeyFileReader)
        , _valueFileReader(valueFileReader)
    {
    }
    ~KKVBuiltSegmentIteratorFactory() = default;

public:
    // TODO(xinfei.sxf) refactor to Result
    FL_LAZY(std::pair<Status, KKVBuiltSegmentIteratorBase<SKeyType>*>)
    Create(OnDiskPKeyOffset firstSkeyOffset, autil::mem_pool::Pool* pool, KVMetricsCollector* metricsCollector) const
    {
        using Helper = KKVBuiltSegmentIteratorOptionHelper;
        int8_t optionBits = Helper::generateOptionBits(_valueInline, _storeTs, _storeExpireTime);
        switch (optionBits) {
#define CASE_MACRO(OPTION_BITS)                                                                                        \
    case OPTION_BITS: {                                                                                                \
        using Option = KKVBuiltSegmentIteratorOption<Helper::valueInline(OPTION_BITS), Helper::storeTs(OPTION_BITS),   \
                                                     Helper::storeExpireTime(OPTION_BITS)>;                            \
        FL_CORETURN FL_COAWAIT InnerCreate<Option>(firstSkeyOffset, pool, metricsCollector);                           \
    }
            CASE_MACRO(0);
            CASE_MACRO(1);
            CASE_MACRO(2);
            CASE_MACRO(3);
            CASE_MACRO(4);
            CASE_MACRO(5);
            CASE_MACRO(6);
            CASE_MACRO(7);
#undef CASE_MACRO
        default:
            assert(false);
            FL_CORETURN std::make_pair(Status::Unknown(), nullptr);
        }
    }

private:
    template <typename Option>
    FL_LAZY(std::pair<Status, KKVBuiltSegmentIteratorBase<SKeyType>*>)
    InnerCreate(OnDiskPKeyOffset firstSkeyOffset, autil::mem_pool::Pool* pool,
                KVMetricsCollector* metricsCollector) const
    {
        using IterType = KKVBuiltSegmentIterator<SKeyType, Option>;
        IterType* iterator = POOL_COMPATIBLE_NEW_CLASS(pool, IterType, _indexConfig, _keepSortSeq, _defaultTs,
                                                       _isOnline, pool, metricsCollector);

        auto status = FL_COAWAIT iterator->Init(_skeyFileReader, _valueFileReader, firstSkeyOffset);
        if (!status.IsOK()) {
            POOL_COMPATIBLE_DELETE_CLASS(pool, iterator);
            iterator = nullptr;
            AUTIL_LOG(ERROR, "KKVBuiltSegmentIterator init failed, status[%s]", status.ToString().c_str());
        }
        FL_CORETURN std::make_pair(status, iterator);
    }

private:
    const config::KKVIndexConfig* _indexConfig;
    bool _isOnline;
    bool _storeTs;
    uint32_t _defaultTs;
    bool _keepSortSeq;
    bool _storeExpireTime;
    bool _valueInline;
    indexlib::file_system::FileReader* _skeyFileReader;
    indexlib::file_system::FileReader* _valueFileReader;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, KKVBuiltSegmentIteratorFactory, SKeyType);
} // namespace indexlibv2::index
