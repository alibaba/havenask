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

#include "future_lite/CoroInterface.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/kkv/built/KKVBuiltValueFetcher.h"
#include "indexlib/index/kkv/common/KKVResultBuffer.h"
#include "indexlib/index/kkv/common/KKVSegmentIteratorBase.h"
#include "indexlib/index/kkv/common/OnDiskPKeyOffset.h"
#include "indexlib/index/kkv/common/SKeySearchContext.h"

namespace indexlibv2::index {

template <typename SKeyType>
class KKVBuiltSegmentIteratorBase : public KKVSegmentIteratorBase<SKeyType>
{
public:
    KKVBuiltSegmentIteratorBase(autil::mem_pool::Pool* pool, const config::KKVIndexConfig* indexConfig,
                                bool keepSortSequence)
        : KKVSegmentIteratorBase<SKeyType>(pool, indexConfig, keepSortSequence)
    {
    }
    ~KKVBuiltSegmentIteratorBase() = default;

public:
    // for coroutine
    virtual FL_LAZY(bool) GetSKeysAsync(SKeySearchContext<SKeyType>* skeyContext, uint64_t minTsInSecond,
                                        KKVDocs& result, KKVBuiltValueFetcher& valueFetcher) = 0;
};

} // namespace indexlibv2::index
