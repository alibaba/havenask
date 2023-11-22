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

#include "autil/NoCopyable.h"
#include "indexlib/index/primary_key/PrimaryKeyPair.h"
#include "indexlib/index/primary_key/SegmentDataAdapter.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"

namespace indexlibv2::index {

template <typename Key>
class IPrimaryKeyIterator : public autil::NoCopyable
{
public:
    IPrimaryKeyIterator() {}
    virtual ~IPrimaryKeyIterator() {}

public:
    using PKPairTyped = PKPair<Key, docid64_t>;

public:
    [[nodiscard]] virtual bool Init(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
                                    const std::vector<SegmentDataAdapter::SegmentDataType>& segments) = 0;
    virtual bool HasNext() const = 0;

    virtual bool Next(PKPairTyped& pkPair) = 0;
};

} // namespace indexlibv2::index
