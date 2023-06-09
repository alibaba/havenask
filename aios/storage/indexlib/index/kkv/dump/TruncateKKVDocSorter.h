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

#include "indexlib/index/kkv/dump/KKVDocComparator.h"
#include "indexlib/index/kkv/dump/KKVDocSorterBase.h"

namespace indexlibv2::index {

class TruncateKKVDocSorter : public KKVDocSorterBase
{
public:
    TruncateKKVDocSorter(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& kkvIndexConfig)
        : KKVDocSorterBase(kkvIndexConfig)
        , _skeyCountLimits(0)
        , _skeyFieldType(ft_unknown)
    {
    }

    virtual ~TruncateKKVDocSorter() = default;

public:
    bool Init() override;
    std::vector<std::pair<bool, KKVDoc>> Sort() override;
    bool KeepSkeySorted() const override { return false; }

private:
    uint32_t _skeyCountLimits;
    FieldType _skeyFieldType;
    KKVDocComparator _docComparator;
};

} // namespace indexlibv2::index
