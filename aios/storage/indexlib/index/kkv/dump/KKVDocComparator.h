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

#include <memory>

#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/dump/KKVDocFieldComparator.h"
#include "indexlib/index/kkv/dump/SKeyCollectInfo.h"

namespace indexlibv2::index {

class PackAttributeFormatter;
class AttributeReference;

class KKVDocComparator
{
public:
    void Init(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& kkvIndexConfig);

    bool operator()(SKeyCollectInfo*& lft, SKeyCollectInfo*& rht)
    {
        assert(!lft->isDeletedPKey && !rht->isDeletedPKey);
        if (lft->isDeletedSKey && rht->isDeletedSKey) {
            return _sKeyFieldComparator->Compare(lft, rht);
        }
        if (lft->isDeletedSKey) {
            return true;
        }
        if (rht->isDeletedSKey) {
            return false;
        }
        for (size_t i = 0; i < _sortComps.size(); ++i) {
            const auto& comp = _sortComps[i];
            if (comp->Compare(lft, rht)) {
                return true;
            } else if (comp->Compare(rht, lft)) {
                return false;
            }
        }
        return _sKeyFieldComparator->Compare(lft, rht);
    }

    const std::shared_ptr<KKVDocFieldComparator>& GetSKeyFieldComparator() const { return _sKeyFieldComparator; }

public:
    void AddTimestampComparator(bool isDesc)
    {
        std::shared_ptr<KKVDocFieldComparator> comp(new TimestampComparator(isDesc));
        _sortComps.push_back(comp);
    }
    void AddSingleValueComparator(AttributeReference* ref, bool isDesc)
    {
        std::shared_ptr<KKVDocFieldComparator> comp(new SingleValueComparator(ref, isDesc));
        _sortComps.push_back(comp);
    }

    std::shared_ptr<KKVDocFieldComparator>
    CreateSKeyFieldComparator(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& kkvIndexConfig, bool isDesc);

private:
    void AddSortLayer(const indexlib::config::SortParam& sortParam);

private:
    std::vector<std::shared_ptr<KKVDocFieldComparator>> _sortComps;
    // TODO(chekong.ygm): 依赖SKEY类型特化，才能实现改用SKeyHashComparator
    std::shared_ptr<KKVDocFieldComparator> _sKeyFieldComparator;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _kKVConfig;
    std::shared_ptr<PackAttributeFormatter> _formatter;

private:
    friend class CollectInfoComparatorTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
