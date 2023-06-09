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
#ifndef __INDEXLIB_COLLECT_INFO_COMPARATOR_H
#define __INDEXLIB_COLLECT_INFO_COMPARATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/data_collector_define.h"
#include "indexlib/index/kkv/kkv_comparator.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(common, PackAttributeFormatter);
DECLARE_REFERENCE_CLASS(common, AttributeReference);

namespace indexlib { namespace index {

class CollectInfoValueComp
{
public:
    typedef SKeyCollectInfo CollectInfo;

public:
    CollectInfoValueComp() {}
    ~CollectInfoValueComp() {}

public:
    void Init(const config::KKVIndexConfigPtr& kkvConfig);

    bool operator()(CollectInfo*& lft, CollectInfo*& rht)
    {
        assert(!lft->isDeletedPKey && !rht->isDeletedPKey);
        if (lft->isDeletedSKey && rht->isDeletedSKey) {
            return mSkeyComparator->Compare(lft, rht);
        }
        if (lft->isDeletedSKey) {
            return true;
        }
        if (rht->isDeletedSKey) {
            return false;
        }
        for (size_t i = 0; i < mSortComps.size(); ++i) {
            const KKVComparatorPtr& comp = mSortComps[i];
            if (comp->Compare(lft, rht)) {
                return true;
            } else if (comp->Compare(rht, lft)) {
                return false;
            }
        }
        return mSkeyComparator->Compare(lft, rht);
    }

    const KKVComparatorPtr& GetSKeyComparator() const { return mSkeyComparator; }

public:
    void AddTimestampComparator(bool isDesc)
    {
        KKVComparatorPtr comp(new TimestampComparator(isDesc));
        mSortComps.push_back(comp);
    }
    void AddSingleValueComparator(common::AttributeReference* ref, bool isDesc)
    {
        KKVComparatorPtr comp(new SingleValueComparator(ref, isDesc));
        mSortComps.push_back(comp);
    }

    KKVComparatorPtr CreateSKeyComparator(const config::KKVIndexConfigPtr& kkvConfig, bool isDesc);

private:
    void AddSortLayer(const config::SortParam& sortParam);

private:
    std::vector<KKVComparatorPtr> mSortComps;
    KKVComparatorPtr mSkeyComparator;
    config::KKVIndexConfigPtr mKKVConfig;
    common::PackAttributeFormatterPtr mFormatter;

private:
    friend class CollectInfoComparatorTest;
    IE_LOG_DECLARE();
};

template <typename SKeyType>
class CollectInfoKeyComp
{
public:
    typedef SKeyCollectInfo CollectInfo;

public:
    CollectInfoKeyComp() {}
    ~CollectInfoKeyComp() {}

public:
    bool operator()(CollectInfo*& lft, CollectInfo*& rht)
    {
        assert(!lft->isDeletedPKey && !rht->isDeletedPKey);
        return (SKeyType)lft->skey < (SKeyType)rht->skey;
    }
};
}} // namespace indexlib::index

#endif //__INDEXLIB_COLLECT_INFO_COMPARATOR_H
