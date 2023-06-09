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
#ifndef __INDEXLIB_ON_DISK_PKEY_HASH_ITERATOR_CREATOR_H
#define __INDEXLIB_ON_DISK_PKEY_HASH_ITERATOR_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/on_disk_closed_hash_iterator.h"
#include "indexlib/index/kkv/on_disk_separate_chain_hash_iterator.h"
#include "indexlib/index/kkv/prefix_key_table_iterator_typed.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename IteratorType>
class OnDiskPKeyHashIterator : public PrefixKeyTableIteratorTyped<OnDiskPKeyOffset, IteratorType>
{
public:
    OnDiskPKeyHashIterator(IteratorType* iter) : PrefixKeyTableIteratorTyped<OnDiskPKeyOffset, IteratorType>(iter) {}

    void Get(uint64_t& key, OnDiskPKeyOffset& value) const override final
    {
        assert(this->IsValid());
        key = this->mIter->Key();
        value = this->mIter->Value();
    }
};

class OnDiskPKeyHashIteratorCreator
{
public:
    OnDiskPKeyHashIteratorCreator();
    ~OnDiskPKeyHashIteratorCreator();

public:
    static PrefixKeyTableIteratorBase<OnDiskPKeyOffset>*
    CreateOnDiskPKeyIterator(const config::KKVIndexConfigPtr& kkvIndexConfig,
                             const index_base::SegmentData& segmentData)
    {
        assert(kkvIndexConfig);
        const std::string& hashTypeStr = kkvIndexConfig->GetIndexPreference().GetHashDictParam().GetHashType();
        PKeyTableType tableType = GetPrefixKeyTableType(hashTypeStr);
        switch (tableType) {
        case PKT_DENSE: {
            typedef OnDiskClosedHashIterator<PKT_DENSE> DataIteratorType;
            typedef OnDiskPKeyHashIterator<DataIteratorType> HashIterator;
            DataIteratorType* dataIter = new DataIteratorType();
            dataIter->Open(kkvIndexConfig, segmentData);
            dataIter->SortByKey();
            return new HashIterator(dataIter);
        }
        case PKT_CUCKOO: {
            typedef OnDiskClosedHashIterator<PKT_CUCKOO> DataIteratorType;
            typedef OnDiskPKeyHashIterator<DataIteratorType> HashIterator;
            DataIteratorType* dataIter = new DataIteratorType();
            dataIter->Open(kkvIndexConfig, segmentData);
            dataIter->SortByKey();
            return new HashIterator(dataIter);
        }
        case PKT_SEPARATE_CHAIN: {
            typedef OnDiskSeparateChainHashIterator DataIteratorType;
            typedef OnDiskPKeyHashIterator<DataIteratorType> HashIterator;
            DataIteratorType* dataIter = new DataIteratorType();
            dataIter->Open(kkvIndexConfig, segmentData);
            dataIter->SortByKey();
            return new HashIterator(dataIter);
        }
        default:
            assert(false);
        }
        return NULL;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskPKeyHashIteratorCreator);
}} // namespace indexlib::index

#endif //__INDEXLIB_ON_DISK_PKEY_HASH_ITERATOR_CREATOR_H
