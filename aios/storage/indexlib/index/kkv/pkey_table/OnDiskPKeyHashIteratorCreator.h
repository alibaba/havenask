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

#include "indexlib/index/kkv/common/OnDiskPKeyOffset.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/pkey_table/OnDiskClosedHashIterator.h"
#include "indexlib/index/kkv/pkey_table/OnDiskSeparateChainHashIterator.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableIteratorTyped.h"

namespace indexlibv2::index {

template <typename IteratorType>
class OnDiskPKeyHashIterator : public PrefixKeyTableIteratorTyped<OnDiskPKeyOffset, IteratorType>
{
public:
    OnDiskPKeyHashIterator(IteratorType* iter) : PrefixKeyTableIteratorTyped<OnDiskPKeyOffset, IteratorType>(iter) {}

    void Get(uint64_t& key, OnDiskPKeyOffset& value) const override final
    {
        assert(this->IsValid());
        key = this->_iter->Key();
        value = this->_iter->Value();
    }
};

class OnDiskPKeyHashIteratorCreator
{
public:
    OnDiskPKeyHashIteratorCreator();
    ~OnDiskPKeyHashIteratorCreator();

public:
    static PrefixKeyTableIteratorBase<OnDiskPKeyOffset>*
    CreateOnDiskPKeyIterator(const std::shared_ptr<config::KKVIndexConfig>& kkvIndexConfig,
                             const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
    {
        assert(kkvIndexConfig);
        const std::string& hashTypeStr = kkvIndexConfig->GetIndexPreference().GetHashDictParam().GetHashType();
        PKeyTableType tableType = GetPrefixKeyTableType(hashTypeStr);
        switch (tableType) {
        case PKeyTableType::DENSE: {
            using DataIteratorType = OnDiskClosedHashIterator<PKeyTableType::DENSE>;
            using HashIterator = OnDiskPKeyHashIterator<DataIteratorType>;
            DataIteratorType* dataIter = new DataIteratorType();
            dataIter->Open(kkvIndexConfig, indexDirectory);
            dataIter->SortByKey();
            return new HashIterator(dataIter);
        }
        case PKeyTableType::CUCKOO: {
            using DataIteratorType = OnDiskClosedHashIterator<PKeyTableType::CUCKOO>;
            using HashIterator = OnDiskPKeyHashIterator<DataIteratorType>;
            DataIteratorType* dataIter = new DataIteratorType();
            dataIter->Open(kkvIndexConfig, indexDirectory);
            dataIter->SortByKey();
            return new HashIterator(dataIter);
        }
        case PKeyTableType::SEPARATE_CHAIN: {
            using DataIteratorType = OnDiskSeparateChainHashIterator;
            using HashIterator = OnDiskPKeyHashIterator<DataIteratorType>;
            DataIteratorType* dataIter = new DataIteratorType();
            dataIter->Open(kkvIndexConfig, indexDirectory);
            dataIter->SortByKey();
            return new HashIterator(dataIter);
        }
        default:
            assert(false);
        }
        return nullptr;
    }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
