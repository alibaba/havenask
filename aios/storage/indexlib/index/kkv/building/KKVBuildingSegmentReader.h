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

#include "indexlib/index/kkv/Types.h"
#include "indexlib/index/kkv/building/KKVBuildingSegmentIterator.h"
#include "indexlib/index/kkv/building/SKeyWriter.h"
#include "indexlib/index/kkv/pkey_table/ClosedHashPrefixKeyTable.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableBase.h"
#include "indexlib/index/kkv/pkey_table/SeparateChainPrefixKeyTable.h"

namespace indexlibv2::index {

template <typename SKeyType>
class KKVBuildingSegmentReader
{
public:
    using PKeyTable = PrefixKeyTableBase<SKeyListInfo>;

public:
    KKVBuildingSegmentReader(const std::shared_ptr<config::KKVIndexConfig>& kkvConfig,
                             const std::shared_ptr<PKeyTable>& pkeyTable,
                             const std::shared_ptr<SKeyWriter<SKeyType>>& skeyWriter,
                             const std::shared_ptr<KKVValueWriter>& valueWriter)
        : _kkvConfig(kkvConfig)
        , _pkeyTable(pkeyTable)
        , _skeyWriter(skeyWriter)
        , _valueWriter(valueWriter)
    {
    }
    ~KKVBuildingSegmentReader() {}

public:
    KKVBuildingSegmentIterator<SKeyType>* Lookup(PKeyType pkey, autil::mem_pool::Pool* pool,
                                                 KKVMetricsCollector* metricsCollector = nullptr) const
    {
        // TODO(xinfei.sxf) check assert below
        // assert(options && options->sortParams.empty());
        // SKeyListInfo* listInfo = PrefixKeyTableSeeker::FindForRW(_pKeyTable.get(), pkey);
        SKeyListInfo* listInfo = nullptr;
        PKeyTableType tableType = _pkeyTable->GetTableType();
        switch (tableType) {
        case PKeyTableType::SEPARATE_CHAIN: {
            using Table = SeparateChainPrefixKeyTable<SKeyListInfo>;
            listInfo = (static_cast<Table*>(_pkeyTable.get()))->FindForRW(pkey);
            break;
        }
        case PKeyTableType::DENSE: {
            using Table = ClosedHashPrefixKeyTable<SKeyListInfo, PKeyTableType::DENSE>;
            listInfo = (static_cast<Table*>(_pkeyTable.get()))->FindForRW(pkey);
            break;
        }
        case PKeyTableType::CUCKOO: {
            using Table = ClosedHashPrefixKeyTable<SKeyListInfo, PKeyTableType::DENSE>;
            listInfo = (static_cast<Table*>(_pkeyTable.get()))->FindForRW(pkey);
            break;
        }
        case PKeyTableType::ARRAY:
        default:
            assert(false);
        }
        // SKeyListInfo* listInfo = ((PKeyTable*)_pKeyTable.get())->FindForRW(pkey);
        if (listInfo == nullptr) {
            return nullptr;
        }

        return POOL_COMPATIBLE_NEW_CLASS(pool, KKVBuildingSegmentIterator<SKeyType>, _skeyWriter, *listInfo,
                                         _valueWriter, pool, _kkvConfig.get());
    }

private:
    std::shared_ptr<config::KKVIndexConfig> _kkvConfig;
    std::shared_ptr<PKeyTable> _pkeyTable;
    std::shared_ptr<SKeyWriter<SKeyType>> _skeyWriter;
    std::shared_ptr<KKVValueWriter> _valueWriter;

private:
    friend class KKVReaderTest;
};

} // namespace indexlibv2::index
