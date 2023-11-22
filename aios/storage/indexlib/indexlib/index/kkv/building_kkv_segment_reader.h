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

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/building_kkv_segment_iterator.h"
#include "indexlib/index/kkv/kkv_segment_reader.h"
#include "indexlib/indexlib.h"
//#include "indexlib/index/kkv/prefix_key_table_seeker.h"
#include "indexlib/codegen/codegen_object.h"
#include "indexlib/index/kkv/closed_hash_prefix_key_table.h"
#include "indexlib/index/kkv/prefix_key_table_base.h"
#include "indexlib/index/kkv/separate_chain_prefix_key_table.h"
#include "indexlib/index/kkv/suffix_key_writer.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class BuildingKKVSegmentReader : public KKVSegmentReader
{
public:
    typedef typename index::PrefixKeyTableBase<SKeyListInfo> PKeyTableBase;
    DEFINE_SHARED_PTR(PKeyTableBase);

    typedef typename index::PrefixKeyTableBase<SKeyListInfo> PKeyTable;
    DEFINE_SHARED_PTR(PKeyTable);

    typedef index::SuffixKeyWriter<SKeyType> SKeyWriter;
    DEFINE_SHARED_PTR(SKeyWriter);

public:
    BuildingKKVSegmentReader() {};
    ~BuildingKKVSegmentReader() {};

public:
    void Init(const PKeyTableBasePtr& pkeyTable, const ValueWriterPtr& valueWriter, const SKeyWriterPtr& skeyWriter)
    {
        mPKeyTable = DYNAMIC_POINTER_CAST(PKeyTable, pkeyTable);
        mValueWriter = valueWriter;
        mSKeyWriter = skeyWriter;
    }

    template <class BuildingKKVSegmentIteratorTyped>
    BuildingKKVSegmentIteratorTyped* Lookup(uint64_t pKey, autil::mem_pool::Pool* sessionPool,
                                            const KKVIndexOptions* options,
                                            KVMetricsCollector* metricsCollector = nullptr) const
    {
        assert(options && options->sortParams.empty());
        // SKeyListInfo* listInfo = PrefixKeyTableSeeker::FindForRW(mPKeyTable.get(), pKey);
        SKeyListInfo* listInfo = NULL;
        PKeyTableType tableType = mPKeyTable->GetTableType();
        switch (tableType) {
        case PKT_SEPARATE_CHAIN: {
            typedef SeparateChainPrefixKeyTable<SKeyListInfo> Table;
            listInfo = (static_cast<Table*>(mPKeyTable.get()))->FindForRW(pKey);
            break;
        }
        case PKT_DENSE: {
            typedef ClosedHashPrefixKeyTable<SKeyListInfo, PKT_DENSE> Table;
            listInfo = (static_cast<Table*>(mPKeyTable.get()))->FindForRW(pKey);
            break;
        }
        case PKT_CUCKOO: {
            typedef ClosedHashPrefixKeyTable<SKeyListInfo, PKT_DENSE> Table;
            listInfo = (static_cast<Table*>(mPKeyTable.get()))->FindForRW(pKey);
            break;
        }
        case PKT_ARRAY:
        default:
            assert(false);
        }
        // SKeyListInfo* listInfo = ((PKeyTable*)mPKeyTable.get())->FindForRW(pKey);
        if (listInfo == NULL) {
            return NULL;
        }

        regionid_t regionId = options->GetRegionId();
        if (regionId != INVALID_REGIONID && regionId != listInfo->regionId) {
            IE_LOG(ERROR, "Lookup pKey [%lu] not match regionId [%d:%d] from building segment", pKey, regionId,
                   listInfo->regionId);
            return NULL;
        }

        BuildingKKVSegmentIteratorTyped* iter =
            IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, BuildingKKVSegmentIteratorTyped, sessionPool);
        iter->Init(mSKeyWriter, mValueWriter, *listInfo, options);
        return iter;
    }

private:
    PKeyTablePtr mPKeyTable;
    ValueWriterPtr mValueWriter;
    SKeyWriterPtr mSKeyWriter;
    friend class KKVReaderTest;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, BuildingKKVSegmentReader);
}} // namespace indexlib::index
