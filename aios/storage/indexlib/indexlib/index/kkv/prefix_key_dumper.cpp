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
#include "indexlib/index/kkv/prefix_key_dumper.h"

#include "indexlib/index/kkv/prefix_key_table_creator.h"
#include "indexlib/index/kkv/separate_chain_prefix_key_table.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PrefixKeyDumper);

PrefixKeyDumper::PrefixKeyDumper() : mLastKey(uint64_t()), mSize(0) {}

void PrefixKeyDumper::Init(const KKVIndexPreference& indexPreference,
                           const DumpablePKeyOffsetIteratorPtr& pkeyOffsetIter, const DirectoryPtr& directory,
                           size_t totalPkeyCount)
{
    mPkeyOffsetIter = pkeyOffsetIter;
    mPKeyTable.reset(
        PrefixKeyTableCreator<OnDiskPKeyOffset>::Create(directory, indexPreference, PKOT_WRITE, totalPkeyCount));
    mHintColector.reset(new HintPKeyCollector(mPKeyTable));
}

void PrefixKeyDumper::Flush()
{
    assert(mPKeyTable);
    while (mPkeyOffsetIter->IsValid()) {
        OnDiskPKeyOffset offset;
        uint64_t pkey = mPkeyOffsetIter->Get(offset);
        if (mHintColector->Empty() || pkey != mLastKey) {
            // ignore same pkey in neighbor chunk
            mHintColector->Collect(pkey, offset);
            mLastKey = pkey;
            ++mSize;
        }
        mPkeyOffsetIter->MoveToNext();
    }
}

void PrefixKeyDumper::Close()
{
    Flush();
    mHintColector->Close();
    mPKeyTable->Close();
}

size_t PrefixKeyDumper::GetTotalDumpSize(const BuildingPKeyTablePtr& pkeyTable)
{
    assert(pkeyTable);
    PKeyTableType type = pkeyTable->GetTableType();
    if (type == PKT_SEPARATE_CHAIN) {
        typedef SeparateChainPrefixKeyTable<SKeyListInfo> Table;
        Table* table = static_cast<Table*>(pkeyTable.get());
        return SeparateChainPrefixKeyTable<OnDiskPKeyOffset>::GetDumpSize(pkeyTable->Size(),
                                                                          table->GetRecommendBucketCount());
    }
    if (type == PKT_DENSE) {
        typedef ClosedHashPrefixKeyTable<SKeyListInfo, PKT_DENSE> Table;
        Table* table = static_cast<Table*>(pkeyTable.get());
        return ClosedHashPrefixKeyTable<OnDiskPKeyOffset, PKT_DENSE>::CalculateBuildMemoryUse(
            PKOT_WRITE, pkeyTable->Size(), table->GetOccupancyPct());
    }
    if (type == PKT_CUCKOO) {
        typedef ClosedHashPrefixKeyTable<SKeyListInfo, PKT_CUCKOO> Table;
        Table* table = static_cast<Table*>(pkeyTable.get());
        return ClosedHashPrefixKeyTable<OnDiskPKeyOffset, PKT_CUCKOO>::CalculateBuildMemoryUse(
            PKOT_WRITE, pkeyTable->Size(), table->GetOccupancyPct());
    }
    assert(false);
    return 0;
}
}} // namespace indexlib::index
