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
#include "indexlib/index/kkv/dump/PKeyDumper.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableCreator.h"
#include "indexlib/index/kkv/pkey_table/SeparateChainPrefixKeyTable.h"

using namespace std;

namespace indexlibv2::index {

Status PKeyDumper::Init(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                        const indexlib::config::KKVIndexPreference& indexPrefer, size_t totalPkeyCount)
{
    _pkeyTable.reset(PrefixKeyTableCreator<OnDiskPKeyOffset>::Create(directory, indexPrefer, PKeyTableOpenType::WRITE,
                                                                     totalPkeyCount, 0));
    return Status::OK();
}

Status PKeyDumper::Dump(uint64_t pkey, ChunkItemOffset skeyOffset)
{
    if (skeyOffset.chunkOffset >= OnDiskPKeyOffset::MAX_VALID_CHUNK_OFFSET) {
        return Status::OutOfRange("skey chunk offset[%lu] out of range[%lu]", skeyOffset.chunkOffset,
                                  OnDiskPKeyOffset::MAX_VALID_CHUNK_OFFSET);
    }

    if (!_priorPkeyOffset.IsValidOffset()) {
        UpdatePriorPkeyOffset(pkey, skeyOffset);
        return Status::OK();
    }
    if (_priorPkey == pkey) {
        return Status::OK();
    }
    if (skeyOffset.chunkOffset > _priorPkeyOffset.chunkOffset) {
        _priorPkeyOffset.SetBlockHint(skeyOffset.chunkOffset);
    }
    RETURN_STATUS_DIRECTLY_IF_ERROR(DoDump(_priorPkey, _priorPkeyOffset));
    UpdatePriorPkeyOffset(pkey, skeyOffset);
    return Status::OK();
}

Status PKeyDumper::Close()
{
    if (_priorPkeyOffset.IsValidOffset()) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(DoDump(_priorPkey, _priorPkeyOffset));
    }
    _pkeyTable->Close();
    return Status::OK();
}

// TODO: refactor
size_t PKeyDumper::GetTotalDumpSize(const std::shared_ptr<BuildingPKeyTable>& pkeyTable)
{
    assert(pkeyTable);
    PKeyTableType type = pkeyTable->GetTableType();
    if (type == PKeyTableType::SEPARATE_CHAIN) {
        using Table = const SeparateChainPrefixKeyTable<SKeyListInfo>;
        Table* table = static_cast<Table*>(pkeyTable.get());
        return SeparateChainPrefixKeyTable<OnDiskPKeyOffset>::GetDumpSize(pkeyTable->Size(),
                                                                          table->GetRecommendBucketCount());
    }
    if (type == PKeyTableType::DENSE) {
        using Table = const ClosedHashPrefixKeyTable<SKeyListInfo, PKeyTableType::DENSE>;
        Table* table = static_cast<Table*>(pkeyTable.get());
        return ClosedHashPrefixKeyTable<OnDiskPKeyOffset, PKeyTableType::DENSE>::CalculateBuildMemoryUse(
            PKeyTableOpenType::WRITE, pkeyTable->Size(), table->GetOccupancyPct());
    }
    if (type == PKeyTableType::CUCKOO) {
        using Table = const ClosedHashPrefixKeyTable<SKeyListInfo, PKeyTableType::CUCKOO>;
        Table* table = static_cast<Table*>(pkeyTable.get());
        return ClosedHashPrefixKeyTable<OnDiskPKeyOffset, PKeyTableType::CUCKOO>::CalculateBuildMemoryUse(
            PKeyTableOpenType::WRITE, pkeyTable->Size(), table->GetOccupancyPct());
    }
    assert(false);
    return 0;
}
} // namespace indexlibv2::index
