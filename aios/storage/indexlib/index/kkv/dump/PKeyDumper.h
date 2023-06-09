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

#include "indexlib/index/kkv/common/ChunkDefine.h"
#include "indexlib/index/kkv/common/OnDiskPKeyOffset.h"
#include "indexlib/index/kkv/common/SKeyListInfo.h"
#include "indexlib/index/kkv/config/KKVIndexPreference.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableBase.h"

namespace indexlibv2::index {

class PKeyDumper
{
private:
    using BuildingPKeyTable = const PrefixKeyTableBase<SKeyListInfo>;
    using DumpPKeyTable = PrefixKeyTableBase<OnDiskPKeyOffset>;

public:
    PKeyDumper() = default;
    ~PKeyDumper() = default;

public:
    Status Init(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                const indexlib::config::KKVIndexPreference& indexPrefer, size_t totalPkeyCount);
    Status Dump(uint64_t pkey, ChunkItemOffset skeyOffset);
    Status Close();
    size_t GetPKeyCount() const { return _pkeyCount; }
    static size_t GetTotalDumpSize(const std::shared_ptr<BuildingPKeyTable>& pkeyTable);

private:
    Status DoDump(uint64_t pkey, OnDiskPKeyOffset pkeyOffset)
    {
        assert(pkeyOffset.IsValidOffset());
        if (!_pkeyTable->Insert(pkey, pkeyOffset)) {
            return Status::Unknown("insert pkey [%lu] failed", _priorPkey);
        }
        return Status::OK();
    }

    void UpdatePriorPkeyOffset(uint64_t pkey, ChunkItemOffset curSkeyOffset)
    {
        ++_pkeyCount;
        _priorPkey = pkey;
        _priorPkeyOffset = OnDiskPKeyOffset();
        _priorPkeyOffset.chunkOffset = curSkeyOffset.chunkOffset;
        _priorPkeyOffset.inChunkOffset = curSkeyOffset.inChunkOffset;
    }

private:
    std::unique_ptr<DumpPKeyTable> _pkeyTable;
    uint64_t _priorPkey = 0UL;
    OnDiskPKeyOffset _priorPkeyOffset;
    size_t _pkeyCount = 0UL;
};

} // namespace indexlibv2::index