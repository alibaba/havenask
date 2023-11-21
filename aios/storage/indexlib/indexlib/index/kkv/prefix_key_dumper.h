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
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/dumpable_pkey_offset_iterator.h"
#include "indexlib/index/kkv/hint_pkey_collector.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/prefix_key_table_base.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PrimeNumberTable.h"

namespace indexlib { namespace index {

class PrefixKeyDumper
{
private:
    typedef PrefixKeyTableBase<SKeyListInfo> BuildingPKeyTable;
    DEFINE_SHARED_PTR(BuildingPKeyTable);

    typedef PrefixKeyTableBase<OnDiskPKeyOffset> DumpPKeyTable;
    DEFINE_SHARED_PTR(DumpPKeyTable);

public:
    PrefixKeyDumper();
    ~PrefixKeyDumper() {}

public:
    void Init(const config::KKVIndexPreference& indexPreference, const DumpablePKeyOffsetIteratorPtr& pkeyOffsetIter,
              const file_system::DirectoryPtr& directory, size_t totalPkeyCount);

    void Flush();
    void Close();

    static size_t GetTotalDumpSize(const BuildingPKeyTablePtr& pkeyTable);
    size_t Size() const { return mSize; }

private:
    DumpablePKeyOffsetIteratorPtr mPkeyOffsetIter;
    DumpPKeyTablePtr mPKeyTable;
    uint64_t mLastKey;
    size_t mSize;
    HintPKeyCollectorPtr mHintColector;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrefixKeyDumper);
}} // namespace indexlib::index
