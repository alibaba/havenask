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

#include <deque>
#include <memory>
#include <utility>

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/prefix_key_table_base.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class HintPKeyCollector
{
private:
    typedef PrefixKeyTableBase<OnDiskPKeyOffset> DumpPKeyTable;
    DEFINE_SHARED_PTR(DumpPKeyTable);

public:
    HintPKeyCollector(const DumpPKeyTablePtr& pkeyTabe);
    ~HintPKeyCollector();

public:
    void Collect(uint64_t pkey, OnDiskPKeyOffset offset);
    void Close();
    bool Empty() const { return (mPKeyTable->Size() == 0) && mBuffer.empty(); }

private:
    DumpPKeyTablePtr mPKeyTable;
    std::deque<std::pair<PKeyType, OnDiskPKeyOffset>> mBuffer;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(HintPKeyCollector);
}} // namespace indexlib::index
