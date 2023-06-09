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
#include "indexlib/index/kkv/hint_pkey_collector.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, HintPKeyCollector);

HintPKeyCollector::HintPKeyCollector(const DumpPKeyTablePtr& pkeyTabe) : mPKeyTable(pkeyTabe) {}

HintPKeyCollector::~HintPKeyCollector() {}

void HintPKeyCollector::Collect(uint64_t pkey, OnDiskPKeyOffset offset)
{
    while ((mBuffer.size() > 1 && mBuffer.back().second.chunkOffset != offset.chunkOffset) ||
           (offset.inChunkOffset == 0 && !mBuffer.empty())) {
        auto curKey = mBuffer.front().first;
        auto curOffset = mBuffer.front().second;
        assert(curKey != pkey);
        mBuffer.pop_front();
        curOffset.SetBlockHint(offset.chunkOffset);
        bool ret = mPKeyTable->Insert(curKey, curOffset);
        assert(ret);
        (void)ret;
    }

    mBuffer.push_back({pkey, offset});
}

void HintPKeyCollector::Close()
{
    while (!mBuffer.empty()) {
        auto curKey = mBuffer.front().first;
        auto curOffset = mBuffer.front().second;
        mBuffer.pop_front();
        bool ret = mPKeyTable->Insert(curKey, curOffset);
        assert(ret);
        (void)ret;
    }
}
}} // namespace indexlib::index
