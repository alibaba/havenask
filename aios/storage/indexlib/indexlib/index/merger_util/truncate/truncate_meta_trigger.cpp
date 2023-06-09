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
#include "indexlib/index/merger_util/truncate/truncate_meta_trigger.h"

#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, TruncateMetaTrigger);

TruncateMetaTrigger::TruncateMetaTrigger(uint64_t truncThreshold) : mTruncThreshold(truncThreshold)
{
    (void)mTruncThreshold;
}

TruncateMetaTrigger::~TruncateMetaTrigger() {}

bool TruncateMetaTrigger::NeedTruncate(const TruncateTriggerInfo& info)
{
    if (!mMetaReader) {
        IE_LOG(WARN, "truncate_meta for some index does not "
                     "exist in full index, check your config!");
        return false;
    }
    return mMetaReader->IsExist(info.GetDictKey());
}

void TruncateMetaTrigger::SetTruncateMetaReader(const TruncateMetaReaderPtr& reader) { mMetaReader = reader; }
} // namespace indexlib::index::legacy
