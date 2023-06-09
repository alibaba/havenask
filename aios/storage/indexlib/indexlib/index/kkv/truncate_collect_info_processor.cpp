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
#include "indexlib/index/kkv/truncate_collect_info_processor.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, TruncateCollectInfoProcessor);

bool TruncateCollectInfoProcessor::Init(const KKVIndexConfigPtr& kkvConfig)
{
    CollectInfoProcessor::Init(kkvConfig);
    mSKeyCountLimits = mKKVConfig->GetSuffixKeyTruncateLimits();
    if (!mKKVConfig->NeedSuffixKeyTruncate()) {
        return false;
    }
    mCollectComp.Init(kkvConfig);
    mSkeyFieldType = GetSKeyDictKeyType(mKKVConfig->GetSuffixFieldConfig()->GetFieldType());
    return true;
}

uint32_t TruncateCollectInfoProcessor::Process(SKeyCollectInfos& skeyInfos, uint32_t validSkeyCount)
{
    // assert(validSkeyCount <= (uint32_t)skeyInfos.size());
    uint32_t flushCount = skeyInfos.size();
    if (validSkeyCount > mSKeyCountLimits) {
        uint32_t deleteInfoCount = skeyInfos.size() - validSkeyCount;
        flushCount = deleteInfoCount + mSKeyCountLimits;
        typename SKeyCollectInfos::iterator beginIter = skeyInfos.begin();
        typename SKeyCollectInfos::iterator endIter = skeyInfos.end();

        bool skipFirstNode = false;
        // if first skey node is delete pkey node, skip it
        if ((*beginIter)->isDeletedPKey) {
            skipFirstNode = true;
            beginIter++;
            flushCount--;
        }

        // get flushCount elements
        if (flushCount > 0 && beginIter + flushCount < endIter) {
            std::nth_element(beginIter, beginIter + flushCount - 1, endIter, mCollectComp);
        }
        beginIter = skipFirstNode ? skeyInfos.begin() + 1 : skeyInfos.begin();
        switch (mSkeyFieldType) {
#define MACRO(fieldType)                                                                                               \
    case fieldType: {                                                                                                  \
        CollectInfoKeyComp<typename FieldTypeTraits<fieldType>::AttrItemType> comp;                                    \
        sort(beginIter, beginIter + flushCount, comp);                                                                 \
        break;                                                                                                         \
    }

            NUMBER_FIELD_MACRO_HELPER(MACRO);
        default: {
            assert(false);
        }
#undef MACRO
        }

        if (skipFirstNode) {
            ++flushCount;
        }
    }
    return flushCount;
}
}} // namespace indexlib::index
