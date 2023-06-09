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
#include "indexlib/index/kv/kv_doc_timestamp_decider.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index_define.h"

using namespace std;

using namespace indexlib::config;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, KvDocTimestampDecider);

KvDocTimestampDecider::KvDocTimestampDecider() {}

KvDocTimestampDecider::~KvDocTimestampDecider() {}

void KvDocTimestampDecider::Init(const IndexPartitionOptions& option) { mOption = option; }

// TODO(xinfei.sxf) int64(doc timestamp) -> uint32_t(return value type) leads to same error in doc timestamp < 0 case
uint32_t KvDocTimestampDecider::GetTimestamp(document::KVIndexDocument* doc)
{
    if (mOption.GetBuildConfig(false).buildPhase == BuildConfig::BP_FULL &&
        mOption.GetBuildConfig(false).useUserTimestamp == true && doc->GetUserTimestamp() != INVALID_TIMESTAMP) {
        return MicrosecondToSecond(doc->GetUserTimestamp());
    } else {
        return MicrosecondToSecond(doc->GetTimestamp());
    }
}
}} // namespace indexlib::index
