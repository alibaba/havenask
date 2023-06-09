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
#include "indexlib/index/kv/kv_segment_reader.h"

#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"

using namespace std;
using namespace indexlib::codegen;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KVSegmentReader);

void KVSegmentReader::Open(const config::KVIndexConfigPtr& kvIndexConfig, const index_base::SegmentData& segmentData)
{
    mSegmentId = segmentData.GetSegmentId();
    bool isOnlineSegment = index_base::RealtimeSegmentDirectory::IsRtSegmentId(mSegmentId);
    if (IsVarLenSegment(kvIndexConfig)) {
        bool valueCompress = kvIndexConfig->GetIndexPreference().GetValueParam().EnableFileCompress();
        if (valueCompress && !isOnlineSegment) {
            mReader.reset((KVSegmentReaderBase*)new HashTableCompressVarSegmentReaderType);
            mActualReaderType = "HashTableCompressVarSegmentReaderType";
        } else {
            mReader.reset((KVSegmentReaderBase*)new HashTableVarSegmentReaderType);
            mActualReaderType = "HashTableVarSegmentReaderType";
        }
    } else {
        mReader.reset((KVSegmentReaderBase*)new HashTableFixSegmentReaderType);
        mActualReaderType = "HashTableFixSegmentReaderType";
    }
    mReader->Open(kvIndexConfig, segmentData);
}

bool KVSegmentReader::IsVarLenSegment(const config::KVIndexConfigPtr& kvConfig)
{
    if (kvConfig->GetRegionCount() > 1) {
        return true;
    }
    auto& valueConfig = kvConfig->GetValueConfig();
    if (kvConfig->GetIndexPreference().GetValueParam().IsFixLenAutoInline()) {
        int32_t valueSize = valueConfig->GetFixedLength();
        if (valueSize > 8 || valueSize < 0) {
            return true;
        }
        return false;
    }
    if (valueConfig->GetAttributeCount() > 1) {
        return true;
    }
    config::AttributeConfigPtr attrConfig = valueConfig->GetAttributeConfig(0);
    FieldType fieldType = attrConfig->GetFieldType();
    return attrConfig->IsMultiValue() || fieldType == ft_string;
}

bool KVSegmentReader::doCollectAllCode()
{
    INIT_CODEGEN_INFO(KVSegmentReader, true);
    mReader->collectAllCode();
    COLLECT_TYPE_REDEFINE(KVSegmentReaderBase, mReader->getTargetClassName());
    if (mActualReaderType == "HashTableCompressVarSegmentReaderType") {
        COLLECT_TYPE_REDEFINE(HashTableCompressVarSegmentReaderType, mReader->getTargetClassName());
    }
    if (mActualReaderType == "HashTableVarSegmentReaderType") {
        COLLECT_TYPE_REDEFINE(HashTableVarSegmentReaderType, mReader->getTargetClassName());
    }
    if (mActualReaderType == "HashTableFixSegmentReaderType") {
        COLLECT_TYPE_REDEFINE(HashTableFixSegmentReaderType, mReader->getTargetClassName());
    }
    combineCodegenInfos(mReader.get());
    return true;
}

void KVSegmentReader::TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id)
{
    CodegenCheckerPtr checker(new CodegenChecker);
    COLLECT_TYPE_DEFINE(checker, KVSegmentReaderBase);
    COLLECT_TYPE_DEFINE(checker, HashTableCompressVarSegmentReaderType);
    COLLECT_TYPE_DEFINE(checker, HashTableVarSegmentReaderType);
    COLLECT_TYPE_DEFINE(checker, HashTableFixSegmentReaderType);
    checkers[string("KVSegmentReader") + id] = checker;
    mReader->TEST_collectCodegenResult(checkers, id);
}
}} // namespace indexlib::index
