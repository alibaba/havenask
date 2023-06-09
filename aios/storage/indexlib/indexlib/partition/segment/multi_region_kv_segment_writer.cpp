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
#include "indexlib/partition/segment/multi_region_kv_segment_writer.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/document/document_parser/kv_parser/multi_region_kv_key_extractor.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index_base/segment/building_segment_data.h"

using namespace std;
using namespace autil;

using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, MultiRegionKVSegmentWriter);

MultiRegionKVSegmentWriter::MultiRegionKVSegmentWriter(const IndexPartitionSchemaPtr& schema,
                                                       const IndexPartitionOptions& options, uint32_t columnIdx,
                                                       uint32_t totalColumnCount)
    : KVSegmentWriter(schema, options, columnIdx, totalColumnCount)
{
}

MultiRegionKVSegmentWriter::~MultiRegionKVSegmentWriter() {}

bool MultiRegionKVSegmentWriter::ExtractDocInfo(document::KVIndexDocument* doc, keytype_t& key, StringView& value,
                                                bool& isDeleted)
{
    if (!mKeyExtractor->GetKey(doc, key)) {
        return false;
    }
    if (!GetDeletedDocFlag(doc, isDeleted)) {
        return false;
    }
    if (isDeleted) {
        return true;
    }

    StringView fieldValue = doc->GetValue();
    if (fieldValue.empty()) {
        IE_LOG(ERROR, "empty pack field value, pkHash[%lu]", key);
        ERROR_COLLECTOR_LOG(ERROR, "empty pack field value, pkHash[%lu]", key);
        return false;
    }

    const AttrValueMeta& meta = mAttrConvertor->Decode(fieldValue);
    if (mFixValueLens[doc->GetRegionId()] != -1) {
        StringView tempData = meta.data;
        size_t headerSize = VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*tempData.data());
        value = StringView(tempData.data() + headerSize, tempData.size() - headerSize);
    } else {
        assert(!mPlainFormatEncoder);
        value = meta.data;
    }
    return true;
}

void MultiRegionKVSegmentWriter::InitConfig()
{
    assert(mSchema->GetRegionCount() > 1);
    mKVConfig = CreateKVIndexConfigForMultiRegionData(mSchema);
    PackAttributeConfigPtr packAttrConfig = mKVConfig->GetValueConfig()->CreatePackAttributeConfig();
    assert(packAttrConfig);
    mAttrConvertor.reset(AttributeConvertorFactory::GetInstance()->CreatePackAttrConvertor(packAttrConfig));
    mKeyExtractor.reset(new MultiRegionKVKeyExtractor(mSchema));

    mFixValueLens.clear();
    mFixValueLens.resize(mSchema->GetRegionCount());
    for (regionid_t id = 0; id < (regionid_t)mSchema->GetRegionCount(); id++) {
        const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema(id);
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        assert(kvConfig);
        mFixValueLens[id] = kvConfig->GetValueConfig()->GetFixedLength();
    }
}

MultiRegionKVSegmentWriter* MultiRegionKVSegmentWriter::CloneWithNewSegmentData(BuildingSegmentData& segmentData,
                                                                                bool isShared) const
{
    assert(isShared);
    MultiRegionKVSegmentWriter* newSegmentWriter = new MultiRegionKVSegmentWriter(*this);
    segmentData.PrepareDirectory();
    newSegmentWriter->mSegmentData = segmentData;
    return newSegmentWriter;
}
}} // namespace indexlib::partition
