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
#include "indexlib/index/normal/attribute/accessor/attribute_modifier.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"

using namespace std;
using namespace autil::mem_pool;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::common;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributeModifier);

AttributeModifier::AttributeModifier(const IndexPartitionSchemaPtr& schema,
                                     util::BuildResourceMetrics* buildResourceMetrics)
    : mSchema(schema)
    , mBuildResourceMetrics(buildResourceMetrics)
{
    InitAttributeConvertorMap(schema);
}

AttributeModifier::~AttributeModifier() { mPackUpdateBitmapVec.clear(); }

void AttributeModifier::InitAttributeConvertorMap(const IndexPartitionSchemaPtr& schema)
{
    size_t fieldCount = schema->GetFieldCount();
    mFieldId2ConvertorMap.resize(fieldCount);
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    if (!attrSchema) {
        return;
    }

    AttributeSchema::Iterator iter = attrSchema->Begin();
    for (; iter != attrSchema->End(); iter++) {
        const auto& attrConfig = *iter;
        AttributeConvertorPtr attrConvertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
        mFieldId2ConvertorMap[attrConfig->GetFieldId()] = attrConvertor;
    }
}

void AttributeModifier::InitPackAttributeUpdateBitmap(const PartitionDataPtr& partitionData)
{
    assert(partitionData);
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema) {
        return;
    }

    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    for (; packIter != packAttrConfigs->End(); packIter++) {
        AttributeUpdateBitmapPtr packAttrUpdateBitmap;
        packAttrUpdateBitmap.reset(new AttributeUpdateBitmap());
        packAttrUpdateBitmap->Init(partitionData, mBuildResourceMetrics);
        mPackUpdateBitmapVec.push_back(packAttrUpdateBitmap);
    }
}

void AttributeModifier::DumpPackAttributeUpdateInfo(const DirectoryPtr& attrDirectory) const
{
    DumpPackAttributeUpdateInfo(attrDirectory, mSchema, mPackUpdateBitmapVec);
}

void AttributeModifier::DumpPackAttributeUpdateInfo(const file_system::DirectoryPtr& attrDirectory,
                                                    const config::IndexPartitionSchemaPtr& schema,
                                                    const PackAttrUpdateBitmapVec& packUpdateBitmapVec)
{
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    if (!attrSchema) {
        return;
    }

    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    for (size_t i = 0; packIter != packAttrConfigs->End(); ++packIter, ++i) {
        const PackAttributeConfigPtr& packAttrConfig = *packIter;
        const AttributeUpdateBitmapPtr& packAttrUpdateBitmap = packUpdateBitmapVec[i];
        const string& packName = packAttrConfig->GetAttrName();
        DirectoryPtr packAttrDir = attrDirectory->MakeDirectory(packName);
        packAttrUpdateBitmap->Dump(packAttrDir);
    }
}
}} // namespace indexlib::index
