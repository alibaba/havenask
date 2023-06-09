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
#include "indexlib/index/normal/attribute/accessor/attribute_reader_container.h"

#include "autil/TimeUtility.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/multi_pack_attribute_reader.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributeReaderContainer);

PackAttributeReaderPtr AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER = PackAttributeReaderPtr();
AttributeReaderPtr AttributeReaderContainer::RET_EMPTY_ATTR_READER = AttributeReaderPtr();
AttributeReaderContainerPtr AttributeReaderContainer::RET_EMPTY_ATTR_READER_CONTAINER = AttributeReaderContainerPtr();

AttributeReaderContainer::AttributeReaderContainer(const IndexPartitionSchemaPtr& schema) : mSchema(schema) {}

AttributeReaderContainer::~AttributeReaderContainer()
{
    mAttrReaders.reset();
    mVirtualAttrReaders.reset();
    mPackAttrReaders.reset();
}

void AttributeReaderContainer::Init(const PartitionDataPtr& partitionData, AttributeMetrics* attrMetrics,
                                    ReadPreference readPreference, bool needPackAttributeReaders,
                                    int32_t initReaderThreadCount, const AttributeReaderContainer* hintContainer)
{
    autil::ScopedTime2 timer1;
    IE_LOG(INFO, "InitAttributeReaders begin");

    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    if (attrSchema) {
        MultiFieldAttributeReader* hintReader = GET_IF_NOT_NULL(hintContainer, mAttrReaders.get());
        mAttrReaders.reset(
            new MultiFieldAttributeReader(attrSchema, attrMetrics, readPreference, initReaderThreadCount));
        mAttrReaders->Open(partitionData, hintReader);
    }

    const AttributeSchemaPtr& virtualAttrSchema = mSchema->GetVirtualAttributeSchema();
    if (virtualAttrSchema) {
        MultiFieldAttributeReader* hintReader = GET_IF_NOT_NULL(hintContainer, mVirtualAttrReaders.get());
        mVirtualAttrReaders.reset(
            new MultiFieldAttributeReader(virtualAttrSchema, attrMetrics, ReadPreference::RP_RANDOM_PREFER, 0));
        mVirtualAttrReaders->Open(partitionData, hintReader);
    }
    IE_LOG(INFO, "InitAttributeReaders end, used[%.3f]s", timer1.done_sec());

    if (!needPackAttributeReaders) {
        return;
    }

    autil::ScopedTime2 timer2;
    IE_LOG(INFO, "InitPackAttributeReaders begin");
    if (!attrSchema) {
        return;
    }
    MultiPackAttributeReader* hintPackReader = GET_IF_NOT_NULL(hintContainer, mPackAttrReaders.get());
    mPackAttrReaders.reset(new MultiPackAttributeReader(attrSchema, attrMetrics));
    mPackAttrReaders->Open(partitionData, hintPackReader);

    IE_LOG(INFO, "InitPackAttributeReaders end, used[%.3f]s", timer2.done_sec());
}

const AttributeReaderPtr& AttributeReaderContainer::GetAttributeReader(const string& field) const
{
    if (mAttrReaders) {
        const AttributeReaderPtr& attrReader = mAttrReaders->GetAttributeReader(field);
        if (attrReader) {
            return attrReader;
        }
    }

    if (mVirtualAttrReaders) {
        const AttributeReaderPtr& attrReader = mVirtualAttrReaders->GetAttributeReader(field);
        if (attrReader) {
            return attrReader;
        }
    }
    return RET_EMPTY_ATTR_READER;
}

const PackAttributeReaderPtr& AttributeReaderContainer::GetPackAttributeReader(const string& packAttrName) const
{
    if (mPackAttrReaders) {
        return mPackAttrReaders->GetPackAttributeReader(packAttrName);
    }
    return RET_EMPTY_PACK_ATTR_READER;
}

const PackAttributeReaderPtr& AttributeReaderContainer::GetPackAttributeReader(packattrid_t packAttrId) const
{
    if (mPackAttrReaders) {
        return mPackAttrReaders->GetPackAttributeReader(packAttrId);
    }
    return RET_EMPTY_PACK_ATTR_READER;
}

void AttributeReaderContainer::InitAttributeReader(const PartitionDataPtr& partitionData, ReadPreference readPreference,
                                                   const string& field, const AttributeReaderContainer* hintContainer)
{
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    if (!mAttrReaders) {
        mAttrReaders.reset(new MultiFieldAttributeReader(attrSchema, NULL, readPreference, 0));
    }
    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(field);
    mAttrReaders->InitAttributeReader(attrConfig, partitionData, GET_IF_NOT_NULL(hintContainer, mAttrReaders.get()));
}
}} // namespace indexlib::index
