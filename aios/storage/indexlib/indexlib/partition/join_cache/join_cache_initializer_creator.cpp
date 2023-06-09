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
#include "indexlib/partition/join_cache/join_cache_initializer_creator.h"

#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer_creator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/partition/join_cache/join_cache_initializer.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::common;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, JoinCacheInitializerCreator);

JoinCacheInitializerCreator::JoinCacheInitializerCreator() {}

JoinCacheInitializerCreator::~JoinCacheInitializerCreator() {}

bool JoinCacheInitializerCreator::Init(const string& joinFieldName, bool isSubField, IndexPartitionSchemaPtr schema,
                                       IndexPartition* auxPart, versionid_t auxReaderIncVersionId)
{
    // mAuxPartition = auxPartition;
    // IndexPartitionSchemaPtr schema = mainPartition->GetSchema();
    if (isSubField) {
        const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
        if (!subSchema) {
            return false;
        }
        schema = subSchema;
    }

    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    if (attrSchema) {
        mJoinFieldAttrConfig = attrSchema->GetAttributeConfig(joinFieldName);
    }
    if (!mJoinFieldAttrConfig) {
        IE_LOG(ERROR, "joinField [%s] not exist in attribute!", joinFieldName.c_str());
        return false;
    }
    mAuxPart = auxPart;
    mAuxReaderIncVersionId = auxReaderIncVersionId;
    return true;
}

AttributeValueInitializerPtr JoinCacheInitializerCreator::Create(const PartitionDataPtr& partitionData)
{
    if (!mJoinFieldAttrConfig) {
        IE_LOG(ERROR, "joinField not in attribute!");
        return AttributeValueInitializerPtr();
    }

    AttributeReaderPtr attrReader = AttributeReaderFactory::CreateAttributeReader(
        mJoinFieldAttrConfig, partitionData, config::ReadPreference::RP_RANDOM_PREFER, nullptr, nullptr);
    if (!attrReader) {
        IE_LOG(ERROR, "create attribute reader for joinField [%s] fail!", mJoinFieldAttrConfig->GetAttrName().c_str());
        return AttributeValueInitializerPtr();
    }

    JoinCacheInitializerPtr initializer(new JoinCacheInitializer);
    initializer->Init(attrReader, mAuxPart->GetReader(mAuxReaderIncVersionId));
    return initializer;
}

AttributeValueInitializerPtr JoinCacheInitializerCreator::Create(const InMemorySegmentReaderPtr& inMemSegmentReader)
{
    if (!mJoinFieldAttrConfig) {
        IE_LOG(ERROR, "joinField not in attribute!");
        return AttributeValueInitializerPtr();
    }

    FieldConfigPtr fieldConfig(new FieldConfig("join_docid_cache", ft_int32, false, true, false));
    docid_t defaultValue = INVALID_DOCID;
    string defaultJoinValueStr = StringUtil::toString(defaultValue);
    fieldConfig->SetDefaultValue(defaultJoinValueStr);
    AttributeConfigPtr attrConfig(new AttributeConfig());
    attrConfig->Init(fieldConfig);

    DefaultAttributeValueInitializerCreator creator(attrConfig);
    return creator.Create(inMemSegmentReader);
}
}} // namespace indexlib::partition
