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
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"

#include <sstream>

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_creator.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::config;

namespace indexlib { namespace index {

AttributeReaderFactory::AttributeReaderFactory() { Init(); }

AttributeReaderFactory::~AttributeReaderFactory() {}

extern void RegisterMultiValueInt8(AttributeReaderFactory* factory);
extern void RegisterMultiValueUInt8(AttributeReaderFactory* factory);
extern void RegisterMultiValueInt16(AttributeReaderFactory* factory);
extern void RegisterMultiValueUInt16(AttributeReaderFactory* factory);
extern void RegisterMultiValueInt32(AttributeReaderFactory* factory);
extern void RegisterMultiValueUInt32(AttributeReaderFactory* factory);
extern void RegisterMultiValueInt64(AttributeReaderFactory* factory);
extern void RegisterMultiValueUInt64(AttributeReaderFactory* factory);
extern void RegisterMultiValueFloat(AttributeReaderFactory* factory);
extern void RegisterMultiValueDouble(AttributeReaderFactory* factory);
extern void RegisterMultiValueMultiString(AttributeReaderFactory* factory);

extern void RegisterSingleValueInt8(AttributeReaderFactory* factory);
extern void RegisterSingleValueUInt8(AttributeReaderFactory* factory);
extern void RegisterSingleValueInt16(AttributeReaderFactory* factory);
extern void RegisterSingleValueUInt16(AttributeReaderFactory* factory);
extern void RegisterSingleValueInt32(AttributeReaderFactory* factory);
extern void RegisterSingleValueUInt32(AttributeReaderFactory* factory);
extern void RegisterSingleValueInt64(AttributeReaderFactory* factory);
extern void RegisterSingleValueUInt64(AttributeReaderFactory* factory);
extern void RegisterSingleValueFloat(AttributeReaderFactory* factory);
extern void RegisterSingleValueDouble(AttributeReaderFactory* factory);
extern void RegisterSingleValueUInt128(AttributeReaderFactory* factory);

extern void RegisterSpecialMultiValueLine(AttributeReaderFactory* factory);
extern void RegisterSpecialMultiValuePolygon(AttributeReaderFactory* factory);
extern void RegisterSpecialMultiValueLocation(AttributeReaderFactory* factory);

extern void RegisterSpecialSingleValueTime(AttributeReaderFactory* factory);
extern void RegisterSpecialSingleValueTimestamp(AttributeReaderFactory* factory);
extern void RegisterSpecialSingleValueDate(AttributeReaderFactory* factory);
extern void RegisterAttributeString(AttributeReaderFactory* factory);

void AttributeReaderFactory::Init()
{
    RegisterMultiValueInt8(this);
    RegisterMultiValueUInt8(this);
    RegisterMultiValueInt16(this);
    RegisterMultiValueUInt16(this);
    RegisterMultiValueInt32(this);
    RegisterMultiValueUInt32(this);
    RegisterMultiValueInt64(this);
    RegisterMultiValueUInt64(this);
    RegisterMultiValueFloat(this);
    RegisterMultiValueDouble(this);
    RegisterMultiValueMultiString(this);

    RegisterSingleValueInt8(this);
    RegisterSingleValueUInt8(this);
    RegisterSingleValueInt16(this);
    RegisterSingleValueUInt16(this);
    RegisterSingleValueInt32(this);
    RegisterSingleValueUInt32(this);
    RegisterSingleValueInt64(this);
    RegisterSingleValueUInt64(this);
    RegisterSingleValueFloat(this);
    RegisterSingleValueDouble(this);
    RegisterSingleValueUInt128(this);

    RegisterSpecialMultiValueLine(this);
    RegisterSpecialMultiValuePolygon(this);
    RegisterSpecialMultiValueLocation(this);

    RegisterSpecialSingleValueTime(this);
    RegisterSpecialSingleValueTimestamp(this);
    RegisterSpecialSingleValueDate(this);

    RegisterAttributeString(this);
}

void AttributeReaderFactory::RegisterCreator(AttributeReaderCreatorPtr creator)
{
    assert(creator != NULL);

    ScopedLock l(mLock);
    FieldType type = creator->GetAttributeType();
    CreatorMap::iterator it = mReaderCreators.find(type);
    if (it != mReaderCreators.end()) {
        mReaderCreators.erase(it);
    }
    mReaderCreators.insert(make_pair(type, creator));
}

void AttributeReaderFactory::RegisterMultiValueCreator(AttributeReaderCreatorPtr creator)
{
    assert(creator != NULL);

    ScopedLock l(mLock);
    FieldType type = creator->GetAttributeType();
    CreatorMap::iterator it = mMultiValueReaderCreators.find(type);
    if (it != mMultiValueReaderCreators.end()) {
        mMultiValueReaderCreators.erase(it);
    }
    mMultiValueReaderCreators.insert(make_pair(type, creator));
}

AttributeReader* AttributeReaderFactory::CreateJoinDocidAttributeReader() { return new JoinDocidAttributeReader; }

AttributeReader* AttributeReaderFactory::CreateAttributeReader(const FieldConfigPtr& fieldConfig,
                                                               config::ReadPreference readPreference,
                                                               AttributeMetrics* metrics)
{
    autil::ScopedLock l(mLock);
    bool isMultiValue = fieldConfig->IsMultiValue();
    FieldType fieldType = fieldConfig->GetFieldType();
    const string& fieldName = fieldConfig->GetFieldName();
    if (!isMultiValue) {
        if (fieldName == MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME || fieldName == SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME) {
            return CreateJoinDocidAttributeReader();
        }

        CreatorMap::const_iterator it = mReaderCreators.find(fieldType);
        if (it != mReaderCreators.end()) {
            return it->second->Create(metrics);
        } else {
            stringstream s;
            s << "Attribute Reader type: " << fieldType << " not implemented yet!";
            INDEXLIB_THROW(util::UnImplementException, "%s", s.str().c_str());
        }
    } else {
        CreatorMap::const_iterator it = mMultiValueReaderCreators.find(fieldType);
        if (it != mMultiValueReaderCreators.end()) {
            return it->second->Create(metrics);
        } else {
            stringstream s;
            s << "MultiValueAttribute Reader type: " << fieldType << " not implemented yet!";
            INDEXLIB_THROW(util::UnImplementException, "%s", s.str().c_str());
        }
    }
    return NULL;
}

AttributeReaderPtr AttributeReaderFactory::CreateAttributeReader(const AttributeConfigPtr& attrConfig,
                                                                 const PartitionDataPtr& partitionData,
                                                                 config::ReadPreference readPreference,
                                                                 AttributeMetrics* metrics,
                                                                 const AttributeReader* hintReader)
{
    AttributeReaderPtr attrReader(AttributeReaderFactory::GetInstance()->CreateAttributeReader(
        attrConfig->GetFieldConfig(), readPreference, metrics));
    if (!attrReader ||
        !attrReader->Open(attrConfig, partitionData, ReadPreferenceToPatchApplyStrategy(readPreference), hintReader)) {
        return AttributeReaderPtr();
    }
    return attrReader;
}

JoinDocidAttributeReaderPtr AttributeReaderFactory::CreateJoinAttributeReader(const IndexPartitionSchemaPtr& schema,
                                                                              const string& attrName,
                                                                              const PartitionDataPtr& partitionData)
{
    PartitionDataPtr subPartitionData = partitionData->GetSubPartitionData();
    assert(subPartitionData);
    if (attrName == MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME) {
        return CreateJoinAttributeReader(schema, attrName, partitionData, subPartitionData);
    } else if (attrName == SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME) {
        IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
        return CreateJoinAttributeReader(subSchema, attrName, subPartitionData, partitionData);
    }
    assert(false);
    return JoinDocidAttributeReaderPtr();
}

JoinDocidAttributeReaderPtr AttributeReaderFactory::CreateJoinAttributeReader(const IndexPartitionSchemaPtr& schema,
                                                                              const string& attrName,
                                                                              const PartitionDataPtr& partitionData,
                                                                              const PartitionDataPtr& joinPartitionData)
{
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    assert(attrSchema);

    AttributeConfigPtr attrConfig;
    attrConfig = attrSchema->GetAttributeConfig(attrName);
    assert(attrConfig);

    JoinDocidAttributeReaderPtr attrReader(new JoinDocidAttributeReader);
    attrReader->Open(attrConfig, partitionData, PatchApplyStrategy::PAS_APPLY_NO_PATCH);
    attrReader->InitJoinBaseDocId(joinPartitionData);
    return attrReader;
}

PatchApplyStrategy AttributeReaderFactory::ReadPreferenceToPatchApplyStrategy(config::ReadPreference readPreference)
{
    switch (readPreference) {
    case ReadPreference::RP_RANDOM_PREFER:
        return PatchApplyStrategy::PAS_APPLY_NO_PATCH;
    case ReadPreference::RP_SEQUENCE_PREFER:
        return PatchApplyStrategy::PAS_APPLY_ON_READ;
    default:
        assert(false);
    }
    return PAS_APPLY_UNKNOW;
}
}} // namespace indexlib::index
