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
#include "indexlib/index/normal/attribute/accessor/attribute_merger_factory.h"

#include <sstream>

#include "indexlib/index/normal/attribute/accessor/date_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/docid_join_value_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/float_attribute_merger_creator.h"
#include "indexlib/index/normal/attribute/accessor/line_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/location_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/polygon_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/time_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/timestamp_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/uniq_encoded_pack_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger_creator.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::common;
using namespace indexlib::config;

namespace indexlib { namespace index {

AttributeMergerFactory::AttributeMergerFactory() { Init(); }

AttributeMergerFactory::~AttributeMergerFactory() {}

void AttributeMergerFactory::Init()
{
    RegisterCreator(AttributeMergerCreatorPtr(new Int16AttributeMerger::Creator()));
    RegisterCreator(AttributeMergerCreatorPtr(new UInt16AttributeMerger::Creator()));
    RegisterCreator(AttributeMergerCreatorPtr(new Int8AttributeMerger::Creator()));
    RegisterCreator(AttributeMergerCreatorPtr(new UInt8AttributeMerger::Creator()));
    RegisterCreator(AttributeMergerCreatorPtr(new Int32AttributeMerger::Creator()));
    RegisterCreator(AttributeMergerCreatorPtr(new UInt32AttributeMerger::Creator()));
    RegisterCreator(AttributeMergerCreatorPtr(new Int64AttributeMerger::Creator()));
    RegisterCreator(AttributeMergerCreatorPtr(new UInt64AttributeMerger::Creator()));
    RegisterCreator(AttributeMergerCreatorPtr(new Hash64AttributeMerger::Creator()));
    RegisterCreator(AttributeMergerCreatorPtr(new Hash128AttributeMerger::Creator()));
    RegisterCreator(AttributeMergerCreatorPtr(new FloatAttributeMerger::Creator()));
    RegisterCreator(AttributeMergerCreatorPtr(new FloatFp16AttributeMergerCreator()));
    RegisterCreator(AttributeMergerCreatorPtr(new FloatInt8AttributeMergerCreator()));
    RegisterCreator(AttributeMergerCreatorPtr(new DoubleAttributeMerger::Creator()));
    RegisterCreator(AttributeMergerCreatorPtr(new StringAttributeMergerCreator()));
    RegisterCreator(AttributeMergerCreatorPtr(new TimeAttributeMergerCreator()));
    RegisterCreator(AttributeMergerCreatorPtr(new TimestampAttributeMergerCreator()));
    RegisterCreator(AttributeMergerCreatorPtr(new DateAttributeMergerCreator()));

    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new VarNumAttributeMergerCreator<int8_t>()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new VarNumAttributeMergerCreator<uint8_t>()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new VarNumAttributeMergerCreator<int16_t>()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new VarNumAttributeMergerCreator<uint16_t>()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new VarNumAttributeMergerCreator<int32_t>()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new VarNumAttributeMergerCreator<uint32_t>()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new VarNumAttributeMergerCreator<int64_t>()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new VarNumAttributeMergerCreator<uint64_t>()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new VarNumAttributeMergerCreator<float>()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new VarNumAttributeMergerCreator<double>()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new VarNumAttributeMergerCreator<MultiChar>()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new LocationAttributeMergerCreator()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new LineAttributeMergerCreator()));
    RegisterMultiValueCreator(AttributeMergerCreatorPtr(new PolygonAttributeMergerCreator()));
}

void AttributeMergerFactory::RegisterCreator(const AttributeMergerCreatorPtr& creator)
{
    assert(creator != NULL);

    autil::ScopedLock l(mLock);
    FieldType type = creator->GetAttributeType();
    CreatorMap::iterator it = mMergerCreators.find(type);
    if (it != mMergerCreators.end()) {
        mMergerCreators.erase(it);
    }
    mMergerCreators.insert(make_pair(type, creator));
}

void AttributeMergerFactory::RegisterMultiValueCreator(const AttributeMergerCreatorPtr& creator)
{
    assert(creator != NULL);

    autil::ScopedLock l(mLock);
    FieldType type = creator->GetAttributeType();
    CreatorMap::iterator it = mMultiValueMergerCreators.find(type);
    if (it != mMultiValueMergerCreators.end()) {
        mMultiValueMergerCreators.erase(it);
    }
    mMultiValueMergerCreators.insert(make_pair(type, creator));
}

AttributeMerger*
AttributeMergerFactory::CreatePackAttributeMerger(const PackAttributeConfigPtr& packAttrConfig, bool needMergePatch,
                                                  size_t patchBufferSize, const index_base::MergeItemHint& hint,
                                                  const index_base::MergeTaskResourceVector& taskResources)
{
    assert(packAttrConfig);
    autil::ScopedLock l(mLock);
    CompressTypeOption compressType = packAttrConfig->GetCompressType();
    if (compressType.HasUniqEncodeCompress()) {
        UniqEncodedPackAttributeMerger* uniqMerger =
            new UniqEncodedPackAttributeMerger(needMergePatch, patchBufferSize);
        uniqMerger->Init(packAttrConfig, hint, taskResources);
        return uniqMerger;
    }
    PackAttributeMerger* merger = new PackAttributeMerger(needMergePatch);
    merger->Init(packAttrConfig, hint, taskResources);
    return merger;
}

AttributeMerger* AttributeMergerFactory::CreateAttributeMerger(const AttributeConfigPtr& attrConfig,
                                                               bool needMergePatch,
                                                               const index_base::MergeItemHint& hint,
                                                               const index_base::MergeTaskResourceVector& taskResources)
{
    autil::ScopedLock l(mLock);

    FieldType fieldType = attrConfig->GetFieldType();
    const FieldConfigPtr& fieldConfig = attrConfig->GetFieldConfig();
    bool isMultiValue = fieldConfig->IsMultiValue();

    if (IsDocIdJoinAttribute(attrConfig)) {
        AttributeMerger* merger = new DocidJoinValueAttributeMerger();
        merger->Init(attrConfig, hint, taskResources);
        return merger;
    }

    AttributeMerger* merger;
    if (!isMultiValue) {
        CreatorMap::const_iterator it = mMergerCreators.find(fieldType);
        if (it != mMergerCreators.end()) {
            merger = it->second->Create(attrConfig->IsUniqEncode(), attrConfig->IsAttributeUpdatable(), needMergePatch);
        } else {
            INDEXLIB_THROW(util::UnImplementException,
                           "Attribute Merger type: [%d] "
                           "not implemented yet!",
                           (int)fieldType);
        }
    } else {
        CreatorMap::const_iterator it = mMultiValueMergerCreators.find(fieldType);
        if (it != mMultiValueMergerCreators.end()) {
            merger = it->second->Create(attrConfig->IsUniqEncode(), attrConfig->IsAttributeUpdatable(), needMergePatch);
        } else {
            INDEXLIB_THROW(util::UnImplementException,
                           "Updatable MultiValueAttribute "
                           "Merger type [%d] not implemented yet.",
                           fieldType);
        }
    }
    assert(merger);
    merger->Init(attrConfig, hint, taskResources);
    return merger;
}

bool AttributeMergerFactory::IsDocIdJoinAttribute(const AttributeConfigPtr& attrConfig)
{
    const string& fieldName = attrConfig->GetAttrName();
    return fieldName == MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME || fieldName == SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME;
}

typedef SingleValueAttributeMerger<int64_t> Int64AttributeMerger;
DEFINE_SHARED_PTR(Int64AttributeMerger);

typedef SingleValueAttributeMerger<uint64_t> UInt64AttributeMerger;
DEFINE_SHARED_PTR(UInt64AttributeMerger);

typedef SingleValueAttributeMerger<int32_t> Int32AttributeMerger;
DEFINE_SHARED_PTR(Int32AttributeMerger);

typedef SingleValueAttributeMerger<uint32_t> UInt32AttributeMerger;
DEFINE_SHARED_PTR(UInt32AttributeMerger);

typedef SingleValueAttributeMerger<int16_t> Int16AttributeMerger;
DEFINE_SHARED_PTR(Int16AttributeMerger);

typedef SingleValueAttributeMerger<uint16_t> UInt16AttributeMerger;
DEFINE_SHARED_PTR(UInt16AttributeMerger);

typedef SingleValueAttributeMerger<int8_t> Int8AttributeMerger;
DEFINE_SHARED_PTR(Int8AttributeMerger);

typedef SingleValueAttributeMerger<uint8_t> UInt8AttributeMerger;
DEFINE_SHARED_PTR(UInt8AttributeMerger);

typedef SingleValueAttributeMerger<float> FloatAttributeMerger;
DEFINE_SHARED_PTR(FloatAttributeMerger);

typedef SingleValueAttributeMerger<double> DoubleAttributeMerger;
DEFINE_SHARED_PTR(DoubleAttributeMerger);
}} // namespace indexlib::index
