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
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"

#include "indexlib/config/attribute_config.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_patch_iterator_creator.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/uniq_encode_var_num_attribute_segment_reader_for_offline.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/util/class_typed_factory.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, OfflineAttributeSegmentReaderContainer);

AttributeSegmentReaderPtr OfflineAttributeSegmentReaderContainer::NULL_READER = AttributeSegmentReaderPtr();

OfflineAttributeSegmentReaderContainer::OfflineAttributeSegmentReaderContainer(
    const config::IndexPartitionSchemaPtr& schema, const SegmentDirectoryBasePtr& segDir)
    : mSchema(schema)
    , mSegDir(segDir)
{
}

OfflineAttributeSegmentReaderContainer::~OfflineAttributeSegmentReaderContainer() {}

AttributeSegmentReaderPtr
OfflineAttributeSegmentReaderContainer::CreateOfflineSegmentReader(const AttributeConfigPtr& attrConfig,
                                                                   const SegmentData& segData)
{
    auto fieldType = attrConfig->GetFieldType();
    if (attrConfig->IsMultiValue()) {
        assert(false);
        return AttributeSegmentReaderPtr();
    } else if (fieldType == ft_string) {
        if (attrConfig->IsUniqEncode()) {
            UniqEncodeVarNumAttributeSegmentReaderForOffline<char>* attrReader =
                new UniqEncodeVarNumAttributeSegmentReaderForOffline<char>(attrConfig);
            attrReader->Open(mSegDir->GetPartitionData(), *segData.GetSegmentInfo(), segData.GetSegmentId());
            AttributeSegmentReaderPtr ret(attrReader);
            return ret;
        } else {
            AttributeSegmentPatchIteratorPtr patchIterator(new VarNumAttributePatchReader<char>(attrConfig));
            patchIterator->Init(mSegDir->GetPartitionData(), segData.GetSegmentId());
            MultiValueAttributeSegmentReader<char>* attrReader = new MultiValueAttributeSegmentReader<char>(attrConfig);
            attrReader->Open(segData, PatchApplyOption::OnRead(patchIterator), nullptr, nullptr, true);
            AttributeSegmentReaderPtr ret(attrReader);
            return ret;
        }
    } else {
        auto attrReader = ClassTypedFactory<AttributeSegmentReader, SingleValueAttributeSegmentReader,
                                            const AttributeConfigPtr&>::GetInstance()
                              ->Create(fieldType, attrConfig);
        AttributeSegmentReaderPtr ret(attrReader);
        PatchApplyOption patchOption;
        patchOption.applyStrategy = PAS_APPLY_ON_READ;
        AttributeSegmentPatchIteratorPtr patchIterator(AttributeSegmentPatchIteratorCreator::Create(attrConfig));
        patchIterator->Init(mSegDir->GetPartitionData(), segData.GetSegmentId());
        patchOption.patchReader = patchIterator;
        switch (attrConfig->GetFieldConfig()->GetFieldType()) {
#define MACRO(type)                                                                                                    \
    case type: {                                                                                                       \
        typedef indexlib::config::FieldTypeTraits<type>::AttrItemType ActualType;                                      \
        auto typedReader = DYNAMIC_POINTER_CAST(SingleValueAttributeSegmentReader<ActualType>, ret);                   \
        typedReader->Open(segData, patchOption, nullptr, true);                                                        \
        assert(typedReader->GetDataBaseAddr() == nullptr);                                                             \
        if (typedReader->GetDataBaseAddr()) {                                                                          \
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "expected open in block cache, but get base address!");               \
        }                                                                                                              \
        break;                                                                                                         \
    }

            NUMBER_FIELD_MACRO_HELPER(MACRO)
#undef MACRO

        default:
            assert(false);
        }
        return ret;
    }
} // namespace index

void OfflineAttributeSegmentReaderContainer::Reset()
{
    autil::ScopedLock lock(mMapMutex);
    mReaderMap.clear();
}

const AttributeSegmentReaderPtr&
OfflineAttributeSegmentReaderContainer::GetAttributeSegmentReader(const std::string& attrName, segmentid_t segId)
{
    autil::ScopedLock lock(mMapMutex);
    const auto& attrSchema = mSchema->GetAttributeSchema();
    const auto& attrConfig = attrSchema->GetAttributeConfig(attrName);
    if (!attrConfig) {
        IE_LOG(ERROR, "attribute config [%s] not found in schema", attrName.c_str());
        return NULL_READER;
    }
    const auto& partData = mSegDir->GetPartitionData();
    auto segData = partData->GetSegmentData(segId);

    auto attrMapIter = mReaderMap.find(attrName);
    if (attrMapIter == mReaderMap.end()) {
        attrMapIter = mReaderMap.insert(attrMapIter, {attrName, SegmentReaderMap()});
    }
    auto& segReaderMap = attrMapIter->second;
    auto readerIter = segReaderMap.find(segId);
    if (readerIter == segReaderMap.end()) {
        auto reader = CreateOfflineSegmentReader(attrConfig, segData);
        readerIter = segReaderMap.insert(readerIter, {segId, reader});
    }
    return readerIter->second;
}
}} // namespace indexlib::index
