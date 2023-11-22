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
#pragma once

#include <memory>
#include <unordered_map>

#include "autil/DataBuffer.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/attribute_field_printer.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_creator.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_var_num_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/index_base/index_meta/temperature_doc_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T>
class VarNumAttributeReader : public AttributeReader
{
private:
    using AttributeReader::Read;

public:
    typedef MultiValueAttributeSegmentReader<T> SegmentReader;
    typedef std::shared_ptr<SegmentReader> SegmentReaderPtr;
    typedef typename MultiValueAttributeSegmentReader<T>::ReadContext ReadContext;
    typedef InMemVarNumAttributeReader<T> InMemSegmentReader;
    typedef std::shared_ptr<InMemSegmentReader> InMemSegmentReaderPtr;

    typedef AttributeIteratorTyped<autil::MultiValueType<T>, AttributeReaderTraits<autil::MultiValueType<T>>>
        AttributeIterator;
    typedef std::shared_ptr<AttributeIterator> AttributeIteratorPtr;

    typedef BuildingAttributeReader<autil::MultiValueType<T>, AttributeReaderTraits<autil::MultiValueType<T>>>
        BuildingAttributeReaderType;
    typedef std::shared_ptr<BuildingAttributeReaderType> BuildingAttributeReaderPtr;

public:
    class Creator : public AttributeReaderCreator
    {
    public:
        FieldType GetAttributeType() const { return common::TypeInfo<T>::GetFieldType(); }

        AttributeReader* Create(AttributeMetrics* metrics = NULL) const
        {
            return new VarNumAttributeReader<T>(metrics);
        }
    };

public:
    VarNumAttributeReader(AttributeMetrics* metrics = NULL)
        : AttributeReader(metrics)
        , mBuildingBaseDocId(INVALID_DOCID)
        , mUpdatble(false)
        , mIsIntegrated(true)
    {
    }

    ~VarNumAttributeReader() {}

    DECLARE_ATTRIBUTE_READER_IDENTIFIER(var_num);

public:
    bool Open(const config::AttributeConfigPtr& attrConfig, const index_base::PartitionDataPtr& partitionData,
              PatchApplyStrategy patchApplyStrategy, const AttributeReader* hintReader = nullptr) override;

    bool Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool = NULL) const override;

    AttrType GetType() const override;
    bool IsMultiValue() const override;
    AttributeIterator* CreateIterator(autil::mem_pool::Pool* pool) const override;

    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull = false) override;

    bool Updatable() const override { return mUpdatble; }

    bool Read(docid_t docId, autil::MultiValueType<T>& value, autil::mem_pool::Pool* pool = NULL) const;

    virtual bool GetSortedDocIdRange(const table::DimensionDescription::Range& range, const DocIdRange& rangeLimit,
                                     DocIdRange& resultRange) const override
    {
        return false;
    }

    std::string GetAttributeName() const override { return mAttrConfig->GetAttrName(); }

    AttributeSegmentReaderPtr GetSegmentReader(docid_t docId) const override;

    bool IsIntegrated() const { return mIsIntegrated; }

    void EnableAccessCountors() override;

    void EnableGlobalReadContext() override;

public:
    // for test
    const std::vector<SegmentReaderPtr>& GetSegmentReaders() const { return mSegmentReaders; }

protected:
    virtual void InitBuildingAttributeReader(const index_base::SegmentIteratorPtr& segIter);

protected:
    std::vector<SegmentReaderPtr> mSegmentReaders;
    BuildingAttributeReaderPtr mBuildingAttributeReader;
    common::AttributeFieldPrinter mFieldPrinter;

    std::vector<uint64_t> mSegmentDocCount;
    config::AttributeConfigPtr mAttrConfig;

    std::vector<segmentid_t> mReaderSegmentIds;
    docid_t mBuildingBaseDocId;
    bool mUpdatble;
    bool mIsIntegrated;

    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, VarNumAttributeReader);

///////////////////////////////////////////////////
template <typename T>
bool VarNumAttributeReader<T>::Open(const config::AttributeConfigPtr& attrConfig,
                                    const index_base::PartitionDataPtr& partitionData,
                                    PatchApplyStrategy patchApplyStrategy, const AttributeReader* hintReader)
{
    // dup with single value attribute reader
    assert(attrConfig);
    mAttrConfig = attrConfig;
    mUpdatble = true;
    mFieldPrinter.Init(attrConfig->GetFieldConfig());
    mTemperatureDocInfo = partitionData->GetTemperatureDocInfo();

    std::string attrName = GetAttributeName();
    IE_LOG(DEBUG, "Start opening attribute(%s).", attrName.c_str());

    index_base::PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    index_base::SegmentIteratorPtr builtSegIter = segIter->CreateIterator(index_base::SIT_BUILT);
    assert(builtSegIter);
    std::unordered_map<segmentid_t, SegmentReader*> hintReaderMap;
    if (hintReader) {
        auto typedHintReader = dynamic_cast<const VarNumAttributeReader*>(hintReader);
        for (size_t i = 0; i < typedHintReader->mReaderSegmentIds.size(); ++i) {
            hintReaderMap[typedHintReader->mReaderSegmentIds[i]] = typedHintReader->mSegmentReaders[i].get();
        }
    }
    while (builtSegIter->IsValid()) {
        const index_base::SegmentData& segData = builtSegIter->GetSegmentData();
        index_base::SegmentInfo segmentInfo = *segData.GetSegmentInfo();
        if (segmentInfo.docCount == 0) {
            builtSegIter->MoveToNext();
            continue;
        }

        try {
            TemperatureProperty property = UNKNOWN;
            if (mAttributeMetrics && mTemperatureDocInfo && !mTemperatureDocInfo->IsEmptyInfo()) {
                property = mTemperatureDocInfo->GetTemperaturePropery(segData.GetSegmentId());
            }
            SegmentReaderPtr segReader(new SegmentReader(mAttrConfig, mAttributeMetrics, property));
            SegmentReader* hintSegmentReader = nullptr;
            file_system::DirectoryPtr attrDirectory = GetAttributeDirectory(segData, mAttrConfig);
            auto iter = hintReaderMap.find(segData.GetSegmentId());
            if (iter != hintReaderMap.end()) {
                hintSegmentReader = iter->second;
            }
            PatchApplyOption patchOption;
            patchOption.applyStrategy = patchApplyStrategy;
            if (patchOption.applyStrategy == PatchApplyStrategy::PAS_APPLY_ON_READ) {
                AttributeSegmentPatchIteratorPtr patchIterator(new VarNumAttributePatchReader<T>(mAttrConfig));
                patchIterator->Init(partitionData, segData.GetSegmentId());
                patchOption.patchReader = patchIterator;
            }
            segReader->Open(segData, patchOption, attrDirectory, hintSegmentReader, /* disableUpdate*/ false);
            if (!segReader->Updatable()) {
                mUpdatble = false;
            }

            mIsIntegrated = mIsIntegrated && (segReader->GetBaseAddress() != nullptr);
            mSegmentReaders.push_back(segReader);
            mSegmentDocCount.push_back((uint64_t)segmentInfo.docCount);
            mReaderSegmentIds.push_back(segData.GetSegmentId());
            builtSegIter->MoveToNext();
        } catch (const util::NonExistException& e) {
            if (mAttrConfig->GetConfigType() == config::AttributeConfig::ct_index_accompany) {
                IE_LOG(WARN, "index accompany attribute [%s] open fail: [%s]", mAttrConfig->GetAttrName().c_str(),
                       e.what());
                return false;
            }
            throw;
        } catch (const util::ExceptionBase& e) {
            IE_LOG(ERROR, "Load segment FAILED, segment id: [%d], reason: [%s].", segData.GetSegmentId(), e.what());
            throw;
        }
    }

    mBuildingBaseDocId = segIter->GetBuildingBaseDocId();
    index_base::SegmentIteratorPtr buildingSegIter = segIter->CreateIterator(index_base::SIT_BUILDING);
    InitBuildingAttributeReader(buildingSegIter);
    IE_LOG(DEBUG, "Finish opening attribute(%s).", attrName.c_str());
    return true;
}

template <typename T>
void VarNumAttributeReader<T>::EnableAccessCountors()
{
    mEnableAccessCountors = true;
    for (size_t i = 0; i < mSegmentDocCount.size(); i++) {
        mSegmentReaders[i]->EnableAccessCountors();
    }
}

template <typename T>
void VarNumAttributeReader<T>::EnableGlobalReadContext()
{
    for (size_t i = 0; i < mSegmentDocCount.size(); i++) {
        mSegmentReaders[i]->EnableGlobalReadContext();
    }
}

template <typename T>
AttrType VarNumAttributeReader<T>::GetType() const
{
    return common::TypeInfo<T>::GetAttrType();
}

template <typename T>
inline bool VarNumAttributeReader<T>::IsMultiValue() const
{
    return true;
}

template <>
inline bool VarNumAttributeReader<char>::IsMultiValue() const
{
    // string is regard as single value
    return false;
}

template <typename T>
bool VarNumAttributeReader<T>::Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<T> value;
    bool ret = Read(docId, value, pool);
    if (!ret) {
        IE_LOG(ERROR, "read value from docId [%d] fail!", docId);
        return false;
    }
    return mFieldPrinter.Print(value.isNull(), value, &attrValue);
}

template <typename T>
inline bool VarNumAttributeReader<T>::Read(docid_t docId, autil::MultiValueType<T>& value,
                                           autil::mem_pool::Pool* pool) const
{
    docid_t baseDocId = 0;
    bool isNull = false;
    for (size_t i = 0; i < mSegmentDocCount.size(); ++i) {
        const uint64_t docCount = mSegmentDocCount[i];
        if (docId < baseDocId + (docid_t)docCount) {
            ReadContext* globalCtx = (ReadContext*)mSegmentReaders[i]->GetGlobalReadContext();
            if (globalCtx) {
                return mSegmentReaders[i]->Read(docId - baseDocId, value, isNull, *globalCtx);
            }
            auto ctx = mSegmentReaders[i]->CreateReadContext(pool);
            return mSegmentReaders[i]->Read(docId - baseDocId, value, isNull, ctx);
        }
        baseDocId += docCount;
    }

    size_t buildingSegIdx = 0;
    return mBuildingAttributeReader && mBuildingAttributeReader->Read(docId, value, buildingSegIdx, isNull, pool);
}

template <typename T>
typename VarNumAttributeReader<T>::AttributeIterator*
VarNumAttributeReader<T>::CreateIterator(autil::mem_pool::Pool* pool) const
{
    AttributeIterator* attrIt =
        IE_POOL_COMPATIBLE_NEW_CLASS(pool, AttributeIterator, mSegmentReaders, mBuildingAttributeReader,
                                     mSegmentDocCount, mBuildingBaseDocId, &mFieldPrinter, pool);
    return attrIt;
}

template <typename T>
bool VarNumAttributeReader<T>::UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull)
{
    if (!mAttrConfig->IsAttributeUpdatable()) {
        IE_LOG(ERROR, "attribute [%s] is not updatable!", mAttrConfig->GetAttrName().c_str());
        return false;
    }
    if (!mUpdatble) {
        return false;
    }
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentDocCount.size(); i++) {
        uint64_t docCount = mSegmentDocCount[i];
        if (docId < baseDocId + (docid_t)docCount) {
            return mSegmentReaders[i]->UpdateField(docId - baseDocId, buf, bufLen, isNull);
        }
        baseDocId += docCount;
    }

    if (mBuildingAttributeReader) {
        // TODO: mBuildingSegmentReader support updateField
        return mBuildingAttributeReader->UpdateField(docId, buf, bufLen, isNull);
    }
    return false;
}

template <typename T>
AttributeSegmentReaderPtr VarNumAttributeReader<T>::GetSegmentReader(docid_t docId) const
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentDocCount.size(); i++) {
        uint64_t docCount = mSegmentDocCount[i];
        if (docId < baseDocId + (docid_t)docCount) {
            return mSegmentReaders[i];
        }
        baseDocId += docCount;
    }
    return AttributeSegmentReaderPtr();
}

template <typename T>
inline void VarNumAttributeReader<T>::InitBuildingAttributeReader(const index_base::SegmentIteratorPtr& buildingIter)
{
    if (!buildingIter) {
        return;
    }
    while (buildingIter->IsValid()) {
        index_base::InMemorySegmentPtr inMemorySegment = buildingIter->GetInMemSegment();
        if (!inMemorySegment) {
            buildingIter->MoveToNext();
            continue;
        }
        index_base::InMemorySegmentReaderPtr segmentReader = inMemorySegment->GetSegmentReader();
        if (!segmentReader) {
            buildingIter->MoveToNext();
            continue;
        }
        AttributeSegmentReaderPtr attrSegReader = segmentReader->GetAttributeSegmentReader(GetAttributeName());
        assert(attrSegReader);

        InMemSegmentReaderPtr inMemSegReader = DYNAMIC_POINTER_CAST(InMemSegmentReader, attrSegReader);
        if (!mBuildingAttributeReader) {
            mBuildingAttributeReader.reset(new BuildingAttributeReaderType);
        }
        mBuildingAttributeReader->AddSegmentReader(buildingIter->GetBaseDocId(), inMemSegReader);
        buildingIter->MoveToNext();
    }
}

////////////////////////////////////////////////////////////
typedef VarNumAttributeReader<char> StringAttributeReader;
DEFINE_SHARED_PTR(StringAttributeReader);
}} // namespace indexlib::index
