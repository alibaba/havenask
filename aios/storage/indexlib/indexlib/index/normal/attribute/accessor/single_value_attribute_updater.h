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
#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_UPDATER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_UPDATER_H

#include <memory>
#include <unordered_map>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/common_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater_creator.h"
#include "indexlib/index/normal/attribute/format/single_value_attribute_patch_formatter.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/HashMap.h"

namespace indexlib { namespace index {

template <typename T>
class SingleValueAttributeUpdater : public AttributeUpdater
{
public:
    SingleValueAttributeUpdater(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                                const config::AttributeConfigPtr& attrConfig);

    ~SingleValueAttributeUpdater() {}

public:
    class Creator : public AttributeUpdaterCreator
    {
    public:
        FieldType GetAttributeType() const { return common::TypeInfo<T>::GetFieldType(); }

        AttributeUpdater* Create(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                                 const config::AttributeConfigPtr& attrConfig) const
        {
            return new SingleValueAttributeUpdater<T>(buildResourceMetrics, segId, attrConfig);
        }
    };

public:
    void Update(docid_t docId, const autil::StringView& attributeValue, bool isNull = false) override;

    void Dump(const file_system::DirectoryPtr& attributeDir, segmentid_t srcSegment = INVALID_SEGMENTID) override;

private:
    void UpdateBuildResourceMetrics()
    {
        if (!mBuildResourceMetricsNode) {
            return;
        }
        int64_t poolSize = mSimplePool.getUsedBytes();
        int64_t dumpTempMemUse = sizeof(docid_t) * mHashMap.size() + GetPatchFileWriterBufferSize();

        int64_t dumpFileSize = EstimateDumpFileSize(mHashMap.size() * sizeof(typename HashMap::value_type));

        mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
        mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempMemUse);
        mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
    }

private:
    typedef autil::mem_pool::pool_allocator<std::pair<const docid_t, T>> AllocatorType;
    typedef std::unordered_map<docid_t, T, std::hash<docid_t>, std::equal_to<docid_t>, AllocatorType> HashMap;
    const static size_t MEMORY_USE_ESTIMATE_FACTOR = 2;
    HashMap mHashMap;
    bool mIsSupportNull;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributeUpdater);

template <typename T>
SingleValueAttributeUpdater<T>::SingleValueAttributeUpdater(util::BuildResourceMetrics* buildResourceMetrics,
                                                            segmentid_t segId,
                                                            const config::AttributeConfigPtr& attrConfig)
    : AttributeUpdater(buildResourceMetrics, segId, attrConfig)
    , mHashMap(autil::mem_pool::pool_allocator<std::pair<const docid_t, T>>(&mSimplePool))
{
    mIsSupportNull = attrConfig->GetFieldConfig()->IsEnableNullField();
}

template <typename T>
void SingleValueAttributeUpdater<T>::Update(docid_t docId, const autil::StringView& attributeValue, bool isNull)
{
    size_t lastCount = mHashMap.size();
    if (mIsSupportNull && isNull) {
        assert(docId >= 0);
        // remove not null key
        mHashMap.erase(docId);
        SingleValueAttributePatchFormatter::EncodedDocId(docId);
        mHashMap[docId] = 0;
    } else {
        mHashMap[docId] = *(T*)attributeValue.data();
        if (mIsSupportNull) {
            // remove null key
            SingleValueAttributePatchFormatter::EncodedDocId(docId);
            mHashMap.erase(docId);
        }
    }

    if (mHashMap.size() > lastCount) {
        UpdateBuildResourceMetrics();
    }
}

template <typename T>
void SingleValueAttributeUpdater<T>::Dump(const file_system::DirectoryPtr& attributeDir, segmentid_t srcSegment)
{
    std::vector<docid_t> docIdVect;
    docIdVect.reserve(mHashMap.size());
    typename HashMap::iterator it = mHashMap.begin();
    typename HashMap::iterator itEnd = mHashMap.end();
    for (; it != itEnd; ++it) {
        docIdVect.push_back(it->first);
    }
    std::sort(docIdVect.begin(), docIdVect.end(), [](const docid_t& left, const docid_t& right) -> bool {
        return SingleValueAttributePatchFormatter::CompareDocId(left, right);
    });

    config::PackAttributeConfig* packAttrConfig = mAttrConfig->GetPackAttributeConfig();
    std::string attrDir = packAttrConfig != NULL ? packAttrConfig->GetPackName() + "/" + mAttrConfig->GetAttrName()
                                                 : mAttrConfig->GetAttrName();

    file_system::DirectoryPtr dir = attributeDir->MakeDirectory(attrDir);

    std::string patchFileName = GetPatchFileName(srcSegment);
    file_system::FileWriterPtr patchFileWriter = CreatePatchFileWriter(dir, patchFileName);
    SingleValueAttributePatchFormatter formatter;
    formatter.InitForWrite(mAttrConfig->GetFieldConfig()->IsEnableNullField(), patchFileWriter);

    size_t reserveSize = docIdVect.size() * (sizeof(T) + sizeof(docid_t));
    patchFileWriter->ReserveFile(reserveSize).GetOrThrow();

    IE_LOG(INFO, "Begin dumping attribute patch to file [%ld]: %s", reserveSize,
           patchFileWriter->DebugString().c_str());
    for (size_t i = 0; i < docIdVect.size(); ++i) {
        docid_t docId = docIdVect[i];
        T value = mHashMap[docId];
        formatter.Write(docId, (uint8_t*)&value, sizeof(T));
    }

    assert(mAttrConfig->GetCompressType().HasPatchCompress() || mIsSupportNull ||
           patchFileWriter->GetLength() == reserveSize);
    formatter.Close();
    IE_LOG(INFO, "Finish dumping attribute patch to file : %s", patchFileWriter->DebugString().c_str());
}

typedef SingleValueAttributeUpdater<int8_t> Int8AttributeUpdater;
typedef SingleValueAttributeUpdater<uint8_t> UInt8AttributeUpdater;

typedef SingleValueAttributeUpdater<int16_t> Int16AttributeUpdater;
typedef SingleValueAttributeUpdater<uint16_t> UInt16AttributeUpdater;

typedef SingleValueAttributeUpdater<int32_t> Int32AttributeUpdater;
typedef SingleValueAttributeUpdater<uint32_t> UInt32AttributeUpdater;

typedef SingleValueAttributeUpdater<int64_t> Int64AttributeUpdater;
typedef SingleValueAttributeUpdater<uint64_t> UInt64AttributeUpdater;

typedef SingleValueAttributeUpdater<float> FloatAttributeUpdater;
typedef SingleValueAttributeUpdater<double> DoubleAttributeUpdater;
}} // namespace indexlib::index

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_UPDATER_H
