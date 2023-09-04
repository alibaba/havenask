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
#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_UPDATER_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_UPDATER_H

#include <memory>
#include <unordered_map>

#include "autil/StringUtil.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater_creator.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T>
class VarNumAttributeUpdater : public AttributeUpdater
{
private:
    typedef autil::mem_pool::pool_allocator<std::pair<const docid_t, std::string>> AllocatorType;
    typedef std::unordered_map<docid_t, std::string, std::hash<docid_t>, std::equal_to<docid_t>, AllocatorType> HashMap;

public:
    VarNumAttributeUpdater(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                           const config::AttributeConfigPtr& attrConfig);
    ~VarNumAttributeUpdater() {}

public:
    class Creator : public AttributeUpdaterCreator
    {
    public:
        FieldType GetAttributeType() const { return common::TypeInfo<T>::GetFieldType(); }

        AttributeUpdater* Create(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                                 const config::AttributeConfigPtr& attrConfig) const
        {
            return new VarNumAttributeUpdater<T>(buildResourceMetrics, segId, attrConfig);
        }
    };

public:
    void Update(docid_t docId, const autil::StringView& attributeValue, bool isNull = false) override;

    void Dump(const file_system::DirectoryPtr& attributeDir, segmentid_t segmentId = INVALID_SEGMENTID) override;

private:
    void UpdateBuildResourceMetrics()
    {
        if (!mBuildResourceMetricsNode) {
            return;
        }
        int64_t totalMemUse = mSimplePool.getUsedBytes() + mDumpValueSize;
        int64_t docIdMemSize = mHashMap.size() * sizeof(docid_t);
        int64_t dumpTempMemUse = docIdMemSize + GetPatchFileWriterBufferSize();
        int64_t dumpFileSize = EstimateDumpFileSize(docIdMemSize + mDumpValueSize);

        mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, totalMemUse);
        mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempMemUse);
        mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
    }

private:
    const static size_t MEMORY_USE_ESTIMATE_FACTOR = 2;
    size_t mDumpValueSize;
    HashMap mHashMap;
    std::string mNullValue;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, VarNumAttributeUpdater);

template <typename T>
VarNumAttributeUpdater<T>::VarNumAttributeUpdater(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                                                  const config::AttributeConfigPtr& attrConfig)
    : AttributeUpdater(buildResourceMetrics, segId, attrConfig)
    , mDumpValueSize(0)
    , mHashMap(AllocatorType(&mSimplePool))
{
    common::VarNumAttributeConvertor<T> convertor;
    mNullValue = convertor.EncodeNullValue();
}

template <typename T>
void VarNumAttributeUpdater<T>::Update(docid_t docId, const autil::StringView& attributeValue, bool isNull)
{
    std::string value;
    if (isNull) {
        common::VarNumAttributeConvertor<T> convertor;
        common::AttrValueMeta meta = convertor.Decode(autil::StringView(mNullValue));
        value = std::string(meta.data.data(), meta.data.size());
    } else {
        value = std::string(attributeValue.data(), attributeValue.size());
    }

    std::pair<HashMap::iterator, bool> ret = mHashMap.insert(std::make_pair(docId, value));
    if (!ret.second) {
        HashMap::iterator& iter = ret.first;
        mDumpValueSize -= iter->second.size();
        iter->second = value;
    }

    mDumpValueSize += value.size();
    UpdateBuildResourceMetrics();
}

template <typename T>
void VarNumAttributeUpdater<T>::Dump(const file_system::DirectoryPtr& attributeDir, segmentid_t srcSegment)
{
    std::vector<docid_t> docIdVect;
    if (mHashMap.empty()) {
        return;
    }

    docIdVect.reserve(mHashMap.size());
    typename HashMap::iterator it = mHashMap.begin();
    typename HashMap::iterator itEnd = mHashMap.end();
    for (; it != itEnd; ++it) {
        docIdVect.push_back(it->first);
    }
    std::sort(docIdVect.begin(), docIdVect.end());

    config::PackAttributeConfig* packAttrConfig = mAttrConfig->GetPackAttributeConfig();
    std::string attrDir = packAttrConfig != NULL ? packAttrConfig->GetPackName() + "/" + mAttrConfig->GetAttrName()
                                                 : mAttrConfig->GetAttrName();

    file_system::DirectoryPtr dir = attributeDir->MakeDirectory(attrDir);

    std::string patchFileName = GetPatchFileName(srcSegment);
    file_system::FileWriterPtr patchFileWriter = CreatePatchFileWriter(dir, patchFileName);

    size_t reserveSize =
        mDumpValueSize + docIdVect.size() * sizeof(docid_t) + sizeof(uint32_t) * 2; // patchCount + maxPatchDataLen
    patchFileWriter->ReserveFile(reserveSize).GetOrThrow();

    IE_LOG(DEBUG, "Begin dumping attribute patch to file : %s", patchFileWriter->DebugString().c_str());

    uint32_t maxPatchDataLen = 0;
    uint32_t patchDataCount = mHashMap.size();
    for (size_t i = 0; i < docIdVect.size(); ++i) {
        docid_t docId = docIdVect[i];
        std::string str = mHashMap[docId];
        patchFileWriter->Write(&docId, sizeof(docid_t)).GetOrThrow();
        patchFileWriter->Write(str.c_str(), str.size()).GetOrThrow();
        maxPatchDataLen = std::max(maxPatchDataLen, (uint32_t)str.size());
    }
    patchFileWriter->Write(&patchDataCount, sizeof(uint32_t)).GetOrThrow();
    patchFileWriter->Write(&maxPatchDataLen, sizeof(uint32_t)).GetOrThrow();

    assert(mAttrConfig->GetCompressType().HasPatchCompress() || patchFileWriter->GetLength() == reserveSize);
    patchFileWriter->Close().GetOrThrow();
    IE_LOG(DEBUG, "Finish dumping attribute patch to file : %s", patchFileWriter->DebugString().c_str());
}

typedef VarNumAttributeUpdater<int8_t> Int8MultiValueAttributeUpdater;
typedef VarNumAttributeUpdater<uint8_t> UInt8MultiValueAttributeUpdater;
typedef VarNumAttributeUpdater<int16_t> Int16MultiValueAttributeUpdater;
typedef VarNumAttributeUpdater<uint16_t> UInt16MultiValueAttributeUpdater;
typedef VarNumAttributeUpdater<int32_t> Int32MultiValueAttributeUpdater;
typedef VarNumAttributeUpdater<uint32_t> UInt32MultiValueAttributeUpdater;
typedef VarNumAttributeUpdater<int64_t> Int64MultiValueAttributeUpdater;
typedef VarNumAttributeUpdater<uint64_t> UInt64MultiValueAttributeUpdater;
typedef VarNumAttributeUpdater<float> FloatMultiValueAttributeUpdater;
typedef VarNumAttributeUpdater<double> DoubleMultiValueAttributeUpdater;
typedef VarNumAttributeUpdater<autil::MultiChar> MultiStringAttributeUpdater;
}} // namespace indexlib::index

#endif //__INDEXLIB_VAR_NUM_ATTRIBUTE_UPDATER_H
