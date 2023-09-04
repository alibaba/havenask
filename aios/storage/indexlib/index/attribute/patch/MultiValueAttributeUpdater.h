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

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/attribute/patch/AttributeUpdater.h"
#include "indexlib/index/attribute/patch/AttributeUpdaterCreator.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/TypeInfo.h"

namespace indexlibv2::index {

template <typename T>
class MultiValueAttributeUpdater : public AttributeUpdater
{
private:
    using AllocatorType = autil::mem_pool::pool_allocator<std::pair<const docid_t, std::string>>;
    using HashMap = std::unordered_map<docid_t, std::string, std::hash<docid_t>, std::equal_to<docid_t>, AllocatorType>;

public:
    MultiValueAttributeUpdater(segmentid_t segId, const std::shared_ptr<config::IIndexConfig>& attrConfig);
    ~MultiValueAttributeUpdater() = default;

public:
    class Creator : public AttributeUpdaterCreator
    {
    public:
        FieldType GetAttributeType() const override { return TypeInfo<T>::GetFieldType(); }

        std::unique_ptr<AttributeUpdater>
        Create(segmentid_t segId, const std::shared_ptr<config::IIndexConfig>& indexConfig) const override
        {
            return std::make_unique<MultiValueAttributeUpdater<T>>(segId, indexConfig);
        }
    };

public:
    void Update(docid_t docId, const autil::StringView& attributeValue, bool isNull) override;

    Status Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& attributeDir, segmentid_t segmentId) override;

    void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater) override;

private:
    const static size_t MEMORY_USE_ESTIMATE_FACTOR = 2;
    size_t _dumpValueSize;
    HashMap _hashMap;
    std::string _nullValue;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, MultiValueAttributeUpdater, T);

template <typename T>
MultiValueAttributeUpdater<T>::MultiValueAttributeUpdater(segmentid_t segId,
                                                          const std::shared_ptr<config::IIndexConfig>& indexConfig)
    : AttributeUpdater(segId, indexConfig)
    , _dumpValueSize(0)
    , _hashMap(AllocatorType(&_simplePool))
{
    // TODO(xiuchen) add open function for _nullValue's initialize?
    MultiValueAttributeConvertor<T> convertor;
    auto [st, nullValue] = convertor.EncodeNullValue();
    assert(st.IsOK());
    _nullValue = nullValue;
}

template <typename T>
void MultiValueAttributeUpdater<T>::Update(docid_t docId, const autil::StringView& attributeValue, bool isNull)
{
    std::string value;
    if (isNull) {
        MultiValueAttributeConvertor<T> convertor;
        AttrValueMeta meta = convertor.Decode(autil::StringView(_nullValue));
        value = std::string(meta.data.data(), meta.data.size());
    } else {
        value = std::string(attributeValue.data(), attributeValue.size());
    }

    std::pair<HashMap::iterator, bool> ret = _hashMap.insert(std::make_pair(docId, value));
    if (!ret.second) {
        HashMap::iterator& iter = ret.first;
        _dumpValueSize -= iter->second.size();
        iter->second = value;
    }

    _dumpValueSize += value.size();
}

template <typename T>
Status MultiValueAttributeUpdater<T>::Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& attributeDir,
                                           segmentid_t srcSegment)
{
    std::vector<docid_t> docIdVect;
    if (_hashMap.empty()) {
        return Status::OK();
    }

    docIdVect.reserve(_hashMap.size());
    typename HashMap::iterator it = _hashMap.begin();
    typename HashMap::iterator itEnd = _hashMap.end();
    for (; it != itEnd; ++it) {
        docIdVect.push_back(it->first);
    }
    std::sort(docIdVect.begin(), docIdVect.end());

    // auto packAttrConfig = _attrConfig->GetPackAttributeConfig();
    // std::string attrDir = packAttrConfig != NULL ? packAttrConfig->GetPackName() + "/" + _attrConfig->GetAttrName()
    //                                              : _attrConfig->GetAttrName();
    std::string attrDir = _attrConfig->GetAttrName();

    auto [status, dir] = attributeDir->MakeDirectory(attrDir, indexlib::file_system::DirectoryOption()).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "make attribute directory failed.");

    _patchFileName = ComputePatchFileName(srcSegment);
    _patchDirectory = dir->GetLogicalPath();

    auto [createFileWriterStatus, patchFileWriter] = CreatePatchFileWriter(dir, _patchFileName);
    RETURN_IF_STATUS_ERROR(createFileWriterStatus, "create patch file writer failed.");

    size_t reserveSize =
        _dumpValueSize + docIdVect.size() * sizeof(docid_t) + sizeof(uint32_t) * 2; // patchCount + maxPatchDataLen
    status = patchFileWriter->ReserveFile(reserveSize).Status();
    RETURN_IF_STATUS_ERROR(status, "write patch file info fail.");

    AUTIL_LOG(DEBUG, "Begin dumping attribute patch to file : %s", patchFileWriter->DebugString().c_str());

    uint32_t maxPatchDataLen = 0;
    uint32_t patchDataCount = _hashMap.size();
    for (size_t i = 0; i < docIdVect.size(); ++i) {
        docid_t docId = docIdVect[i];
        std::string str = _hashMap[docId];
        status = patchFileWriter->Write(&docId, sizeof(docid_t)).Status();
        RETURN_IF_STATUS_ERROR(status, "write docid into patch file fail in MultiValueAttributePatchFile.");
        status = patchFileWriter->Write(str.c_str(), str.size()).Status();
        RETURN_IF_STATUS_ERROR(status, "write value into patch file fail in MultiValueAttributePatchFile.");
        maxPatchDataLen = std::max(maxPatchDataLen, (uint32_t)str.size());
    }
    status = patchFileWriter->Write(&patchDataCount, sizeof(uint32_t)).Status();
    RETURN_IF_STATUS_ERROR(status, "write patch data count into patch file fail in MultiValueAttributePatchFile.");
    status = patchFileWriter->Write(&maxPatchDataLen, sizeof(uint32_t)).Status();
    RETURN_IF_STATUS_ERROR(status, "write max patch data length into patch file fail in MultiValueAttributePatchFile.");

    assert(_attrConfig->GetCompressType().HasPatchCompress() || patchFileWriter->GetLength() == reserveSize);
    status = patchFileWriter->Close().Status();
    RETURN_IF_STATUS_ERROR(status, "close patch file fail in MultiValueAttributePatchFile.");
    AUTIL_LOG(DEBUG, "Finish dumping attribute patch to file : %s", patchFileWriter->DebugString().c_str());
    return Status::OK();
}

template <typename T>
void MultiValueAttributeUpdater<T>::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    int64_t totalMemUse = _simplePool.getUsedBytes() + _dumpValueSize;
    int64_t docIdMemSize = _hashMap.size() * sizeof(docid_t);
    int64_t dumpTempMemUse = docIdMemSize + GetPatchFileWriterBufferSize();
    int64_t dumpFileSize = EstimateDumpFileSize(docIdMemSize + _dumpValueSize);

    memUpdater->UpdateCurrentMemUse(totalMemUse);
    memUpdater->EstimateDumpTmpMemUse(dumpTempMemUse);
    memUpdater->EstimateDumpedFileSize(dumpFileSize);
}

} // namespace indexlibv2::index
