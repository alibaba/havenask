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

#include "autil/Log.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/attribute/config/PackAttributeConfig.h"
#include "indexlib/index/attribute/format/SingleValueAttributePatchFormatter.h"
#include "indexlib/index/attribute/patch/AttributeUpdater.h"
#include "indexlib/index/attribute/patch/AttributeUpdaterCreator.h"
#include "indexlib/index/common/field_format/attribute/TypeInfo.h"
#include "indexlib/util/HashMap.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueAttributeUpdater : public AttributeUpdater
{
public:
    SingleValueAttributeUpdater(segmentid_t segId, const std::shared_ptr<config::IIndexConfig>& attrConfig);

    ~SingleValueAttributeUpdater() = default;

public:
    class Creator : public AttributeUpdaterCreator
    {
    public:
        FieldType GetAttributeType() const override { return TypeInfo<T>::GetFieldType(); }

        std::unique_ptr<AttributeUpdater>
        Create(segmentid_t segId, const std::shared_ptr<config::IIndexConfig>& indexConfig) const override
        {
            return std::make_unique<SingleValueAttributeUpdater<T>>(segId, indexConfig);
        }
    };

public:
    void Update(docid_t docId, const autil::StringView& attributeValue, bool isNull) override;

    Status Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& attributeDir,
                segmentid_t srcSegment) override;

    void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater) override;

private:
    using AllocatorType = autil::mem_pool::pool_allocator<std::pair<const docid_t, T>>;
    using HashMap = std::unordered_map<docid_t, T, std::hash<docid_t>, std::equal_to<docid_t>, AllocatorType>;
    const static size_t MEMORY_USE_ESTIMATE_FACTOR = 2;
    HashMap _hashMap;
    bool _isSupportNull;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeUpdater, T);

template <typename T>
SingleValueAttributeUpdater<T>::SingleValueAttributeUpdater(segmentid_t segId,
                                                            const std::shared_ptr<config::IIndexConfig>& attrConfig)
    : AttributeUpdater(segId, attrConfig)
    , _hashMap(autil::mem_pool::pool_allocator<std::pair<const docid_t, T>>(&_simplePool))
{
    assert(_attrConfig);
    _isSupportNull = _attrConfig->GetFieldConfig()->IsEnableNullField();
}

template <typename T>
void SingleValueAttributeUpdater<T>::Update(docid_t docId, const autil::StringView& attributeValue, bool isNull)
{
    if (_isSupportNull && isNull) {
        assert(docId >= 0);
        // remove not null key
        _hashMap.erase(docId);
        SingleValueAttributePatchFormatter::EncodedDocId(docId);
        _hashMap[docId] = 0;
    } else {
        _hashMap[docId] = *(T*)attributeValue.data();
        if (_isSupportNull) {
            // remove null key
            SingleValueAttributePatchFormatter::EncodedDocId(docId);
            _hashMap.erase(docId);
        }
    }
}

template <typename T>
Status SingleValueAttributeUpdater<T>::Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& attributeDir,
                                            segmentid_t srcSegment)
{
    std::vector<docid_t> docIdVect;
    docIdVect.reserve(_hashMap.size());
    typename HashMap::iterator it = _hashMap.begin();
    typename HashMap::iterator itEnd = _hashMap.end();
    for (; it != itEnd; ++it) {
        docIdVect.push_back(it->first);
    }
    std::sort(docIdVect.begin(), docIdVect.end(), [](const docid_t& left, const docid_t& right) -> bool {
        return SingleValueAttributePatchFormatter::CompareDocId(left, right);
    });

    auto packAttrConfig = _attrConfig->GetPackAttributeConfig();
    std::string attrDir = packAttrConfig != NULL ? packAttrConfig->GetAttrName() + "/" + _attrConfig->GetAttrName()
                                                 : _attrConfig->GetAttrName();

    auto [status, dir] = attributeDir->MakeDirectory(attrDir, indexlib::file_system::DirectoryOption()).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "make attribute directory failed.");

    _patchFileName = ComputePatchFileName(srcSegment);
    _patchDirectory = dir->GetLogicalPath();
    auto [createFileWriterStatus, patchFileWriter] = CreatePatchFileWriter(dir, _patchFileName);
    RETURN_IF_STATUS_ERROR(createFileWriterStatus, "create patch file writer failed.");

    SingleValueAttributePatchFormatter formatter;
    formatter.InitForWrite(_attrConfig->GetFieldConfig()->IsEnableNullField(), patchFileWriter);

    size_t reserveSize = docIdVect.size() * (sizeof(T) + sizeof(docid_t));
    status = patchFileWriter->ReserveFile(reserveSize).Status();
    RETURN_IF_STATUS_ERROR(status, "write patch file info fail.");

    AUTIL_LOG(INFO, "Begin dumping attribute patch to file [%ld]: %s", reserveSize,
              patchFileWriter->DebugString().c_str());
    for (size_t i = 0; i < docIdVect.size(); ++i) {
        docid_t docId = docIdVect[i];
        T value = _hashMap[docId];
        status = formatter.Write(docId, (uint8_t*)&value, sizeof(T));
        RETURN_IF_STATUS_ERROR(status, "write patch file fail in SingleValueAttributePatchFormatter.");
    }

    assert(_attrConfig->GetCompressType().HasPatchCompress() || _isSupportNull ||
           patchFileWriter->GetLength() == reserveSize);
    status = formatter.Close();
    RETURN_IF_STATUS_ERROR(status, "close patch file fail in SingleValueAttributePatchFormatter.");
    AUTIL_LOG(INFO, "Finish dumping attribute patch to file : %s", patchFileWriter->DebugString().c_str());
    return Status::OK();
}

template <typename T>
void SingleValueAttributeUpdater<T>::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    int64_t poolSize = _simplePool.getUsedBytes();
    int64_t dumpTempMemUse = sizeof(docid_t) * _hashMap.size() + GetPatchFileWriterBufferSize();
    int64_t dumpFileSize = EstimateDumpFileSize(_hashMap.size() * sizeof(typename HashMap::value_type));

    memUpdater->UpdateCurrentMemUse(poolSize);
    memUpdater->EstimateDumpTmpMemUse(dumpTempMemUse);
    memUpdater->EstimateDumpedFileSize(dumpFileSize);
}

} // namespace indexlibv2::index
