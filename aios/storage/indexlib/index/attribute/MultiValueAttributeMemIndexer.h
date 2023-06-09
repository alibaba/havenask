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

#include <functional>
#include <memory>
#include <unordered_map>

#include "indexlib/base/Types.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/DocMapDumpParams.h"
#include "indexlib/index/attribute/AttributeDataInfo.h"
#include "indexlib/index/attribute/AttributeMemIndexer.h"
#include "indexlib/index/attribute/AttributeMemIndexerCreator.h"
#include "indexlib/index/attribute/MultiValueAttributeMemReader.h"
#include "indexlib/index/common/FileCompressParamHelper.h"
#include "indexlib/index/common/data_structure/AdaptiveAttributeOffsetDumper.h"
#include "indexlib/index/common/data_structure/AttributeCompressInfo.h"
#include "indexlib/index/common/data_structure/EqualValueCompressDumper.h"
#include "indexlib/index/common/data_structure/VarLenDataDumper.h"
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/TypeInfo.h"
#include "indexlib/util/HashMap.h"
#include "indexlib/util/HashString.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2::index {

template <typename T>
class MultiValueAttributeMemIndexer : public AttributeMemIndexer
{
public:
    MultiValueAttributeMemIndexer(const IndexerParameter& indexerParam)
        : AttributeMemIndexer(indexerParam)
        , _accessor(nullptr)
    {
    }

    ~MultiValueAttributeMemIndexer()
    {
        if (_accessor) {
            _accessor->VarLenDataAccessor::~VarLenDataAccessor();
        }
    }

public:
    class Creator : public AttributeMemIndexerCreator
    {
    public:
        FieldType GetAttributeType() const override { return TypeInfo<T>::GetFieldType(); }

        std::unique_ptr<AttributeMemIndexer> Create(const IndexerParameter& indexerParam) const override
        {
            return std::make_unique<MultiValueAttributeMemIndexer<T>>(indexerParam);
        }
    };

public:
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Dump(autil::mem_pool::PoolBase* dumpPool,
                const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                const std::shared_ptr<framework::DumpParams>& dumpParams) override;
    void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater) override;
    const std::shared_ptr<AttributeMemReader> CreateInMemReader() const override;

    void AddField(docid_t docId, const autil::StringView& attributeValue, bool isNull) override;
    bool UpdateField(docid_t docId, const autil::StringView& attributeValue, bool isNull,
                     const uint64_t* hashKey) override;
    uint64_t InitDumpEstimateFactor() const;
    bool IsDirty() const override;

private:
    VarLenDataAccessor* _accessor;
    std::string _nullValue;
    uint64_t _dumpEstimateFactor;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, MultiValueAttributeMemIndexer, T);

/////////////////////////////////////////////////////////
template <typename T>
inline Status
MultiValueAttributeMemIndexer<T>::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                       document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    Status status = AttributeMemIndexer::Init(indexConfig, docInfoExtractorFactory);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "init MultiValueAttributeMemIndexer fail, error: [%s]", status.ToString().c_str());
        return status;
    }

    assert(_pool);
    char* buffer = (char*)_pool->allocate(sizeof(VarLenDataAccessor));
    _accessor = new (buffer) VarLenDataAccessor();
    _accessor->Init(_pool.get(), _attrConfig->IsUniqEncode());
    _dumpEstimateFactor = InitDumpEstimateFactor();
    MultiValueAttributeConvertor<T> convertor;
    std::tie(status, _nullValue) = convertor.EncodeNullValue();
    RETURN_IF_STATUS_ERROR(status, "encode null value failed, index name[%s].", indexConfig->GetIndexName().c_str());
    return status;
}

template <typename T>
inline bool MultiValueAttributeMemIndexer<T>::IsDirty() const
{
    return _accessor->GetDocCount() > 0;
}

template <typename T>
void MultiValueAttributeMemIndexer<T>::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    int64_t poolSize = _pool->getUsedBytes() + _simplePool.getUsedBytes();
    // dump file size = data file size + max offset file size
    size_t docCount = _accessor->GetDocCount();
    int64_t dumpOffsetFileSize = sizeof(uint64_t) * docCount;
    int64_t dumpFileSize = _accessor->GetAppendDataSize() + dumpOffsetFileSize;
    int64_t dumpTempBufferSize = _dumpEstimateFactor * docCount;

    memUpdater->UpdateCurrentMemUse(poolSize);
    memUpdater->EstimateDumpTmpMemUse(dumpTempBufferSize);
    memUpdater->EstimateDumpExpandMemUse(0);
    memUpdater->EstimateDumpedFileSize(dumpFileSize);
}

template <typename T>
inline void MultiValueAttributeMemIndexer<T>::AddField(docid_t docId, const autil::StringView& attributeValue,
                                                       bool isNull)
{
    assert(_attrConvertor);
    AttrValueMeta meta =
        isNull ? _attrConvertor->Decode(autil::StringView(_nullValue)) : _attrConvertor->Decode(attributeValue);
    _accessor->AppendValue(meta.data, meta.hashKey);
}

template <typename T>
inline bool MultiValueAttributeMemIndexer<T>::UpdateField(docid_t docId, const autil::StringView& value, bool isNull,
                                                          const uint64_t* hashKey)
{
    bool retFlag = false;
    if (hashKey != nullptr) {
        retFlag = _accessor->UpdateValue(docId, value, *hashKey);
    } else {
        retFlag = _accessor->UpdateValue(docId, value);
    }
    return retFlag;
}

template <typename T>
inline Status
MultiValueAttributeMemIndexer<T>::Dump(autil::mem_pool::PoolBase* dumpPool,
                                       const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                                       const std::shared_ptr<framework::DumpParams>& dumpParams)
{
    std::string attributeName = _attrConfig->GetAttrName();
    AUTIL_LOG(DEBUG, "Dumping attribute : [%s]", attributeName.c_str());
    {
        /*
            For alter field custom merge task recover
            To be removed
        */
        auto [isExistStatus, isExist] = indexDirectory->GetIDirectory()->IsExist(attributeName).StatusWith();
        if (!isExistStatus.IsOK()) {
            AUTIL_LOG(ERROR, "check file exist fail. file: [%s], error: [%s]", attributeName.c_str(),
                      isExistStatus.ToString().c_str());
            return isExistStatus;
        }
        if (isExist) {
            AUTIL_LOG(WARN, "File [%s] already exist, will be removed.", attributeName.c_str());
            indexlib::file_system::RemoveOption removeOption;
            auto removeStatus = indexDirectory->GetIDirectory()->RemoveDirectory(attributeName, removeOption).Status();
            if (!removeStatus.IsOK()) {
                AUTIL_LOG(ERROR, "remove file fail. file: [%s], error: [%s]", attributeName.c_str(),
                          removeStatus.ToString().c_str());
                return removeStatus;
            }
        }
    }

    auto param = VarLenDataParamHelper::MakeParamForAttribute(_attrConfig);
    FileCompressParamHelper::SyncParam(_attrConfig->GetFileCompressConfigV2(), nullptr, param);
    VarLenDataDumper dumper;
    dumper.Init(_accessor, param);

    auto [mkDirStatus, attrDir] = indexDirectory->GetIDirectory()
                                      ->MakeDirectory(attributeName, indexlib::file_system::DirectoryOption())
                                      .StatusWith();
    if (!mkDirStatus.IsOK()) {
        AUTIL_LOG(ERROR, "make diretory fail. file: [%s], error: [%s]", attributeName.c_str(),
                  mkDirStatus.ToString().c_str());
        return mkDirStatus;
    }

    auto orderParams = std::dynamic_pointer_cast<DocMapDumpParams>(dumpParams);
    std::vector<docid_t>* new2old = orderParams ? &orderParams->new2old : nullptr;

    auto dumpStatus =
        dumper.Dump(attrDir, ATTRIBUTE_OFFSET_FILE_NAME, ATTRIBUTE_DATA_FILE_NAME, nullptr, new2old, dumpPool);
    if (!dumpStatus.IsOK()) {
        AUTIL_LOG(ERROR, "MultiValueAttributeMemIndexer dump fail. file: [%s], error: [%s]", attributeName.c_str(),
                  dumpStatus.ToString().c_str());
        return dumpStatus;
    }

    AttributeDataInfo dataInfo(dumper.GetDataItemCount(), dumper.GetMaxItemLength());

    auto storeStatus =
        attrDir->Store(ATTRIBUTE_DATA_INFO_FILE_NAME, dataInfo.ToString(), indexlib::file_system::WriterOption())
            .Status();
    if (!storeStatus.IsOK()) {
        AUTIL_LOG(ERROR, "MultiValueAttributeMemIndexer store file fail. file: [%s], error: [%s]",
                  attributeName.c_str(), storeStatus.ToString().c_str());
        return storeStatus;
    }
    AUTIL_LOG(DEBUG, "Finish Store attribute data info");
    return Status::OK();
}

template <typename T>
inline const std::shared_ptr<AttributeMemReader> MultiValueAttributeMemIndexer<T>::CreateInMemReader() const
{
    return std::make_shared<MultiValueAttributeMemReader<T>>(_accessor, _attrConfig->GetCompressType(),
                                                             _attrConfig->GetFieldConfig()->GetFixedMultiValueCount(),
                                                             _attrConfig->SupportNull());
}

template <typename T>
uint64_t MultiValueAttributeMemIndexer<T>::InitDumpEstimateFactor() const
{
    double factor = 0;
    indexlib::config::CompressTypeOption compressType = _attrConfig->GetCompressType();
    if (compressType.HasEquivalentCompress()) {
        // use max offset type
        factor += sizeof(uint64_t) * 2;
    }

    if (compressType.HasUniqEncodeCompress()) {
        // hash map for uniq comress dump
        factor += 2 * sizeof(std::pair<uint64_t, uint64_t>);
    }
    // adaptive offset
    factor += sizeof(uint32_t);
    return factor;
}

} // namespace indexlibv2::index
