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

#include <any>
#include <memory>

#include "indexlib/base/Types.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/attribute/AttributeMemIndexer.h"
#include "indexlib/index/attribute/AttributeMemIndexerCreator.h"
#include "indexlib/index/attribute/SingleValueAttributeMemReader.h"
#include "indexlib/index/attribute/format/SingleValueAttributeMemFormatter.h"
#include "indexlib/index/common/data_structure/AttributeCompressInfo.h"
#include "indexlib/index/common/field_format/attribute/TypeInfo.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueAttributeMemIndexer : public AttributeMemIndexer
{
public:
    SingleValueAttributeMemIndexer(const MemIndexerParameter& indexerParam) : AttributeMemIndexer(indexerParam) {}

    ~SingleValueAttributeMemIndexer() = default;

public:
    class Creator : public AttributeMemIndexerCreator
    {
    public:
        FieldType GetAttributeType() const override { return TypeInfo<T>::GetFieldType(); }

        std::unique_ptr<AttributeMemIndexer> Create(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                    const MemIndexerParameter& indexerParam) const override
        {
            std::shared_ptr<AttributeConfig> attrConfig = std::dynamic_pointer_cast<AttributeConfig>(indexConfig);
            assert(nullptr != attrConfig);
            auto fieldConfig = attrConfig->GetFieldConfig();
            assert(nullptr != fieldConfig);
            FieldType fieldType = fieldConfig->GetFieldType();
            if (fieldType == FieldType::ft_float) {
                auto compressType = attrConfig->GetCompressType();
                if (compressType.HasInt8EncodeCompress()) {
                    return std::make_unique<SingleValueAttributeMemIndexer<int8_t>>(indexerParam);
                }
                if (compressType.HasFp16EncodeCompress()) {
                    return std::make_unique<SingleValueAttributeMemIndexer<int16_t>>(indexerParam);
                }
            }
            return std::make_unique<SingleValueAttributeMemIndexer<T>>(indexerParam);
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
    void AddField(docid_t docId, const T& attributeValue, bool isNull);
    virtual bool UpdateField(docid_t docId, const autil::StringView& attributeValue, bool isNull,
                             const uint64_t* hashKey) override;
    bool IsDirty() const override;

private:
    std::shared_ptr<SingleValueAttributeMemFormatter<T>> _formatter;
    std::unique_ptr<document::extractor::IDocumentInfoExtractor> _docInfoExtractor;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeMemIndexer, T);

/////////////////////////////////////////////////////////
// inline functions
template <typename T>
inline Status
SingleValueAttributeMemIndexer<T>::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                        document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    auto status = AttributeMemIndexer::Init(indexConfig, docInfoExtractorFactory);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "init SingleValueAttributeMemIndexer fail, error: [%s]", status.ToString().c_str());
        return status;
    }
    _formatter.reset(new SingleValueAttributeMemFormatter<T>(_attrConfig));
    _formatter->Init(_pool, _attrConvertor);

    auto fieldId = std::make_any<fieldid_t>(_attrConfig->GetFieldId());
    _docInfoExtractor = docInfoExtractorFactory->CreateDocumentInfoExtractor(
        indexConfig, document::extractor::DocumentInfoExtractorType::ATTRIBUTE_FIELD, fieldId);
    return status;
}

template <typename T>
void SingleValueAttributeMemIndexer<T>::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    int64_t poolSize = _pool->getUsedBytes() + _simplePool.getUsedBytes();
    int64_t dumpFileSize = _formatter->GetDumpFileSize();
    int64_t dumpTempBufferSize = 0;
    if (_attrConfig->GetCompressType().HasEquivalentCompress()) {
        // compressor need double buffer
        // worst case : no compress at all
        dumpTempBufferSize = dumpFileSize * 2;
    }
    memUpdater->UpdateCurrentMemUse(poolSize);
    memUpdater->EstimateDumpTmpMemUse(dumpTempBufferSize);
    // do not report zero value metrics for performance consideration
    // memUpdater->EstimateDumpExpandMemUse(0);
    memUpdater->EstimateDumpedFileSize(dumpFileSize);
}

template <typename T>
inline bool SingleValueAttributeMemIndexer<T>::IsDirty() const
{
    return _formatter->GetDocCount() > 0;
}

template <typename T>
inline void SingleValueAttributeMemIndexer<T>::AddField(docid_t docId, const autil::StringView& attributeValue,
                                                        bool isNull)
{
    _formatter->AddField(docId, attributeValue, isNull);
}

template <typename T>
void SingleValueAttributeMemIndexer<T>::AddField(docid_t docId, const T& attributeValue, bool isNull)
{
    _formatter->AddField(docId, attributeValue, isNull);
}

template <typename T>
inline bool SingleValueAttributeMemIndexer<T>::UpdateField(docid_t docId, const autil::StringView& attributeValue,
                                                           bool isNull, const uint64_t* hashKey)
{
    _formatter->UpdateField(docId, attributeValue, isNull);
    return true;
}

template <typename T>
inline Status
SingleValueAttributeMemIndexer<T>::Dump(autil::mem_pool::PoolBase* dumpPool,
                                        const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                                        const std::shared_ptr<framework::DumpParams>& dumpParams)
{
    return _formatter->Dump(indexDirectory, dumpPool, dumpParams);
}

template <typename T>
inline const std::shared_ptr<AttributeMemReader> SingleValueAttributeMemIndexer<T>::CreateInMemReader() const
{
    return std::make_shared<SingleValueAttributeMemReader<T>>(_formatter.get(), indexlib::config::CompressTypeOption());
}

template <>
inline const std::shared_ptr<AttributeMemReader> SingleValueAttributeMemIndexer<int16_t>::CreateInMemReader() const
{
    const indexlib::config::CompressTypeOption& compress = _attrConfig->GetCompressType();
    if (compress.HasFp16EncodeCompress()) {
        return std::make_shared<SingleValueAttributeMemReader<float>>(_formatter.get(), compress);
    }
    return std::make_shared<SingleValueAttributeMemReader<int16_t>>(_formatter.get(),
                                                                    indexlib::config::CompressTypeOption());
}

template <>
inline const std::shared_ptr<AttributeMemReader> SingleValueAttributeMemIndexer<int8_t>::CreateInMemReader() const
{
    const indexlib::config::CompressTypeOption& compress = _attrConfig->GetCompressType();
    if (compress.HasInt8EncodeCompress()) {
        return std::make_shared<SingleValueAttributeMemReader<float>>(_formatter.get(), compress);
    }
    return std::make_shared<SingleValueAttributeMemReader<int8_t>>(_formatter.get(),
                                                                   indexlib::config::CompressTypeOption());
}

} // namespace indexlibv2::index
