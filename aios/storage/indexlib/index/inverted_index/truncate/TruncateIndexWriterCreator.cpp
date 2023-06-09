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
#include "indexlib/index/inverted_index/truncate/TruncateIndexWriterCreator.h"

#include "indexlib/base/Status.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/truncate/DocCollectorCreator.h"
#include "indexlib/index/inverted_index/truncate/EvaluatorCreator.h"
#include "indexlib/index/inverted_index/truncate/MultiTruncateIndexWriter.h"
#include "indexlib/index/inverted_index/truncate/SingleTruncateIndexWriter.h"
#include "indexlib/index/inverted_index/truncate/TimeStrategyTruncateMetaReader.h"
#include "indexlib/index/inverted_index/truncate/TruncateMetaFileReaderCreator.h"
#include "indexlib/index/inverted_index/truncate/TruncateTriggerCreator.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, TruncateIndexWriterCreator);

TruncateIndexWriterCreator::TruncateIndexWriterCreator(
    const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos, bool optimize)
    : _segmentMergeInfos(segmentMergeInfos)
    , _isOptimize(optimize)
{
}

void TruncateIndexWriterCreator::Init(

    const std::shared_ptr<indexlibv2::config::TruncateOptionConfig>& truncateOptionConfig,
    const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper,
    const std::shared_ptr<TruncateAttributeReaderCreator>& truncateAttrCreator, const BucketMaps& bucketMaps,
    const std::shared_ptr<TruncateMetaFileReaderCreator>& truncateMetaFileReaderCreator, int64_t srcVersionTimestamp)
{
    _truncateOptionConfig = truncateOptionConfig;
    _truncateAttributeReaderCreator = truncateAttrCreator;
    _bucketMaps = bucketMaps;
    _docMapper = docMapper;
    _truncateMetaFileReaderCreator = truncateMetaFileReaderCreator;
    int64_t beginTime = srcVersionTimestamp;
    int64_t baseTime = GetBaseTime(beginTime);
    _truncateOptionConfig->UpdateTruncateIndexConfig(beginTime, baseTime);
}

int64_t TruncateIndexWriterCreator::GetBaseTime(int64_t beginTime)
{
    struct tm lts;
    (void)localtime_r(&beginTime, &lts);
    lts.tm_hour = 0;
    lts.tm_min = 0;
    lts.tm_sec = 0;

    return mktime(&lts);
}

std::shared_ptr<TruncateIndexWriter>
TruncateIndexWriterCreator::Create(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                   const file_system::IOConfig& ioConfig)
{
    assert(indexConfig);
    auto invertedIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
    assert(invertedIndexConfig != nullptr);
    if (invertedIndexConfig->GetShardingType() == indexlibv2::config::InvertedIndexConfig::IST_NEED_SHARDING) {
        return nullptr;
    }

    const std::string& indexName = invertedIndexConfig->GetIndexName();
    if (!_truncateOptionConfig->IsTruncateIndex(indexName)) {
        AUTIL_LOG(WARN, "indexName[%s] not exist in truncate option config", indexName.c_str());
        return nullptr;
    }

    const indexlibv2::config::TruncateIndexConfig& truncateIndexConfig =
        _truncateOptionConfig->GetTruncateIndexConfig(indexName);
    const auto& truncateIndexProperties = truncateIndexConfig.GetTruncateIndexProperties();
    assert(truncateIndexProperties.size() > 0);

    auto multiIndexWriter = std::make_shared<MultiTruncateIndexWriter>();
    auto bucketVecAllocator = std::make_shared<BucketVectorAllocator>();
    for (const auto& truncateIndexProperty : truncateIndexProperties) {
        std::shared_ptr<TruncateIndexWriter> indexWriter =
            CreateSingleIndexWriter(truncateIndexProperty, invertedIndexConfig, bucketVecAllocator, ioConfig);
        if (indexWriter != nullptr) {
            multiIndexWriter->AddIndexWriter(indexWriter);
        }
    }
    return multiIndexWriter;
}

std::pair<Status, std::shared_ptr<TruncateMetaReader>>
TruncateIndexWriterCreator::CreateMetaReader(const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty,
                                             const std::shared_ptr<file_system::FileReader>& metaFile)
{
    if (!NeedTruncMeta(truncateIndexProperty)) {
        if (metaFile) {
            RETURN2_IF_STATUS_ERROR(metaFile->Close().Status(), nullptr, "close truncate meta file failed.");
        }
        return {Status::OK(), nullptr};
    }
    std::shared_ptr<TruncateMetaReader> metaReader;
    bool desc = truncateIndexProperty.truncateProfile->sortParams[0].IsDesc();
    if (!truncateIndexProperty.IsFilterByTimeStamp()) {
        metaReader.reset(new TruncateMetaReader(desc));
    } else {
        const indexlibv2::config::DiversityConstrain& constrain =
            truncateIndexProperty.truncateStrategy->GetDiversityConstrain();
        metaReader.reset(
            new TimeStrategyTruncateMetaReader(constrain.GetFilterMinValue(), constrain.GetFilterMaxValue(), desc));
    }
    auto st = metaReader->Open(metaFile);
    RETURN2_IF_STATUS_ERROR(st, nullptr, "meta reader open failed");
    return {Status::OK(), metaReader};
}

bool TruncateIndexWriterCreator::CheckBitmapConfigValid(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
    const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty) const
{
    if (indexConfig->GetHighFrequencyTermPostingType() == hp_bitmap && indexConfig->GetDictConfig() &&
        indexConfig->GetTruncateTermVocabulary() &&
        truncateIndexProperty.GetStrategyType() != TRUNCATE_META_STRATEGY_TYPE) {
        return false;
    }
    return true;
}

std::shared_ptr<TruncateIndexWriter> TruncateIndexWriterCreator::CreateSingleIndexWriter(
    const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty,
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
    const std::shared_ptr<BucketVectorAllocator>& bucketVecAllocator, const file_system::IOConfig& ioConfig)
{
    const auto& truncateIndexName = truncateIndexProperty.truncateIndexName;
    if (!CheckBitmapConfigValid(indexConfig, truncateIndexProperty)) {
        const std::string& indexName = indexConfig->GetIndexName();
        AUTIL_LOG(ERROR,
                  "hp_bitmap typed index [%s] should use truncate_meta"
                  " Strategy when already has truncate term vocabulary, truncate index[%s]",
                  truncateIndexName.c_str(), indexName.c_str());
        return nullptr;
    }

    std::shared_ptr<DocInfoAllocator> allocator = CreateDocInfoAllocator(truncateIndexProperty);
    if (!allocator) {
        AUTIL_LOG(ERROR, "get doc info allocator failed, truncate index[%s].", truncateIndexName.c_str());
        return nullptr;
    }

    file_system::ReaderOption option(file_system::FSOT_BUFFERED);
    option.mayNonExist = true;
    auto [status, truncateMetaFile] =
        _truncateMetaFileReaderCreator->Create(truncateIndexProperty.truncateIndexName, option);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create file reader for truncate index failed, index[%s]", truncateIndexName.c_str());
        return nullptr;
    }
    auto [st, metaReader] = CreateMetaReader(truncateIndexProperty, truncateMetaFile);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "create meta reader failed, truncate index[%s]", truncateIndexName.c_str());
        return nullptr;
    }

    auto evaluator = EvaluatorCreator::Create(truncateIndexProperty, _truncateAttributeReaderCreator, allocator);

    std::shared_ptr<DocCollector> docCollector =
        DocCollectorCreator::Create(truncateIndexProperty, allocator, _bucketMaps, _truncateAttributeReaderCreator,
                                    bucketVecAllocator, metaReader, evaluator);

    auto truncateTrigger = TruncateTriggerCreator::Create(truncateIndexProperty);
    truncateTrigger->SetTruncateMetaReader(metaReader);

    auto writer = std::make_shared<SingleTruncateIndexWriter>(indexConfig, _docMapper);
    writer->SetParam(evaluator, docCollector, truncateTrigger, truncateIndexProperty.truncateIndexName, allocator,
                     ioConfig);

    if (truncateIndexProperty.HasSort()) {
        auto truncateMetaDir = _segmentMergeInfos.relocatableGlobalRoot->MakeRelocatableFolder(TRUNCATE_META_DIR_NAME);
        if (truncateMetaDir == nullptr) {
            AUTIL_LOG(ERROR, "make relocatable folder failed, truncate index[%s]", truncateIndexName.c_str());
            return nullptr;
        }

        const auto& firstSortParam = truncateIndexProperty.truncateProfile->sortParams[0];
        std::shared_ptr<file_system::FileWriter> fileWriter;
        auto st = Status::OK();
        if (_isOptimize) {
            st = _segmentMergeInfos.relocatableGlobalRoot->RemoveFile(truncateIndexProperty.truncateIndexName,
                                                                      file_system::RemoveOption::MayNonExist());
            if (!st.IsOK()) {
                AUTIL_LOG(ERROR, "remove truncate meta file for [%s] failed", truncateIndexName.c_str());
                return nullptr;
            }
            std::tie(st, fileWriter) = truncateMetaDir->CreateFileWriter(
                truncateIndexProperty.truncateIndexName, indexlib::file_system::WriterOption::AtomicDump());
            if (!st.IsOK()) {
                AUTIL_LOG(ERROR, "create file writer for [%s] failed", truncateIndexName.c_str());
                return nullptr;
            }
            AUTIL_LOG(INFO, "optimize merge create file writer for [%s] success", truncateIndexName.c_str());
            writer->SetTruncateIndexMetaInfo(fileWriter, firstSortParam.GetSortField(), firstSortParam.IsDesc());
        } else {
            if (truncateMetaFile == nullptr) {
                std::tie(st, fileWriter) = truncateMetaDir->CreateFileWriter(
                    truncateIndexProperty.truncateIndexName, indexlib::file_system::WriterOption::AtomicDump());
                if (!st.IsOK()) {
                    AUTIL_LOG(ERROR, "create file writer for [%s] failed", truncateIndexName.c_str());
                    return nullptr;
                }
                AUTIL_LOG(INFO, "unable to find existing file, create file writer for [%s] success",
                          truncateIndexName.c_str());
                writer->SetTruncateIndexMetaInfo(fileWriter, firstSortParam.GetSortField(), firstSortParam.IsDesc());
            }
        }
    }
    AUTIL_LOG(INFO, "create single truncate index writer success, truncate[%s]", truncateIndexName.c_str());
    return writer;
}

bool TruncateIndexWriterCreator::CheckReferenceField(const std::string& fieldName) const
{
    if (fieldName == DOC_PAYLOAD_FIELD_NAME) {
        return true;
    }
    auto attrConfig = _truncateAttributeReaderCreator->GetAttributeConfig(fieldName);
    if (attrConfig == nullptr) {
        AUTIL_LOG(ERROR, "field [%s] not an attribute", fieldName.c_str());
        return false;
    }
    return true;
}

bool TruncateIndexWriterCreator::GetReferenceFields(
    const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty, std::vector<std::string>& fieldNames) const
{
    const auto& constrain = truncateIndexProperty.truncateStrategy->GetDiversityConstrain();

    if (truncateIndexProperty.HasSort()) {
        size_t sortDimenNum = truncateIndexProperty.GetSortDimenNum();
        for (size_t i = 0; i < sortDimenNum; ++i) {
            const std::string& fieldName = truncateIndexProperty.truncateProfile->sortParams[i].GetSortField();
            if (!CheckReferenceField(fieldName)) {
                return false;
            }
            fieldNames.push_back(fieldName);
            if (indexlibv2::config::TruncateProfileConfig::IsSortParamByDocPayload(
                    truncateIndexProperty.truncateProfile->sortParams[i])) {
                const std::string& factorFieldName = truncateIndexProperty.truncateProfile->GetDocPayloadFactorField();
                if (!factorFieldName.empty()) {
                    if (!CheckReferenceField(factorFieldName)) {
                        return false;
                    }
                    fieldNames.push_back(factorFieldName);
                }
            }
        }
    }

    if (!GetReferenceFieldsFromConstrain(constrain, &fieldNames)) {
        return false;
    }
    return true;
}

bool TruncateIndexWriterCreator::GetReferenceFieldsFromConstrain(
    const indexlibv2::config::DiversityConstrain& constrain, std::vector<std::string>* fieldNames) const
{
    if (constrain.NeedFilter()) {
        const std::string& fieldName = constrain.GetFilterField();
        if (!CheckReferenceField(fieldName)) {
            return false;
        }
        fieldNames->push_back(fieldName);
    }

    if (constrain.NeedDistinct()) {
        const std::string& fieldName = constrain.GetDistField();
        if (!CheckReferenceField(fieldName)) {
            return false;
        }
        fieldNames->push_back(fieldName);
    }
    return true;
}

std::shared_ptr<DocInfoAllocator> TruncateIndexWriterCreator::CreateDocInfoAllocator(
    const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty)
{
    assert(_allocators.find(truncateIndexProperty.truncateIndexName) == _allocators.end());

    std::vector<std::string> referFields;
    if (!GetReferenceFields(truncateIndexProperty, referFields)) {
        return nullptr;
    }

    auto allocator = std::make_shared<DocInfoAllocator>();
    _allocators[truncateIndexProperty.truncateIndexName] = allocator;

    for (size_t i = 0; i < referFields.size(); ++i) {
        DeclareReference(referFields[i], allocator);
    }
    return allocator;
}

void TruncateIndexWriterCreator::DeclareReference(const std::string& fieldName,
                                                  const std::shared_ptr<DocInfoAllocator>& allocator)
{
    if (fieldName == DOC_PAYLOAD_FIELD_NAME) {
        allocator->DeclareReference(fieldName, ft_double, false);
    } else {
        auto attrConfig = _truncateAttributeReaderCreator->GetAttributeConfig(fieldName);
        assert(attrConfig != nullptr);
        allocator->DeclareReference(fieldName, attrConfig->GetFieldType(),
                                    attrConfig->GetFieldConfig()->IsEnableNullField());
    }
}

bool TruncateIndexWriterCreator::NeedTruncMeta(
    const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty) const
{
    return (truncateIndexProperty.GetStrategyType() == TRUNCATE_META_STRATEGY_TYPE) && truncateIndexProperty.HasSort();
}

} // namespace indexlib::index
