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
#include "indexlib/index/merger_util/truncate/truncate_index_writer_creator.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/archive/ArchiveFile.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/merger_util/truncate/bucket_vector_allocator.h"
#include "indexlib/index/merger_util/truncate/doc_collector_creator.h"
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/index/merger_util/truncate/evaluator_creator.h"
#include "indexlib/index/merger_util/truncate/multi_truncate_index_writer.h"
#include "indexlib/index/merger_util/truncate/single_truncate_index_writer.h"
#include "indexlib/index/merger_util/truncate/time_strategy_truncate_meta_reader.h"
#include "indexlib/index/merger_util/truncate/truncate_common.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer.h"
#include "indexlib/index/merger_util/truncate/truncate_meta_reader.h"
#include "indexlib/index/merger_util/truncate/truncate_trigger_creator.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::file_system;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, TruncateIndexWriterCreator);

TruncateIndexWriterCreator::TruncateIndexWriterCreator(bool optimize) : mIsOptimize(optimize) {}

TruncateIndexWriterCreator::~TruncateIndexWriterCreator() {}

void TruncateIndexWriterCreator::Init(const IndexPartitionSchemaPtr& schema, const MergeConfig& mergeConfig,
                                      const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
                                      const ReclaimMapPtr& reclaimMap,
                                      const file_system::ArchiveFolderPtr& metaRootFolder,
                                      TruncateAttributeReaderCreator* truncateAttrCreator, const BucketMaps* bucketMaps,
                                      int64_t beginTimeSecond)
{
    mTruncateAttrCreator = truncateAttrCreator;

    mTruncOptionConfig = mergeConfig.truncateOptionConfig;
    int64_t beginTime = beginTimeSecond;
    int64_t baseTime = GetBaseTime(beginTime);
    mTruncOptionConfig->UpdateTruncateIndexConfig(beginTime, baseTime);
    mOutputSegmentMergeInfos = outputSegmentMergeInfos;
    mReclaimMap = reclaimMap;
    mBucketMaps = bucketMaps;
    mTruncateMetaFolder = metaRootFolder;
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

TruncateIndexWriterPtr TruncateIndexWriterCreator::Create(const config::IndexConfigPtr& indexConfig,
                                                          const config::MergeIOConfig& ioConfig)
{
    assert(indexConfig);
    if (indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING) {
        return TruncateIndexWriterPtr();
    }

    const string& indexName = indexConfig->GetIndexName();
    if (!mTruncOptionConfig->IsTruncateIndex(indexName)) {
        return TruncateIndexWriterPtr();
    }

    const TruncateIndexConfig& truncIndexConfig = mTruncOptionConfig->GetTruncateIndexConfig(indexName);
    const TruncateIndexPropertyVector& truncIndexProperties = truncIndexConfig.GetTruncateIndexProperties();
    assert(truncIndexProperties.size() > 0);

    MultiTruncateIndexWriterPtr multiIndexWriter(new MultiTruncateIndexWriter());
    BucketVectorAllocatorPtr bucketVecAllocator(new BucketVectorAllocator());
    for (size_t i = 0; i < truncIndexProperties.size(); ++i) {
        TruncateIndexWriterPtr indexWriter =
            CreateSingleIndexWriter(truncIndexProperties[i], indexConfig, bucketVecAllocator, ioConfig);
        if (indexWriter) {
            multiIndexWriter->AddIndexWriter(indexWriter);
        }
    }
    return multiIndexWriter;
}

TruncateMetaReaderPtr TruncateIndexWriterCreator::CreateMetaReader(const TruncateIndexProperty& truncateIndexProperty,
                                                                   const FileReaderPtr& metaFile)
{
    TruncateMetaReaderPtr metaReader;
    if (!NeedTruncMeta(truncateIndexProperty)) {
        if (metaFile) {
            metaFile->Close().GetOrThrow();
        }
        return metaReader;
    }

    bool desc = truncateIndexProperty.mTruncateProfile->mSortParams[0].IsDesc();
    if (!truncateIndexProperty.IsFilterByTimeStamp()) {
        metaReader.reset(new TruncateMetaReader(desc));
    } else {
        const DiversityConstrain& constrain = truncateIndexProperty.mTruncateStrategy->GetDiversityConstrain();
        metaReader.reset(
            new TimeStrategyTruncateMetaReader(constrain.GetFilterMinValue(), constrain.GetFilterMaxValue(), desc));
    }
    metaReader->Open(metaFile);
    return metaReader;
}

bool TruncateIndexWriterCreator::CheckBitmapConfigValid(const IndexConfigPtr& indexConfig,
                                                        const TruncateIndexProperty& truncateIndexProperty)
{
    if (indexConfig->GetHighFrequencyTermPostingType() == indexlib::index::hp_bitmap && indexConfig->GetDictConfig() &&
        indexConfig->GetTruncateTermVocabulary() &&
        truncateIndexProperty.GetStrategyType() != TRUNCATE_META_STRATEGY_TYPE) {
        return false;
    }
    return true;
}

TruncateIndexWriterPtr TruncateIndexWriterCreator::CreateSingleIndexWriter(
    const TruncateIndexProperty& truncateIndexProperty, const IndexConfigPtr& indexConfig,
    const BucketVectorAllocatorPtr& bucketVecAllocator, const config::MergeIOConfig& ioConfig)
{
    if (!CheckBitmapConfigValid(indexConfig, truncateIndexProperty)) {
        const string& indexName = indexConfig->GetIndexName();
        IE_LOG(ERROR,
               "hp_bitmap typed index [%s] should use truncate_meta"
               " Strategy when already has truncate term vocabulary!",
               indexName.c_str());
        return TruncateIndexWriterPtr();
    }

    DocInfoAllocatorPtr allocator = CreateDocInfoAllocator(truncateIndexProperty);
    if (!allocator) {
        return TruncateIndexWriterPtr();
    }
    FileReaderPtr truncateMetaFile =
        mTruncateMetaFolder->CreateFileReader(truncateIndexProperty.mTruncateIndexName).GetOrThrow();
    // string truncateMetaFile
    //     = util::PathUtil::JoinPath(mTruncateMetaDir, truncateIndexProperty.mTruncateIndexName);
    TruncateMetaReaderPtr metaReader = CreateMetaReader(truncateIndexProperty, truncateMetaFile);

    EvaluatorPtr evaluator = EvaluatorCreator::Create(*truncateIndexProperty.mTruncateProfile,
                                                      truncateIndexProperty.mTruncateStrategy->GetDiversityConstrain(),
                                                      mTruncateAttrCreator, allocator);

    DocCollectorPtr docCollector =
        DocCollectorCreator::Create(truncateIndexProperty, allocator, *mBucketMaps, mTruncateAttrCreator,
                                    bucketVecAllocator, metaReader, evaluator);

    const auto& reTruncateConfig = truncateIndexProperty.mTruncateStrategy->GetReTruncateConfig();

    TruncateTriggerPtr truncateTrigger = TruncateTriggerCreator::Create(truncateIndexProperty);
    truncateTrigger->SetTruncateMetaReader(metaReader);

    SingleTruncateIndexWriterPtr writer(new SingleTruncateIndexWriter(indexConfig, mReclaimMap));
    writer->Init(evaluator, DocCollectorPtr(docCollector), truncateTrigger, truncateIndexProperty.mTruncateIndexName,
                 allocator, ioConfig);

    if (reTruncateConfig.NeedReTruncate()) {
        std::unique_ptr<DocCollector> reTruncateCollector = DocCollectorCreator::CreateReTruncateCollector(
            truncateIndexProperty, allocator, *mBucketMaps, mTruncateAttrCreator, bucketVecAllocator, metaReader,
            evaluator);
        EvaluatorPtr reTruncateEvaluator =
            EvaluatorCreator::Create(*truncateIndexProperty.mTruncateProfile, reTruncateConfig.GetDiversityConstrain(),
                                     mTruncateAttrCreator, allocator);
        writer->SetReTruncateInfo(std::move(reTruncateEvaluator), std::move(reTruncateCollector),
                                  truncateIndexProperty.mTruncateProfile->mSortParams[0].IsDesc());
    }

    if (truncateIndexProperty.HasSort()) {
        file_system::FileWriterPtr fileWriter;
        if (mIsOptimize) {
            truncateMetaFile.reset();
            mTruncateMetaFolder->GetRootDirectory()
                ->RemoveFile(truncateIndexProperty.mTruncateIndexName, RemoveOption::MayNonExist())
                .GetOrThrow();
            fileWriter = mTruncateMetaFolder->CreateFileWriter(truncateIndexProperty.mTruncateIndexName).GetOrThrow();
            writer->SetTruncateIndexMetaInfo(fileWriter,
                                             truncateIndexProperty.mTruncateProfile->mSortParams[0].GetSortField(),
                                             truncateIndexProperty.mTruncateProfile->mSortParams[0].IsDesc());
        } else {
            if (!truncateMetaFile) {
                fileWriter =
                    mTruncateMetaFolder->CreateFileWriter(truncateIndexProperty.mTruncateIndexName).GetOrThrow();
                writer->SetTruncateIndexMetaInfo(fileWriter,
                                                 truncateIndexProperty.mTruncateProfile->mSortParams[0].GetSortField(),
                                                 truncateIndexProperty.mTruncateProfile->mSortParams[0].IsDesc());
            }
        }
    }
    return writer;
}

bool TruncateIndexWriterCreator::CheckReferenceField(const std::string& fieldName)
{
    if (fieldName == DOC_PAYLOAD_FIELD_NAME) {
        return true;
    }
    const AttributeSchemaPtr& attrSchema = mTruncateAttrCreator->GetAttributeSchema();
    if (!attrSchema->GetAttributeConfig(fieldName)) {
        INDEXLIB_FATAL_ERROR(NonExist, "fieldName[%s] does not exist", fieldName.c_str());
    }
    return true;
}

bool TruncateIndexWriterCreator::GetReferenceFields(vector<string>& fieldNames,
                                                    const TruncateIndexProperty& truncateIndexProperty)
{
    const DiversityConstrain& constrain = truncateIndexProperty.mTruncateStrategy->GetDiversityConstrain();

    if (truncateIndexProperty.HasSort()) {
        size_t sortDimenNum = truncateIndexProperty.GetSortDimenNum();
        for (size_t i = 0; i < sortDimenNum; ++i) {
            const string& fieldName = truncateIndexProperty.mTruncateProfile->mSortParams[i].GetSortField();
            if (!CheckReferenceField(fieldName)) {
                return false;
            }
            fieldNames.push_back(fieldName);
            if (TruncateProfile::IsSortParamByDocPayload(truncateIndexProperty.mTruncateProfile->mSortParams[i])) {
                const string& factorFieldName = truncateIndexProperty.mTruncateProfile->GetDocPayloadFactorField();
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
    const auto& reTruncateConfig = truncateIndexProperty.mTruncateStrategy->GetReTruncateConfig();
    if (reTruncateConfig.NeedReTruncate()) {
        const DiversityConstrain& reTruncateConstrain = reTruncateConfig.GetDiversityConstrain();
        if (!GetReferenceFieldsFromConstrain(reTruncateConstrain, &fieldNames)) {
            return false;
        }
    }
    return true;
}

bool TruncateIndexWriterCreator::GetReferenceFieldsFromConstrain(const config::DiversityConstrain& constrain,
                                                                 std::vector<std::string>* fieldNames)
{
    if (constrain.NeedFilter()) {
        const string& fieldName = constrain.GetFilterField();
        if (!CheckReferenceField(fieldName)) {
            return false;
        }
        fieldNames->push_back(fieldName);
    }

    if (constrain.NeedDistinct()) {
        const string& fieldName = constrain.GetDistField();
        if (!CheckReferenceField(fieldName)) {
            return false;
        }
        fieldNames->push_back(fieldName);
    }
    return true;
}

DocInfoAllocatorPtr
TruncateIndexWriterCreator::CreateDocInfoAllocator(const TruncateIndexProperty& truncateIndexProperty)
{
    assert(mAllocators.find(truncateIndexProperty.mTruncateIndexName) == mAllocators.end());

    vector<string> referFields;
    if (!GetReferenceFields(referFields, truncateIndexProperty)) {
        return DocInfoAllocatorPtr();
    }

    DocInfoAllocatorPtr allocator(new DocInfoAllocator());
    mAllocators[truncateIndexProperty.mTruncateIndexName] = allocator;

    for (size_t i = 0; i < referFields.size(); ++i) {
        DeclareReference(referFields[i], allocator);
    }
    return allocator;
}

void TruncateIndexWriterCreator::DeclareReference(const string& fieldName, const DocInfoAllocatorPtr& allocator)
{
    if (fieldName == DOC_PAYLOAD_FIELD_NAME) {
        allocator->DeclareReference(fieldName, ft_double, false);
    } else {
        const AttributeSchemaPtr& attrSchema = mTruncateAttrCreator->GetAttributeSchema();
        const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(fieldName);
        if (!attrConfig) {
            INDEXLIB_FATAL_ERROR(NonExist, "fieldName[%s] does not exist", fieldName.c_str());
        }
        allocator->DeclareReference(fieldName, attrConfig->GetFieldType(),
                                    attrConfig->GetFieldConfig()->IsEnableNullField());
    }
}

bool TruncateIndexWriterCreator::NeedTruncMeta(const TruncateIndexProperty& truncateIndexProperty)
{
    return (truncateIndexProperty.GetStrategyType() == TRUNCATE_META_STRATEGY_TYPE) && truncateIndexProperty.HasSort();
}
} // namespace indexlib::index::legacy
