#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_common.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_meta_reader.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_vector_allocator.h"
#include "indexlib/index/normal/inverted_index/truncate/multi_truncate_index_writer.h"
#include "indexlib/index/normal/inverted_index/truncate/single_truncate_index_writer.h"
#include "indexlib/index/normal/inverted_index/truncate/time_strategy_truncate_meta_reader.h"
#include "indexlib/index/normal/inverted_index/truncate/evaluator_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_collector_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_trigger_creator.h"
#include "indexlib/index_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/archive_folder.h"
#include "indexlib/storage/archive_file.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TruncateIndexWriterCreator);

TruncateIndexWriterCreator::TruncateIndexWriterCreator(bool optimize)
    : mIsOptimize(optimize)
{
}

TruncateIndexWriterCreator::~TruncateIndexWriterCreator() 
{
}

void TruncateIndexWriterCreator::Init(
        const IndexPartitionSchemaPtr& schema, 
        const MergeConfig& mergeConfig,
        const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
        const ReclaimMapPtr& reclaimMap,
        const storage::ArchiveFolderPtr& metaRootFolder,
        TruncateAttributeReaderCreator *truncateAttrCreator,
        const BucketMaps *bucketMaps,
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
    lts.tm_min  = 0;
    lts.tm_sec  = 0;

    return mktime(&lts);
}

TruncateIndexWriterPtr TruncateIndexWriterCreator::Create(
        const config::IndexConfigPtr& indexConfig,
        const config::MergeIOConfig& ioConfig)
{
    assert(indexConfig);
    if (indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
    {
        return TruncateIndexWriterPtr();
    }
    
    const string &indexName = indexConfig->GetIndexName();
    if (!mTruncOptionConfig->IsTruncateIndex(indexName))
    {
        return TruncateIndexWriterPtr();
    }

    const TruncateIndexConfig &truncIndexConfig =
        mTruncOptionConfig->GetTruncateIndexConfig(indexName);
    const TruncateIndexPropertyVector& truncIndexProperties =
        truncIndexConfig.GetTruncateIndexProperties();
    assert(truncIndexProperties.size() > 0);

    MultiTruncateIndexWriterPtr multiIndexWriter(new MultiTruncateIndexWriter());
    BucketVectorAllocatorPtr bucketVecAllocator(new BucketVectorAllocator());
    for (size_t i = 0; i < truncIndexProperties.size(); ++i)
    {
        TruncateIndexWriterPtr indexWriter = CreateSingleIndexWriter(
                truncIndexProperties[i], indexConfig, bucketVecAllocator, ioConfig);
        if (indexWriter)
        {
            multiIndexWriter->AddIndexWriter(indexWriter);
        }
    }
    return multiIndexWriter;
}

TruncateMetaReaderPtr TruncateIndexWriterCreator::CreateMetaReader(
        const TruncateIndexProperty& truncateIndexProperty, 
        const FileWrapperPtr& metaFile)
{
    TruncateMetaReaderPtr metaReader;
    if (!NeedTruncMeta(truncateIndexProperty))
    {
        if (metaFile) {
            metaFile->Close();
        }
        return metaReader;
    }

    bool desc = truncateIndexProperty.mTruncateProfile->mSortParams[0].IsDesc();
    if (!truncateIndexProperty.IsFilterByTimeStamp())
    {
        metaReader.reset(new TruncateMetaReader(desc));
    }
    else
    {
        const DiversityConstrain& constrain = 
            truncateIndexProperty.mTruncateStrategy->GetDiversityConstrain();
        metaReader.reset(new TimeStrategyTruncateMetaReader(
                        constrain.GetFilterMinValue(), 
                        constrain.GetFilterMaxValue(), desc));
    }
    metaReader->Open(metaFile);
    return metaReader;
}

bool TruncateIndexWriterCreator::CheckBitmapConfigValid(
        const IndexConfigPtr& indexConfig,
        const TruncateIndexProperty& truncateIndexProperty)
{
    if (indexConfig->GetHighFrequencyTermPostingType() == hp_bitmap
        && indexConfig->GetDictConfig()
        && indexConfig->GetTruncateTermVocabulary() 
        && truncateIndexProperty.GetStrategyType() != TRUNCATE_META_STRATEGY_TYPE)
    {
        return false;
    }
    return true;
}

TruncateIndexWriterPtr TruncateIndexWriterCreator::CreateSingleIndexWriter(
        const TruncateIndexProperty& truncateIndexProperty, 
        const IndexConfigPtr& indexConfig,
        const BucketVectorAllocatorPtr &bucketVecAllocator,
        const config::MergeIOConfig& ioConfig)
{
    if (!CheckBitmapConfigValid(indexConfig, truncateIndexProperty))
    {
        const string& indexName = indexConfig->GetIndexName();
        IE_LOG(ERROR, "hp_bitmap typed index [%s] should use truncate_meta"
               " Strategy when already has truncate term vocabulary!", indexName.c_str());
        return TruncateIndexWriterPtr();
    }

    DocInfoAllocatorPtr allocator = 
        CreateDocInfoAllocator(truncateIndexProperty);
    if (!allocator)
    {
        return TruncateIndexWriterPtr();
    }
    FileWrapperPtr truncateMetaFile = mTruncateMetaFolder->GetInnerFile(
            truncateIndexProperty.mTruncateIndexName, fslib::READ);
    // string truncateMetaFile
    //     = FileSystemWrapper::JoinPath(mTruncateMetaDir, truncateIndexProperty.mTruncateIndexName);
    TruncateMetaReaderPtr metaReader = CreateMetaReader(truncateIndexProperty,
            truncateMetaFile);
    DocCollectorPtr docCollector = DocCollectorCreator::Create(
            truncateIndexProperty, allocator, *mBucketMaps,
            mTruncateAttrCreator, bucketVecAllocator, metaReader);

    TruncateTriggerPtr truncateTrigger = TruncateTriggerCreator::Create(
            truncateIndexProperty);
    truncateTrigger->SetTruncateMetaReader(metaReader);

    EvaluatorPtr evaluator = EvaluatorCreator::Create(
            truncateIndexProperty, mTruncateAttrCreator, allocator);


    SingleTruncateIndexWriterPtr writer(new SingleTruncateIndexWriter(indexConfig, mReclaimMap));
    writer->Init(
            evaluator, DocCollectorPtr(docCollector), truncateTrigger, truncateIndexProperty.mTruncateIndexName, allocator, ioConfig);
    
    if (truncateIndexProperty.HasSort())
    {
        FileWrapperPtr fileForWrite;
        if (mIsOptimize)
        {
            string filePath = 
                FileSystemWrapper::JoinPath(mTruncateMetaFolder->GetFolderPath(),
                        truncateIndexProperty.mTruncateIndexName);

            FileSystemWrapper::DeleteIfExist(filePath);//for legacy code
            fileForWrite = mTruncateMetaFolder->GetInnerFile(
                    truncateIndexProperty.mTruncateIndexName, fslib::WRITE);
            writer->SetTruncateIndexMetaInfo(fileForWrite,
                    truncateIndexProperty.mTruncateProfile
                    ->mSortParams[0].GetSortField());

        } else {
            if (!truncateMetaFile)
            {
                fileForWrite = mTruncateMetaFolder->GetInnerFile(
                        truncateIndexProperty.mTruncateIndexName, fslib::WRITE);
                writer->SetTruncateIndexMetaInfo(fileForWrite,
                        truncateIndexProperty.mTruncateProfile
                        ->mSortParams[0].GetSortField());
            }
        }
    }
    return writer;
}

bool TruncateIndexWriterCreator::CheckReferenceField(const std::string &fieldName)
{
    if (fieldName == DOC_PAYLOAD_FIELD_NAME)
    {
        return true;
    }
    const AttributeSchemaPtr &attrSchema = mTruncateAttrCreator->GetAttributeSchema();
    if (!attrSchema->GetAttributeConfig(fieldName))
    {
        INDEXLIB_FATAL_ERROR(NonExist, "fieldName[%s] does not exist", 
			     fieldName.c_str());
    }
    return true;
}

bool TruncateIndexWriterCreator::GetReferenceFields(
        vector<string> &fieldNames,
        const TruncateIndexProperty& truncateIndexProperty)
{
    const DiversityConstrain& constrain = 
        truncateIndexProperty.mTruncateStrategy->GetDiversityConstrain();

    if (truncateIndexProperty.HasSort())
    {
        size_t sortDimenNum = truncateIndexProperty.GetSortDimenNum();
        for(size_t i = 0; i < sortDimenNum; ++i)
        {
            const string& fieldName = truncateIndexProperty.mTruncateProfile
                    ->mSortParams[i].GetSortField();
            if (!CheckReferenceField(fieldName))
            {
                return false;
            }
            fieldNames.push_back(fieldName);
        }
    }

    if (constrain.NeedFilter())
    {
        const string& fieldName = constrain.GetFilterField();
        if (!CheckReferenceField(fieldName))
        {
            return false;
        }
        fieldNames.push_back(fieldName);
    }

    if (constrain.NeedDistinct())
    {
        const string& fieldName = constrain.GetDistField();
        if (!CheckReferenceField(fieldName))
        {
            return false;
        }
        fieldNames.push_back(fieldName);
    }
    return true;
}

DocInfoAllocatorPtr TruncateIndexWriterCreator::CreateDocInfoAllocator(
        const TruncateIndexProperty& truncateIndexProperty)
{
    assert(mAllocators.find(truncateIndexProperty.mTruncateIndexName) 
           == mAllocators.end());

    vector<string> referFields;
    if (!GetReferenceFields(referFields, truncateIndexProperty))
    {
        return DocInfoAllocatorPtr();
    }

    DocInfoAllocatorPtr allocator(new DocInfoAllocator());
    mAllocators[truncateIndexProperty.mTruncateIndexName] = allocator;

    for (size_t i = 0; i < referFields.size(); ++i)
    {
        DeclareReference(referFields[i], allocator);
    }
    return allocator;
}

void TruncateIndexWriterCreator::DeclareReference(
        const string& fieldName, const DocInfoAllocatorPtr& allocator)
{
    if (fieldName == DOC_PAYLOAD_FIELD_NAME)
    {
        allocator->DeclareReference(fieldName, ft_uint16);
    }
    else
    {
        const AttributeSchemaPtr &attrSchema = mTruncateAttrCreator->GetAttributeSchema();
        const AttributeConfigPtr &attrConfig = attrSchema->GetAttributeConfig(fieldName);
        if (!attrConfig)
        {
            INDEXLIB_FATAL_ERROR(NonExist, "fieldName[%s] does not exist", 
                    fieldName.c_str());
        }
        allocator->DeclareReference(fieldName, attrConfig->GetFieldType());
    }
}

bool TruncateIndexWriterCreator::NeedTruncMeta(
        const TruncateIndexProperty& truncateIndexProperty)
{
    return (truncateIndexProperty.GetStrategyType() == TRUNCATE_META_STRATEGY_TYPE)
        && truncateIndexProperty.HasSort();    
}

IE_NAMESPACE_END(index);
