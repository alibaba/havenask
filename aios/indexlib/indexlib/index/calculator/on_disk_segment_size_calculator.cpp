#include <autil/StringUtil.h>
#include "indexlib/index_define.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/index_meta/segment_file_meta.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, OnDiskSegmentSizeCalculator);

OnDiskSegmentSizeCalculator::OnDiskSegmentSizeCalculator()
{
}

OnDiskSegmentSizeCalculator::~OnDiskSegmentSizeCalculator() 
{
}

int64_t OnDiskSegmentSizeCalculator::CollectSegmentSizeInfo(
    const SegmentData& segmentData, const config::IndexPartitionSchemaPtr& schema,
    SizeInfoMap& sizeInfos)
{
    assert(schema);
    if (schema->GetTableType() == tt_kkv || schema->GetTableType() == tt_kv)
    {
        return GetKeyValueSegmentSize(segmentData, schema, sizeInfos);
    }
    if (schema->GetTableType() == tt_customized)
    {
        return GetCustomTableSegmentSize(segmentData);
    }
    DirectoryPtr directory = segmentData.GetDirectory();
    if (!directory)
    {
        IE_LOG(WARN, "segment [%d] not exist", segmentData.GetSegmentId());
        return 0;
    }
    int64_t segmentSize = CollectSegmentSizeInfo(segmentData.GetDirectory(), schema, sizeInfos);
    segmentSize += CollectPatchSegmentSizeInfo(segmentData, schema, sizeInfos);
    return segmentSize;
}

int64_t OnDiskSegmentSizeCalculator::GetCustomTableSegmentSize(
    const SegmentData& segmentData) const
{
    auto segDir = segmentData.GetDirectory();
    if (!segDir)
    {
        return 0;
    }
    auto meta = segmentData.GetSegmentFileMeta();
    if (meta)
    {
        return meta->CalculateDirectorySize(CUSTOM_DATA_DIR_NAME);
    }
    DirectoryPtr dataDir = segDir->GetDirectory(CUSTOM_DATA_DIR_NAME, false);
    if (!dataDir)
    {
        IE_LOG(ERROR, "dataDir[%s] is missing in segment[%s]",
               CUSTOM_DATA_DIR_NAME.c_str(), segDir->GetPath().c_str());
        return 0;
    }
    
    fslib::FileList fileList;
    dataDir->ListFile("", fileList, true, false);
    int64_t totalLength = 0;
    for (const auto& path : fileList)
    {
        if (dataDir->IsDir(path))
        {
            continue;
        }
        totalLength += GetFileLength(dataDir, SegmentFileMetaPtr(), "", path);
    }
    return totalLength;    
}

int64_t OnDiskSegmentSizeCalculator::GetKeyValueSegmentSize(
        const SegmentData& segmentData,
        const IndexPartitionSchemaPtr& schema,
        SizeInfoMap& sizeInfos) const
{
    auto rootDir = segmentData.GetDirectory();
    if (!rootDir)
    {
        return 0;
    }

    const auto& segInfo = segmentData.GetSegmentInfo();
    auto meta = SegmentFileMeta::Create(rootDir, false);
    if (!segInfo.mergedSegment && segInfo.shardingColumnCount > 1)
    {
        int64_t totalSize = 0;
        for (size_t i = 0; i < segInfo.shardingColumnCount; ++i) {
            string columnPath = segmentData.GetShardingDirInSegment(i);
            auto columnDir = rootDir->GetDirectory(columnPath, false);
            if (!columnDir)
            {
                AUTIL_LOG(ERROR, "column dir [%s] not exist", columnPath.c_str());
                return 0;
            }
            if (schema->GetTableType() == tt_kv)
            {
                totalSize += GetKvSegmentSize(rootDir, schema->GetIndexSchema(), meta, columnPath);
            }
            else
            {
                totalSize += GetKkvSegmentSize(rootDir, schema->GetIndexSchema(), meta, columnPath);
            }
        }
        return totalSize;
    }

    if (schema->GetTableType() == tt_kv)
    {
        return GetKvSegmentSize(rootDir, schema->GetIndexSchema(), meta, "");
    }
    else
    {
        return GetKkvSegmentSize(rootDir, schema->GetIndexSchema(), meta, "");
    }
}

int64_t OnDiskSegmentSizeCalculator::GetKvSegmentSize(
        const DirectoryPtr& dir,
        const IndexSchemaPtr& indexSchema,
        const SegmentFileMetaPtr& meta,
        const std::string& path) const
{
    if (!dir)
    {
        return 0;
    }
    IndexConfigPtr kvIndexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    //DirectoryPtr indexDir = dir->GetDirectory(INDEX_DIR_NAME, false);
    string indexPath = INDEX_DIR_NAME;
    if (!path.empty())
    {
        indexPath = PathUtil::JoinPath(path, indexPath);
    }
    indexPath = PathUtil::JoinPath(indexPath, kvIndexConfig->GetIndexName());
    // if (!indexDir)
    // {
    //     return 0;
    // }
    // DirectoryPtr kvDir = indexDir->GetDirectory(kvIndexConfig->GetIndexName(), false);
    // if (!kvDir)
    // {
    //     return 0;
    // }
    return GetFileLength(dir, meta, indexPath, KV_KEY_FILE_NAME) +
        GetFileLength(dir, meta, indexPath, KV_VALUE_FILE_NAME);
}

int64_t OnDiskSegmentSizeCalculator::GetKkvSegmentSize(
        const DirectoryPtr& dir,
        const IndexSchemaPtr& indexSchema,
        const SegmentFileMetaPtr& meta,
        const std::string& path) const
{
    if (!dir)
    {
        return 0;
    }
    IndexConfigPtr kkvIndexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    string indexPath = INDEX_DIR_NAME;
    if (!path.empty())
    {
        indexPath = PathUtil::JoinPath(path, indexPath);
    }
    indexPath = PathUtil::JoinPath(indexPath, kkvIndexConfig->GetIndexName());    
    // DirectoryPtr indexDir = dir->GetDirectory(INDEX_DIR_NAME, false);
    // if (!indexDir)
    // {
    //     return 0;
    // }
    // DirectoryPtr kkvDir = indexDir->GetDirectory(kkvIndexConfig->GetIndexName(), false);
    // if (!kkvDir)
    // {
    //     return 0;
    // }
    return GetFileLength(dir, meta, indexPath, PREFIX_KEY_FILE_NAME) +
        GetFileLength(dir, meta, indexPath, SUFFIX_KEY_FILE_NAME) +
        GetFileLength(dir, meta, indexPath, KKV_VALUE_FILE_NAME);
}

int64_t OnDiskSegmentSizeCalculator::GetSegmentSize(
        const SegmentData& segmentData,
        const config::IndexPartitionSchemaPtr& schema,
        bool includePatch)
{
    SizeInfoMap sizeInfos;
    return CollectSegmentSizeInfo(segmentData, schema, sizeInfos);
}

int64_t OnDiskSegmentSizeCalculator::GetSegmentIndexSize(
        const DirectoryPtr& segDir,
        const IndexSchemaPtr& indexSchema,
        const SegmentFileMetaPtr& meta,
        SizeInfoMap& sizeInfos) const
{
    DirectoryPtr indexDirectory = segDir->GetDirectory(INDEX_DIR_NAME, false);
    if (!indexSchema || !indexDirectory)
    {
        return 0;
    }
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();

    int64_t totalSize = 0;
    int64_t singleIndexSize = 0;
    string keyPrefix = "inverted_index.";
    for (; iter != indexConfigs->End(); iter++)
    {
        const IndexConfigPtr& indexConfig = *iter;        
        IndexType type = indexConfig->GetIndexType();
        const string& indexName = indexConfig->GetIndexName();
        switch(type)
        {
        case it_pack:
        case it_expack:
            singleIndexSize = GetPackIndexSize(segDir, meta, indexName);
            break;
        case it_text:
        case it_string:
        case it_spatial:
        case it_date:
        case it_number:
        case it_number_int8:
        case it_number_uint8:
        case it_number_int16:
        case it_number_uint16:
        case it_number_int32:
        case it_number_uint32:
        case it_number_int64:
        case it_number_uint64:
            singleIndexSize = GetTextIndexSize(segDir, meta, indexName);
            break;
        case it_primarykey64:
        case it_primarykey128:
        case it_trie:
        {
            SingleFieldIndexConfigPtr pkIndexConfig = 
                DYNAMIC_POINTER_CAST(SingleFieldIndexConfig, indexConfig);
            assert(pkIndexConfig);
            singleIndexSize = GetPKIndexSize(segDir, meta, indexName,
                    pkIndexConfig->GetFieldConfig()->GetFieldName());
        }
            break;
        case it_customized:
            singleIndexSize = GetCustomizedIndexSize(segDir, meta, indexName);
            break;
        case it_range:
            singleIndexSize = GetRangeIndexSize(segDir, meta, indexName);
            break;
        default:
            assert(false);
            break;
        }
        totalSize += singleIndexSize;
        string keyName = keyPrefix + indexName;
        sizeInfos[keyName] += singleIndexSize;
    }
    // cout << "indexSize: " << totalSize << endl;
    return totalSize;
}

int64_t OnDiskSegmentSizeCalculator::GetSegmentAttributeSize(
        const DirectoryPtr& segDir,
        const AttributeSchemaPtr& attrSchema,
        const SegmentFileMetaPtr& meta,
        SizeInfoMap& sizeInfos) const
{
    DirectoryPtr directory = segDir->GetDirectory(ATTRIBUTE_DIR_NAME, false);
    if (!attrSchema || !directory)
    {
        return 0;
    }

    int64_t totalSize = 0;

    string keyPrefix = "attribute.";
    int64_t singleAttrSize = 0;
    int64_t singleAttrPatchSize = 0;
    auto attrConfigs = attrSchema->CreateIterator();
    auto iter = attrConfigs->Begin();
    for (; iter != attrConfigs->End(); iter++)
    {
        const AttributeConfigPtr& attrConfig = *iter;
        if (attrConfig->GetPackAttributeConfig())
        {
            continue;
        }
        const string& attrName = attrConfig->GetAttrName();        
        singleAttrSize  = CalculateSingleAttributeSize(
                segDir, meta, attrName);
        totalSize += singleAttrSize;
        singleAttrPatchSize = CalculateSingleAttributePatchSize(segDir, meta, attrName);
        string keyName = keyPrefix + attrName;
        sizeInfos[keyName] += singleAttrSize;
        sizeInfos[keyName + "(patch)"] += singleAttrPatchSize;
    }

    
    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    for (; packIter != packAttrConfigs->End(); packIter++)
    {
        const PackAttributeConfigPtr& packConfig = *packIter;
        string attrName = packConfig->GetAttrName();
        singleAttrSize = CalculateSingleAttributeSize(
                segDir, meta, attrName);
        totalSize += singleAttrSize;
        singleAttrPatchSize = CalculateSingleAttributePatchSize(segDir, meta, attrName);
        string keyName = keyPrefix + attrName;
        sizeInfos[keyName] += singleAttrSize;
        sizeInfos[keyName + "(patch)"] += singleAttrPatchSize;
    }
    // cout << "attributeSize: " << totalSize << endl;
    return totalSize;    
}

int64_t OnDiskSegmentSizeCalculator::CalculateSingleAttributeSize(
        const DirectoryPtr& directory,
        const SegmentFileMetaPtr& meta,
        const string& attrName) const
{
    if (!directory)
    {
        return 0;
    }
    string indexPath = PathUtil::JoinPath(ATTRIBUTE_DIR_NAME, attrName);
    return GetFileLength(directory, meta, indexPath, ATTRIBUTE_DATA_FILE_NAME)
        + GetFileLength(directory, meta, indexPath, ATTRIBUTE_OFFSET_FILE_NAME);
}

int64_t OnDiskSegmentSizeCalculator::CalculateSingleAttributePatchSize(
        const DirectoryPtr& directory,
        const SegmentFileMetaPtr& meta,
        const string& attrName) const
{
    if (!directory)
    {
        return 0;
    }
    string attrPath = PathUtil::JoinPath(ATTRIBUTE_DIR_NAME, attrName);
    fslib::FileList fileList;
    if (meta)
    {
        meta->ListFile(attrPath, fileList);
    }
    else
    {
        DirectoryPtr attrDir = directory->GetDirectory(attrPath, false);
        if (!attrDir)
        {
            return 0;
        }
        attrDir->ListFile("", fileList, false, false);
    }
    int64_t patchLength = 0;
    for (const auto& fileName : fileList)
    {
        if (StringUtil::endsWith(fileName, ATTRIBUTE_PATCH_FILE_NAME))
        {
            patchLength += GetFileLength(directory, meta, attrPath, fileName);
        }
    }
    return patchLength;
}

int64_t OnDiskSegmentSizeCalculator::GetSegmentSummarySize(
        const DirectoryPtr& segDir,
        const SummarySchemaPtr& summarySchema,
        const SegmentFileMetaPtr& meta) const
{
    // TODO
    string summaryPath = SUMMARY_DIR_NAME;
    DirectoryPtr directory = segDir->GetDirectory(SUMMARY_DIR_NAME, false);
    int64_t summarySize = GetFileLength(segDir, meta, summaryPath, SUMMARY_OFFSET_FILE_NAME) +
                          GetFileLength(segDir, meta, summaryPath, SUMMARY_DATA_FILE_NAME);
    if (directory && summarySchema)
    {
        for (summarygroupid_t groupId = 1;
             groupId < summarySchema->GetSummaryGroupConfigCount(); ++groupId)
        {
            const SummaryGroupConfigPtr& summaryGroupConfig =
                summarySchema->GetSummaryGroupConfig(groupId);
            if (summaryGroupConfig && !summaryGroupConfig->IsDefaultGroup())
            {
                string groupPath = PathUtil::JoinPath(summaryPath, summaryGroupConfig->GetGroupName());
                summarySize += GetFileLength(segDir, meta, groupPath, SUMMARY_OFFSET_FILE_NAME);
                summarySize += GetFileLength(segDir, meta, groupPath, SUMMARY_DATA_FILE_NAME);
            }
        }
    }
    // cout << "summarySize: " << summarySize << endl;
    return summarySize;
}

int64_t OnDiskSegmentSizeCalculator::GetPackIndexSize(
        const DirectoryPtr& segDir,
        const SegmentFileMetaPtr& meta,
        const string& indexName) const
    
{
    //assert(indexDirectory);
    string indexPath = PathUtil::JoinPath(INDEX_DIR_NAME, indexName);
    string sectionPath = PathUtil::JoinPath(INDEX_DIR_NAME, indexName + "_section/");
    //DirectoryPtr packIndexDir = indexDirectory->GetDirectory(indexName, false);
    //DirectoryPtr sectionDir = indexDirectory->GetDirectory(indexName + "_section/", false);

    int64_t indexSize = 0;
    indexSize += GetFileLength(segDir, meta, indexPath, POSTING_FILE_NAME);
    indexSize += GetFileLength(segDir, meta, indexPath, DICTIONARY_FILE_NAME);
    indexSize += GetFileLength(segDir, meta, indexPath, BITMAP_DICTIONARY_FILE_NAME);
    indexSize += GetFileLength(segDir, meta, indexPath, BITMAP_POSTING_FILE_NAME);

    indexSize += GetFileLength(segDir, meta, sectionPath, ATTRIBUTE_DATA_FILE_NAME);
    indexSize += GetFileLength(segDir, meta, sectionPath, ATTRIBUTE_OFFSET_FILE_NAME);
    return indexSize;
}

int64_t OnDiskSegmentSizeCalculator::GetTextIndexSize(
        const DirectoryPtr& indexDirectory,
        const SegmentFileMetaPtr& meta,
        const std::string& indexName) const
{
    assert(indexDirectory);
    //DirectoryPtr indexDir = indexDirectory->GetDirectory(indexName, false);
    string indexPath = PathUtil::JoinPath(INDEX_DIR_NAME, indexName);
    int64_t indexSize = 0;
    indexSize += GetFileLength(indexDirectory, meta, indexPath, POSTING_FILE_NAME);
    indexSize += GetFileLength(indexDirectory, meta, indexPath, DICTIONARY_FILE_NAME);
    indexSize += GetFileLength(indexDirectory, meta, indexPath, BITMAP_DICTIONARY_FILE_NAME);
    indexSize += GetFileLength(indexDirectory, meta, indexPath, BITMAP_POSTING_FILE_NAME);
    return indexSize;
}

int64_t OnDiskSegmentSizeCalculator::GetRangeIndexSize(
        const DirectoryPtr& segDir,
        const SegmentFileMetaPtr& meta,
        const std::string& indexName) const
{
    // DirectoryPtr indexDir = indexDirectory->GetDirectory(indexName, false);
    // if (!indexDir)
    // {
    //     return 0;
    // }
    int64_t indexSize = 0;
    string bottomIndexPath = PathUtil::JoinPath(indexName,
            RangeIndexConfig::GetBottomLevelIndexName(indexName));
    string highIndexPath = PathUtil::JoinPath(indexName,
            RangeIndexConfig::GetHighLevelIndexName(indexName));
    indexSize += GetTextIndexSize(
            segDir, meta, bottomIndexPath);
    indexSize += GetTextIndexSize(
            segDir, meta, highIndexPath);
    return indexSize;
}

int64_t OnDiskSegmentSizeCalculator::GetCustomizedIndexSize(
        const DirectoryPtr& segDir,
        const SegmentFileMetaPtr& meta,
        const std::string& indexName) const
{
    if (!segDir)
    {
        return 0;
    }
    string indexPath = PathUtil::JoinPath(INDEX_DIR_NAME, indexName);
    if (meta)
    {
        return meta->CalculateDirectorySize(indexPath);
    }
    auto indexDirectory = segDir->GetDirectory(INDEX_DIR_NAME, false);
    auto indexDir = indexDirectory->GetDirectory(indexName, false);
    if (!indexDir)
    {
        return 0;
    }
    int64_t indexSize = 0;
    fslib::FileList fileList;
    indexDir->ListFile("", fileList, true, false);

    for (const auto& fileName : fileList)
    {
        indexSize += GetFileLength(indexDir, SegmentFileMetaPtr(), "",fileName);
    }
    return indexSize;
}

int64_t OnDiskSegmentSizeCalculator::GetPKIndexSize(
        const DirectoryPtr& segDir,
        const SegmentFileMetaPtr& meta,
        const std::string& indexName,
        const std::string& fieldName) const
{
    assert(segDir);
    int64_t indexSize = 0;
    string indexPath = PathUtil::JoinPath(INDEX_DIR_NAME, indexName);
    string attrPath = PathUtil::JoinPath(indexPath, string(PK_ATTRIBUTE_DIR_NAME_PREFIX) + '_' + fieldName);
    indexSize += GetFileLength(segDir, meta, indexPath, PRIMARY_KEY_DATA_FILE_NAME);
    indexSize += GetFileLength(segDir, meta, attrPath,  ATTRIBUTE_DATA_FILE_NAME);
    return indexSize;
}

// int64_t OnDiskSegmentSizeCalculator::GetFileLength(
//         const DirectoryPtr& directory, const string& fileName)
// {
//     if (!directory || !directory->IsExist(fileName))
//     {
//         return 0;
//     }
//     return directory->GetFileLength(fileName);
// }

int64_t OnDiskSegmentSizeCalculator::GetFileLength(
        const DirectoryPtr& directory,
        const SegmentFileMetaPtr& meta,
        const string& indexPath,
        const string& fileName) const
{
    string filePath = fileName;
    if (!indexPath.empty())
    {
        filePath = PathUtil::JoinPath(indexPath, fileName);
    }
    if (meta)
    {
        return meta->GetFileLength(filePath);
    }

    if (!directory || !directory->IsExist(filePath))
    {
        return 0;
    }
    return directory->GetFileLength(filePath);
}


int64_t OnDiskSegmentSizeCalculator::CollectSegmentSizeInfo(
        const DirectoryPtr& segDir, const IndexPartitionSchemaPtr& schema,
        SizeInfoMap& sizeInfos) const
{
    int64_t segmentSize = 0;
    auto segmentFileMeta = SegmentFileMeta::Create(segDir, false);
    segmentSize += GetSegmentIndexSize(segDir, schema->GetIndexSchema(), segmentFileMeta, sizeInfos);
    segmentSize += GetSegmentAttributeSize(segDir, schema->GetAttributeSchema(), segmentFileMeta, sizeInfos);
    int64_t summarySize = GetSegmentSummarySize(segDir, schema->GetSummarySchema(), segmentFileMeta);
    segmentSize += summarySize;
    sizeInfos["summary"] += summarySize;
    // segmentFileMeta->TEST_Print();
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        DirectoryPtr subSegDir;
        if (segDir)
        {
            subSegDir = segDir->GetDirectory(SUB_SEGMENT_DIR_NAME, false);
        }
        if (subSegDir)
        {
            SegmentFileMetaPtr subSegFileMeta = SegmentFileMeta::Create(segDir, true);
            segmentSize += GetSegmentIndexSize(subSegDir, subSchema->GetIndexSchema(), subSegFileMeta, sizeInfos);
            segmentSize += GetSegmentAttributeSize(subSegDir, subSchema->GetAttributeSchema(),
                    subSegFileMeta, sizeInfos);
            summarySize = GetSegmentSummarySize(subSegDir, subSchema->GetSummarySchema(), subSegFileMeta);
            // subSegFileMeta->TEST_Print();
            segmentSize += summarySize;
            sizeInfos["sub_summary"] += summarySize; 
        }
    }
    return segmentSize;
}

int64_t OnDiskSegmentSizeCalculator::CollectPatchSegmentSizeInfo(
        const SegmentData& segmentData, const IndexPartitionSchemaPtr& schema,
        SizeInfoMap& sizeInfos) const
{
    const PartitionPatchIndexAccessorPtr& patchAccessor = segmentData.GetPatchIndexAccessor();
    if (!patchAccessor)
    {
        return 0;
    }
    int64_t segmentSize = 0;
    const PartitionPatchMeta& patchMeta = patchAccessor->GetPartitionPatchMeta();
    DirectoryPtr rootDir = patchAccessor->GetRootDirectory();
    const Version& version = patchAccessor->GetVersion();
    PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
    while (metaIter.HasNext())
    {
        SchemaPatchInfoPtr schemaInfo = metaIter.Next();
        assert(schemaInfo);
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++)
        {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            if (segMeta.GetSegmentId() != segmentData.GetSegmentId())
            {
                continue;
            }
            string patchSegPath = FileSystemWrapper::JoinPath(
                    PartitionPatchIndexAccessor::GetPatchRootDirName(schemaInfo->GetSchemaId()),
                    version.GetSegmentDirName(segMeta.GetSegmentId()));
            DirectoryPtr patchSegDir = rootDir->GetDirectory(patchSegPath, false);
            if (!patchSegDir)
            {
                IE_LOG(WARN, "patch segment path [%s] not exist",
                       FileSystemWrapper::JoinPath(rootDir->GetPath(), patchSegPath).c_str());
            }
            else
            {
                segmentSize += CollectSegmentSizeInfo(patchSegDir, schema, sizeInfos);
            }
        }
    }
    IE_LOG(DEBUG, "total size: %ld of segment [%d] in patch_index dir",
           segmentSize, segmentData.GetSegmentId());
    return segmentSize;
}    

IE_NAMESPACE_END(index);

