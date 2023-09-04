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
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"

#include "autil/StringUtil.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/source/source_define.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, OnDiskSegmentSizeCalculator);

OnDiskSegmentSizeCalculator::OnDiskSegmentSizeCalculator() {}

OnDiskSegmentSizeCalculator::~OnDiskSegmentSizeCalculator() {}

int64_t OnDiskSegmentSizeCalculator::CollectSegmentSizeInfo(const SegmentData& segmentData,
                                                            const config::IndexPartitionSchemaPtr& schema,
                                                            SizeInfoMap& sizeInfos)
{
    assert(schema);
    if (schema->GetTableType() == tt_kkv || schema->GetTableType() == tt_kv) {
        return GetKeyValueSegmentSize(segmentData, schema, sizeInfos);
    }
    if (schema->GetTableType() == tt_customized) {
        return GetCustomTableSegmentSize(segmentData);
    }
    DirectoryPtr directory = segmentData.GetDirectory();
    if (!directory) {
        IE_LOG(WARN, "segment [%d] not exist", segmentData.GetSegmentId());
        return 0;
    }
    int64_t segmentSize = CollectSegmentSizeInfo(segmentData.GetDirectory(), schema, sizeInfos);
    segmentSize += CollectPatchSegmentSizeInfo(segmentData, schema, sizeInfos);
    return segmentSize;
}

int64_t OnDiskSegmentSizeCalculator::GetCustomTableSegmentSize(const SegmentData& segmentData) const
{
    auto segDir = segmentData.GetDirectory();
    if (!segDir) {
        return 0;
    }
    DirectoryPtr dataDir = segDir->GetDirectory(CUSTOM_DATA_DIR_NAME, false);
    if (!dataDir) {
        IE_LOG(ERROR, "dataDir[%s] is missing in segment[%s]", CUSTOM_DATA_DIR_NAME.c_str(),
               segDir->DebugString().c_str());
        return 0;
    }

    fslib::FileList fileList;
    dataDir->ListDir("", fileList, true);
    int64_t totalLength = 0;
    for (const auto& path : fileList) {
        if (dataDir->IsDir(path)) {
            continue;
        }
        totalLength += GetFileLength(dataDir, "", path);
    }
    return totalLength;
}

int64_t OnDiskSegmentSizeCalculator::GetKeyValueSegmentSize(const SegmentData& segmentData,
                                                            const IndexPartitionSchemaPtr& schema,
                                                            SizeInfoMap& sizeInfos) const
{
    auto rootDir = segmentData.GetDirectory();
    if (!rootDir) {
        return 0;
    }

    const auto& segInfo = segmentData.GetSegmentInfo();
    if (!segInfo->mergedSegment && segInfo->shardCount > 1) {
        int64_t totalSize = 0;
        for (size_t i = 0; i < segInfo->shardCount; ++i) {
            string columnPath = segmentData.GetShardingDirInSegment(i);
            auto columnDir = rootDir->GetDirectory(columnPath, false);
            if (!columnDir) {
                AUTIL_LOG(ERROR, "column dir [%s] not exist", columnPath.c_str());
                return 0;
            }
            if (schema->GetTableType() == tt_kv) {
                totalSize += GetKvSegmentSize(rootDir, schema->GetIndexSchema(), columnPath);
            } else {
                totalSize += GetKkvSegmentSize(rootDir, schema->GetIndexSchema(), columnPath);
            }
        }
        return totalSize;
    }

    if (schema->GetTableType() == tt_kv) {
        return GetKvSegmentSize(rootDir, schema->GetIndexSchema(), "");
    } else {
        return GetKkvSegmentSize(rootDir, schema->GetIndexSchema(), "");
    }
}

int64_t OnDiskSegmentSizeCalculator::GetKvSegmentSize(const DirectoryPtr& dir, const IndexSchemaPtr& indexSchema,
                                                      const std::string& path) const
{
    if (!dir) {
        return 0;
    }
    IndexConfigPtr kvIndexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    // DirectoryPtr indexDir = dir->GetDirectory(INDEX_DIR_NAME, false);
    string indexPath = INDEX_DIR_NAME;
    if (!path.empty()) {
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
    return GetFileLength(dir, indexPath, KV_KEY_FILE_NAME) + GetFileLength(dir, indexPath, KV_VALUE_FILE_NAME);
}

int64_t OnDiskSegmentSizeCalculator::GetKkvSegmentSize(const DirectoryPtr& dir, const IndexSchemaPtr& indexSchema,
                                                       const std::string& path) const
{
    if (!dir) {
        return 0;
    }
    IndexConfigPtr kkvIndexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    string indexPath = INDEX_DIR_NAME;
    if (!path.empty()) {
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
    return GetFileLength(dir, indexPath, PREFIX_KEY_FILE_NAME) + GetFileLength(dir, indexPath, SUFFIX_KEY_FILE_NAME) +
           GetFileLength(dir, indexPath, KKV_VALUE_FILE_NAME);
}

int64_t OnDiskSegmentSizeCalculator::GetSegmentSize(const SegmentData& segmentData,
                                                    const config::IndexPartitionSchemaPtr& schema, bool includePatch)
{
    SizeInfoMap sizeInfos;
    return CollectSegmentSizeInfo(segmentData, schema, sizeInfos);
}

int64_t OnDiskSegmentSizeCalculator::GetSegmentIndexSize(const DirectoryPtr& segDir, const IndexSchemaPtr& indexSchema,
                                                         SizeInfoMap& sizeInfos) const
{
    DirectoryPtr indexDirectory = segDir->GetDirectory(INDEX_DIR_NAME, false);
    if (!indexSchema || !indexDirectory) {
        return 0;
    }
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();

    int64_t totalSize = 0;
    int64_t singleIndexSize = 0;
    string keyPrefix = "inverted_index.";
    for (; iter != indexConfigs->End(); iter++) {
        const IndexConfigPtr& indexConfig = *iter;
        InvertedIndexType type = indexConfig->GetInvertedIndexType();
        const string& indexName = indexConfig->GetIndexName();
        switch (type) {
        case it_pack:
        case it_expack:
            singleIndexSize = GetPackIndexSize(segDir, indexName);
            break;
        case it_text:
        case it_string:
        case it_spatial:
        case it_datetime:
        case it_number:
        case it_number_int8:
        case it_number_uint8:
        case it_number_int16:
        case it_number_uint16:
        case it_number_int32:
        case it_number_uint32:
        case it_number_int64:
        case it_number_uint64:
            singleIndexSize = GetTextIndexSize(segDir, indexName);
            break;
        case it_primarykey64:
        case it_primarykey128:
        case it_trie: {
            SingleFieldIndexConfigPtr pkIndexConfig = DYNAMIC_POINTER_CAST(SingleFieldIndexConfig, indexConfig);
            assert(pkIndexConfig);
            singleIndexSize = GetPKIndexSize(segDir, indexName, pkIndexConfig->GetFieldConfig()->GetFieldName());
        } break;
        case it_customized:
            singleIndexSize = GetCustomizedIndexSize(segDir, indexName);
            break;
        case it_range:
            singleIndexSize = GetRangeIndexSize(segDir, indexName);
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

int64_t OnDiskSegmentSizeCalculator::GetSegmentAttributeSize(const DirectoryPtr& segDir,
                                                             const AttributeSchemaPtr& attrSchema,
                                                             SizeInfoMap& sizeInfos) const
{
    DirectoryPtr directory = segDir->GetDirectory(ATTRIBUTE_DIR_NAME, false);
    if (!attrSchema || !directory) {
        return 0;
    }

    int64_t totalSize = 0;

    string keyPrefix = "attribute.";
    int64_t singleAttrSize = 0;
    int64_t singleAttrPatchSize = 0;
    auto attrConfigs = attrSchema->CreateIterator();
    auto iter = attrConfigs->Begin();
    for (; iter != attrConfigs->End(); iter++) {
        const AttributeConfigPtr& attrConfig = *iter;
        if (attrConfig->GetPackAttributeConfig()) {
            continue;
        }
        const string& attrName = attrConfig->GetAttrName();
        singleAttrSize = CalculateSingleAttributeSize(segDir, attrName);
        totalSize += singleAttrSize;
        singleAttrPatchSize = CalculateSingleAttributePatchSize(segDir, attrName);
        string keyName = keyPrefix + attrName;
        sizeInfos[keyName] += singleAttrSize;
        sizeInfos[keyName + "(patch)"] += singleAttrPatchSize;
    }

    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    for (; packIter != packAttrConfigs->End(); packIter++) {
        const PackAttributeConfigPtr& packConfig = *packIter;
        string attrName = packConfig->GetPackName();
        singleAttrSize = CalculateSingleAttributeSize(segDir, attrName);
        totalSize += singleAttrSize;
        singleAttrPatchSize = CalculateSingleAttributePatchSize(segDir, attrName);
        string keyName = keyPrefix + attrName;
        sizeInfos[keyName] += singleAttrSize;
        sizeInfos[keyName + "(patch)"] += singleAttrPatchSize;
    }
    // cout << "attributeSize: " << totalSize << endl;
    return totalSize;
}

int64_t OnDiskSegmentSizeCalculator::CalculateSingleAttributeSize(const DirectoryPtr& directory,
                                                                  const string& attrName) const
{
    if (!directory) {
        return 0;
    }
    string indexPath = PathUtil::JoinPath(ATTRIBUTE_DIR_NAME, attrName);
    return GetFileLength(directory, indexPath, ATTRIBUTE_DATA_FILE_NAME) +
           GetFileLength(directory, indexPath, ATTRIBUTE_OFFSET_FILE_NAME);
}

int64_t OnDiskSegmentSizeCalculator::CalculateSingleAttributePatchSize(const DirectoryPtr& directory,
                                                                       const string& attrName) const
{
    if (!directory) {
        return 0;
    }
    string attrPath = PathUtil::JoinPath(ATTRIBUTE_DIR_NAME, attrName);
    fslib::FileList fileList;
    DirectoryPtr attrDir = directory->GetDirectory(attrPath, false);
    if (!attrDir) {
        return 0;
    }
    attrDir->ListDir("", fileList, false);
    int64_t patchLength = 0;
    for (const auto& fileName : fileList) {
        if (StringUtil::endsWith(fileName, PATCH_FILE_NAME)) {
            patchLength += GetFileLength(directory, attrPath, fileName);
        }
    }
    return patchLength;
}

int64_t OnDiskSegmentSizeCalculator::GetSegmentSummarySize(const DirectoryPtr& segDir,
                                                           const SummarySchemaPtr& summarySchema) const
{
    // TODO
    string summaryPath = SUMMARY_DIR_NAME;
    DirectoryPtr directory = segDir->GetDirectory(SUMMARY_DIR_NAME, false);
    int64_t summarySize = GetFileLength(segDir, summaryPath, SUMMARY_OFFSET_FILE_NAME) +
                          GetFileLength(segDir, summaryPath, SUMMARY_DATA_FILE_NAME);
    if (directory && summarySchema) {
        for (summarygroupid_t groupId = 1; groupId < summarySchema->GetSummaryGroupConfigCount(); ++groupId) {
            const SummaryGroupConfigPtr& summaryGroupConfig = summarySchema->GetSummaryGroupConfig(groupId);
            if (summaryGroupConfig && !summaryGroupConfig->IsDefaultGroup()) {
                string groupPath = PathUtil::JoinPath(summaryPath, summaryGroupConfig->GetGroupName());
                summarySize += GetFileLength(segDir, groupPath, SUMMARY_OFFSET_FILE_NAME);
                summarySize += GetFileLength(segDir, groupPath, SUMMARY_DATA_FILE_NAME);
            }
        }
    }
    // cout << "summarySize: " << summarySize << endl;
    return summarySize;
}

int64_t OnDiskSegmentSizeCalculator::GetSegmentSourceSize(const DirectoryPtr& segDir,
                                                          const SourceSchemaPtr& sourceSchema) const
{
    if (!sourceSchema) {
        return 0;
    }
    if (!segDir->GetDirectory(SOURCE_DIR_NAME, false)) {
        return 0;
    }

    string metaDir = FslibWrapper::JoinPath(SOURCE_DIR_NAME, SOURCE_META_DIR);
    int64_t sourceSize =
        GetFileLength(segDir, metaDir, SOURCE_DATA_FILE_NAME) + GetFileLength(segDir, metaDir, SOURCE_OFFSET_FILE_NAME);
    for (auto iter = sourceSchema->Begin(); iter != sourceSchema->End(); iter++) {
        groupid_t groupId = (*iter)->GetGroupId();
        string groupDir = FslibWrapper::JoinPath(SOURCE_DIR_NAME, SourceDefine::GetDataDir(groupId));
        sourceSize = sourceSize + GetFileLength(segDir, groupDir, SOURCE_DATA_FILE_NAME) +
                     GetFileLength(segDir, groupDir, SOURCE_OFFSET_FILE_NAME);
    }

    return sourceSize;
}

int64_t OnDiskSegmentSizeCalculator::GetPackIndexSize(const DirectoryPtr& segDir, const string& indexName) const

{
    // assert(indexDirectory);
    string indexPath = PathUtil::JoinPath(INDEX_DIR_NAME, indexName);
    string sectionPath = PathUtil::JoinPath(INDEX_DIR_NAME, indexName + "_section/");
    // DirectoryPtr packIndexDir = indexDirectory->GetDirectory(indexName, false);
    // DirectoryPtr sectionDir = indexDirectory->GetDirectory(indexName + "_section/", false);

    int64_t indexSize = 0;
    indexSize += GetFileLength(segDir, indexPath, POSTING_FILE_NAME);
    indexSize += GetFileLength(segDir, indexPath, DICTIONARY_FILE_NAME);
    indexSize += GetFileLength(segDir, indexPath, BITMAP_DICTIONARY_FILE_NAME);
    indexSize += GetFileLength(segDir, indexPath, BITMAP_POSTING_FILE_NAME);

    indexSize += GetFileLength(segDir, sectionPath, ATTRIBUTE_DATA_FILE_NAME);
    indexSize += GetFileLength(segDir, sectionPath, ATTRIBUTE_OFFSET_FILE_NAME);
    return indexSize;
}

int64_t OnDiskSegmentSizeCalculator::GetTextIndexSize(const DirectoryPtr& indexDirectory,
                                                      const std::string& indexName) const
{
    assert(indexDirectory);
    // DirectoryPtr indexDir = indexDirectory->GetDirectory(indexName, false);
    string indexPath = PathUtil::JoinPath(INDEX_DIR_NAME, indexName);
    int64_t indexSize = 0;
    indexSize += GetFileLength(indexDirectory, indexPath, POSTING_FILE_NAME);
    indexSize += GetFileLength(indexDirectory, indexPath, DICTIONARY_FILE_NAME);
    indexSize += GetFileLength(indexDirectory, indexPath, BITMAP_DICTIONARY_FILE_NAME);
    indexSize += GetFileLength(indexDirectory, indexPath, BITMAP_POSTING_FILE_NAME);
    return indexSize;
}

int64_t OnDiskSegmentSizeCalculator::GetRangeIndexSize(const DirectoryPtr& segDir, const std::string& indexName) const
{
    // DirectoryPtr indexDir = indexDirectory->GetDirectory(indexName, false);
    // if (!indexDir)
    // {
    //     return 0;
    // }
    int64_t indexSize = 0;
    string bottomIndexPath = PathUtil::JoinPath(indexName, RangeIndexConfig::GetBottomLevelIndexName(indexName));
    string highIndexPath = PathUtil::JoinPath(indexName, RangeIndexConfig::GetHighLevelIndexName(indexName));
    indexSize += GetTextIndexSize(segDir, bottomIndexPath);
    indexSize += GetTextIndexSize(segDir, highIndexPath);
    return indexSize;
}

int64_t OnDiskSegmentSizeCalculator::GetCustomizedIndexSize(const DirectoryPtr& segDir,
                                                            const std::string& indexName) const
{
    if (!segDir) {
        return 0;
    }
    string indexPath = PathUtil::JoinPath(INDEX_DIR_NAME, indexName);
    auto indexDirectory = segDir->GetDirectory(INDEX_DIR_NAME, false);
    auto indexDir = indexDirectory->GetDirectory(indexName, false);
    if (!indexDir) {
        return 0;
    }
    int64_t indexSize = 0;
    fslib::FileList fileList;
    indexDir->ListDir("", fileList, true);

    for (const auto& fileName : fileList) {
        indexSize += GetFileLength(indexDir, "", fileName);
    }
    return indexSize;
}

int64_t OnDiskSegmentSizeCalculator::GetPKIndexSize(const DirectoryPtr& segDir, const std::string& indexName,
                                                    const std::string& fieldName) const
{
    assert(segDir);
    int64_t indexSize = 0;
    string indexPath = PathUtil::JoinPath(INDEX_DIR_NAME, indexName);
    string attrPath = PathUtil::JoinPath(indexPath, string(PK_ATTRIBUTE_DIR_NAME_PREFIX) + '_' + fieldName);
    indexSize += GetFileLength(segDir, indexPath, PRIMARY_KEY_DATA_FILE_NAME);
    indexSize += GetFileLength(segDir, attrPath, ATTRIBUTE_DATA_FILE_NAME);
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

int64_t OnDiskSegmentSizeCalculator::GetFileLength(const DirectoryPtr& directory, const string& indexPath,
                                                   const string& fileName) const
{
    string filePath = fileName;
    if (!indexPath.empty()) {
        filePath = PathUtil::JoinPath(indexPath, fileName);
    }

    if (!directory || !directory->IsExist(filePath)) {
        return 0;
    }
    return directory->GetFileLength(filePath);
}

int64_t OnDiskSegmentSizeCalculator::CollectSegmentSizeInfo(const DirectoryPtr& segDir,
                                                            const IndexPartitionSchemaPtr& schema,
                                                            SizeInfoMap& sizeInfos) const
{
    int64_t segmentSize = 0;
    segmentSize += GetSegmentIndexSize(segDir, schema->GetIndexSchema(), sizeInfos);
    segmentSize += GetSegmentAttributeSize(segDir, schema->GetAttributeSchema(), sizeInfos);
    int64_t summarySize = GetSegmentSummarySize(segDir, schema->GetSummarySchema());
    segmentSize += summarySize;
    sizeInfos["summary"] += summarySize;
    int64_t sourceSize = GetSegmentSourceSize(segDir, schema->GetSourceSchema());
    sizeInfos["source"] += sourceSize;
    segmentSize += sourceSize;
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        DirectoryPtr subSegDir;
        if (segDir) {
            subSegDir = segDir->GetDirectory(SUB_SEGMENT_DIR_NAME, false);
        }
        if (subSegDir) {
            segmentSize += GetSegmentIndexSize(subSegDir, subSchema->GetIndexSchema(), sizeInfos);
            segmentSize += GetSegmentAttributeSize(subSegDir, subSchema->GetAttributeSchema(), sizeInfos);
            summarySize = GetSegmentSummarySize(subSegDir, subSchema->GetSummarySchema());
            segmentSize += summarySize;
            sizeInfos["sub_summary"] += summarySize;
        }
    }
    return segmentSize;
}

int64_t OnDiskSegmentSizeCalculator::CollectPatchSegmentSizeInfo(const SegmentData& segmentData,
                                                                 const IndexPartitionSchemaPtr& schema,
                                                                 SizeInfoMap& sizeInfos) const
{
    const PartitionPatchIndexAccessorPtr& patchAccessor = segmentData.GetPatchIndexAccessor();
    if (!patchAccessor) {
        return 0;
    }
    int64_t segmentSize = 0;
    const PartitionPatchMeta& patchMeta = patchAccessor->GetPartitionPatchMeta();
    DirectoryPtr rootDir = patchAccessor->GetRootDirectory();
    const Version& version = patchAccessor->GetVersion();
    PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
    while (metaIter.HasNext()) {
        SchemaPatchInfoPtr schemaInfo = metaIter.Next();
        assert(schemaInfo);
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++) {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            if (segMeta.GetSegmentId() != segmentData.GetSegmentId()) {
                continue;
            }
            string patchSegPath =
                util::PathUtil::JoinPath(PartitionPatchIndexAccessor::GetPatchRootDirName(schemaInfo->GetSchemaId()),
                                         version.GetSegmentDirName(segMeta.GetSegmentId()));
            DirectoryPtr patchSegDir = rootDir->GetDirectory(patchSegPath, false);
            if (!patchSegDir) {
                IE_LOG(WARN, "patch segment path [%s] not exist", rootDir->DebugString(patchSegPath).c_str());
            } else {
                segmentSize += CollectSegmentSizeInfo(patchSegDir, schema, sizeInfos);
            }
        }
    }
    IE_LOG(DEBUG, "total size: %ld of segment [%d] in patch_index dir", segmentSize, segmentData.GetSegmentId());
    return segmentSize;
}
}} // namespace indexlib::index
