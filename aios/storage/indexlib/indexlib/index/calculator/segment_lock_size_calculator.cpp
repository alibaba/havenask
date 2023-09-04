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
#include "indexlib/index/calculator/segment_lock_size_calculator.h"

#include "fslib/fslib.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_util.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/index/normal/source/source_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_define.h"
#include "indexlib/util/counter/MultiCounter.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::plugin;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SegmentLockSizeCalculator);

SegmentLockSizeCalculator::SegmentLockSizeCalculator(const IndexPartitionSchemaPtr& schema,
                                                     const plugin::PluginManagerPtr& pluginManager, bool needDedup)
    : mSchema(schema)
    , mPluginManager(pluginManager)
    , mNeedDedup(needDedup)
{
}

SegmentLockSizeCalculator::~SegmentLockSizeCalculator() {}

size_t SegmentLockSizeCalculator::CalculateSize(const DirectoryPtr& directory, const MultiCounterPtr& counter) const
{
    if (!directory) {
        return 0;
    }

    if (mSchema->GetTableType() == tt_kv) {
        return CalculateKVSize(directory, counter);
    } else if (mSchema->GetTableType() == tt_kkv) {
        return CalculateKKVSize(directory, counter);
    } else if (mSchema->GetTableType() == tt_customized) {
        return CalculateCustomTableSize(directory, counter);
    }

    size_t indexSize = CalculateIndexSize(directory, counter ? counter->CreateMultiCounter("index") : counter);
    size_t attributeSize =
        CalculateAttributeSize(directory, counter ? counter->CreateMultiCounter("attribute") : counter);
    size_t summarySize = CalculateSummarySize(directory);
    size_t sourceSize = CalculateSourceSize(directory);
    if (counter) {
        counter->CreateStateCounter("summary")->Set(summarySize);
    }
    // TODO: calculate del map size

    return indexSize + attributeSize + summarySize + sourceSize;
}

size_t SegmentLockSizeCalculator::CalculateIndexSize(const DirectoryPtr& directory,
                                                     const MultiCounterPtr& counter) const
{
    std::string segmentDirName = PathUtil::GetFileName(directory->GetLogicalPath());
    if (segmentDirName == SUB_SEGMENT_DIR_NAME) {
        segmentDirName = PathUtil::GetParentDirName(directory->GetLogicalPath());
    }
    segmentid_t segId = Version::GetSegmentIdByDirName(segmentDirName);
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    if (!indexSchema) {
        return 0;
    }

    DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, false);
    if (!indexDir) {
        return 0;
    }
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    size_t totalSize = 0;
    for (; iter != indexConfigs->End(); iter++) {
        const IndexConfigPtr& indexConfig = (*iter);
        InvertedIndexType type = indexConfig->GetInvertedIndexType();
        size_t size = 0;
        if (type == it_primarykey64 || type == it_primarykey128) {
            continue;
        }

        string identifyName = CreateIndexIdentifier(indexConfig, segId);
        if (mNeedDedup && mIndexItemSizeMap.find(identifyName) != mIndexItemSizeMap.end()) {
            IE_LOG(INFO, "%s already be calculated, ignore in segment path [%s]", identifyName.c_str(),
                   directory->DebugString().c_str());
            continue;
        }

        if (type == it_customized) {
            size = CalculateCustomizedIndexSize(indexDir, indexConfig);
        } else if (type == it_range) {
            size = CalculateRangeIndexSize(indexDir, indexConfig);
        } else {
            size = CalculateNormalIndexSize(indexDir, indexConfig);
        }

        if (size > 0) {
            mIndexItemSizeMap[identifyName] = size;
        }
        totalSize += size;
        if (counter) {
            counter->CreateStateCounter(indexConfig->GetIndexName())->Set(size);
        }
    }
    return totalSize;
}

size_t SegmentLockSizeCalculator::CalculateIndexDiffSize(const DirectoryPtr& directory,
                                                         const std::string& oldTemperature,
                                                         const std::string& newTemperature) const
{
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    if (!indexSchema) {
        return 0;
    }

    DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, false);
    if (!indexDir) {
        return 0;
    }
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    size_t totalSize = 0;
    for (; iter != indexConfigs->End(); iter++) {
        const IndexConfigPtr& indexConfig = (*iter);
        InvertedIndexType type = indexConfig->GetInvertedIndexType();
        size_t size = 0;
        if (type == it_primarykey64 || type == it_primarykey128) {
            continue;
        }

        if (type == it_customized) {
            continue;
        } else if (type == it_range) {
            continue;
        } else {
            size = CalculateNormalIndexDiffSize(indexDir, indexConfig, oldTemperature, newTemperature);
            totalSize += size;
        }
    }
    return totalSize;
}

size_t SegmentLockSizeCalculator::CalculateDiffSize(const file_system::DirectoryPtr& directory,
                                                    const std::string& oldTemperature,
                                                    const std::string& newTemperature) const
{
    assert(mSchema->GetTableType() == tt_index);
    size_t indexSize = CalculateIndexDiffSize(directory, oldTemperature, newTemperature);
    size_t attributeSize = CalculateAttributeDiffSize(directory, oldTemperature, newTemperature);
    size_t summarySize = CalculateSummaryDiffSize(directory, oldTemperature, newTemperature);
    size_t sourceSize = CalculateSourceDiffSize(directory, oldTemperature, newTemperature);
    // todo : support tt_customized
    return indexSize + attributeSize + summarySize + sourceSize;
}

size_t SegmentLockSizeCalculator::CalculateAttributeSize(const DirectoryPtr& directory,
                                                         const MultiCounterPtr& counter) const
{
    segmentid_t segId = Version::GetSegmentIdByDirName(PathUtil::GetFileName(directory->GetLogicalPath()));
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema) {
        return 0;
    }

    DirectoryPtr attributeDir = directory->GetDirectory(ATTRIBUTE_DIR_NAME, false);
    if (!attributeDir) {
        return 0;
    }

    auto attrConfigs = attrSchema->CreateIterator();
    auto iter = attrConfigs->Begin();
    int64_t totalSize = 0;
    for (; iter != attrConfigs->End(); iter++) {
        const AttributeConfigPtr& attrConfig = (*iter);
        if (attrConfig->GetPackAttributeConfig()) {
            continue;
        }

        string identifyName = CreateAttrIdentifier(attrConfig, segId);
        if (mNeedDedup && mIndexItemSizeMap.find(identifyName) != mIndexItemSizeMap.end()) {
            IE_LOG(INFO, "%s already be calculated, ignore in segment path [%s]", identifyName.c_str(),
                   directory->DebugString().c_str());
            continue;
        }

        const string& attrName = attrConfig->GetAttrName();
        DirectoryPtr attrDir = attributeDir->GetDirectory(attrName, false);
        size_t size = CalculateSingleAttributeSize(attrDir);
        if (size > 0) {
            mIndexItemSizeMap[identifyName] = size;
        }

        totalSize += size;
        if (counter) {
            counter->CreateStateCounter(attrConfig->GetAttrName())->Set(size);
        }
    }

    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    for (; packIter != packAttrConfigs->End(); packIter++) {
        const PackAttributeConfigPtr& packConfig = *(packIter);
        const string& packAttrName = packConfig->GetPackName();
        DirectoryPtr packAttrDir = attributeDir->GetDirectory(packAttrName, false);
        size_t size = CalculateSingleAttributeSize(packAttrDir);
        totalSize += size;
        if (counter) {
            counter->CreateStateCounter(packConfig->GetPackName())->Set(size);
        }
    }
    return totalSize;
}

size_t SegmentLockSizeCalculator::CalculateAttributeDiffSize(const DirectoryPtr& directory,
                                                             const std::string& oldTemperature,
                                                             const std::string& newTemperature) const
{
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema) {
        return 0;
    }

    DirectoryPtr attributeDir = directory->GetDirectory(ATTRIBUTE_DIR_NAME, false);
    if (!attributeDir) {
        return 0;
    }

    auto attrConfigs = attrSchema->CreateIterator();
    auto iter = attrConfigs->Begin();
    int64_t totalSize = 0;
    for (; iter != attrConfigs->End(); iter++) {
        const AttributeConfigPtr& attrConfig = (*iter);
        if (attrConfig->GetPackAttributeConfig()) {
            continue;
        }

        const string& attrName = attrConfig->GetAttrName();
        DirectoryPtr attrDir = attributeDir->GetDirectory(attrName, false);
        size_t size = CalculateSingleAttributeDiffSize(attrDir, oldTemperature, newTemperature);
        totalSize += size;
    }

    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    for (; packIter != packAttrConfigs->End(); packIter++) {
        const PackAttributeConfigPtr& packConfig = *(packIter);
        const string& packAttrName = packConfig->GetPackName();
        DirectoryPtr packAttrDir = attributeDir->GetDirectory(packAttrName, false);
        size_t size = CalculateSingleAttributeDiffSize(packAttrDir, oldTemperature, newTemperature);
        totalSize += size;
    }
    return totalSize;
}

size_t SegmentLockSizeCalculator::CalculateSingleAttributeSize(const DirectoryPtr& attrDir) const
{
    if (!attrDir) {
        return 0;
    }
    return attrDir->EstimateFileMemoryUse(ATTRIBUTE_DATA_FILE_NAME, FSOT_LOAD_CONFIG) +
           attrDir->EstimateFileMemoryUse(ATTRIBUTE_OFFSET_FILE_NAME, FSOT_LOAD_CONFIG);
}

size_t SegmentLockSizeCalculator::CalculateSingleAttributeDiffSize(const DirectoryPtr& attrDir,
                                                                   const std::string& oldTemperature,
                                                                   const std::string& newTemperature) const
{
    if (!attrDir) {
        return 0;
    }
    return attrDir->EstimateFileMemoryUseChange(ATTRIBUTE_DATA_FILE_NAME, oldTemperature, newTemperature) +
           attrDir->EstimateFileMemoryUseChange(ATTRIBUTE_OFFSET_FILE_NAME, oldTemperature, newTemperature);
}

size_t SegmentLockSizeCalculator::CalculateSummarySize(const DirectoryPtr& directory) const
{
    size_t summarySize = 0;
    vector<DirectoryPtr> groupDirs;
    GetSummaryDirectory(directory, groupDirs);
    for (const auto& groupDir : groupDirs) {
        size_t dataSize = groupDir->EstimateFileMemoryUse(SUMMARY_DATA_FILE_NAME, FSOT_LOAD_CONFIG);
        size_t offsetSize = groupDir->EstimateFileMemoryUse(SUMMARY_OFFSET_FILE_NAME, FSOT_LOAD_CONFIG);
        summarySize += (dataSize + offsetSize);
    }
    IE_LOG(INFO, "CalculateSummarySize total[%lu]", summarySize);
    return summarySize;
}

void SegmentLockSizeCalculator::GetSummaryDirectory(const DirectoryPtr& directory,
                                                    vector<DirectoryPtr>& groupDirs) const
{
    const SummarySchemaPtr& summarySchema = mSchema->GetSummarySchema();
    if (!summarySchema || !summarySchema->NeedStoreSummary() || summarySchema->IsAllFieldsDisabled()) {
        return;
    }

    DirectoryPtr summaryDir = directory->GetDirectory(SUMMARY_DIR_NAME, false);
    if (!summaryDir) {
        return;
    }
    for (summarygroupid_t groupId = 0; groupId < summarySchema->GetSummaryGroupConfigCount(); ++groupId) {
        const SummaryGroupConfigPtr& summaryGroupConfig = summarySchema->GetSummaryGroupConfig(groupId);
        assert(summaryGroupConfig);
        if (!summaryGroupConfig || !summaryGroupConfig->NeedStoreSummary()) {
            continue;
        }
        DirectoryPtr groupDir = summaryGroupConfig->IsDefaultGroup()
                                    ? summaryDir
                                    : summaryDir->GetDirectory(summaryGroupConfig->GetGroupName(), false);
        if (!groupDir) {
            continue;
        }
        groupDirs.push_back(groupDir);
    }
}

size_t SegmentLockSizeCalculator::CalculateSummaryDiffSize(const DirectoryPtr& directory,
                                                           const std::string& oldTemperature,
                                                           const std::string& newTemperature) const
{
    vector<DirectoryPtr> groupDirs;
    GetSummaryDirectory(directory, groupDirs);
    size_t summarySize = 0;
    for (const auto& groupDir : groupDirs) {
        size_t dataSize = groupDir->EstimateFileMemoryUseChange(SUMMARY_DATA_FILE_NAME, oldTemperature, newTemperature);
        size_t offsetSize =
            groupDir->EstimateFileMemoryUseChange(SUMMARY_OFFSET_FILE_NAME, oldTemperature, newTemperature);
        summarySize += (dataSize + offsetSize);
    }
    IE_LOG(INFO, "CalculateSummaryDiffSize total[%lu]", summarySize);
    return summarySize;
}

size_t SegmentLockSizeCalculator::CalculateSourceDiffSize(const DirectoryPtr& directory,
                                                          const std::string& oldTemperature,
                                                          const std::string& newTemperature) const
{
    stringstream ss;
    const SourceSchemaPtr& sourceSchema = mSchema->GetSourceSchema();
    if (!sourceSchema || sourceSchema->IsAllFieldsDisabled()) {
        return 0;
    }
    // TODO support disable source
    DirectoryPtr sourceDir = directory->GetDirectory(SOURCE_DIR_NAME, false);
    if (!sourceDir) {
        return 0;
    }
    DirectoryPtr metaDir = sourceDir->GetDirectory(SOURCE_META_DIR, false);
    if (!metaDir) {
        return 0;
    }
    size_t sourceSize = 0;
    sourceSize += metaDir->EstimateFileMemoryUseChange(SOURCE_DATA_FILE_NAME, oldTemperature, newTemperature);
    sourceSize += metaDir->EstimateFileMemoryUseChange(SOURCE_OFFSET_FILE_NAME, oldTemperature, newTemperature);
    for (auto iter = sourceSchema->Begin(); iter != sourceSchema->End(); iter++) {
        groupid_t groupId = (*iter)->GetGroupId();
        if ((*iter)->IsDisabled()) {
            continue;
        }

        DirectoryPtr groupDir = sourceDir->GetDirectory(SourceDefine::GetDataDir(groupId), false);
        if (!groupDir) {
            continue;
        }
        sourceSize += groupDir->EstimateFileMemoryUseChange(SOURCE_DATA_FILE_NAME, oldTemperature, newTemperature);
        sourceSize += groupDir->EstimateFileMemoryUseChange(SOURCE_OFFSET_FILE_NAME, oldTemperature, newTemperature);
    }
    return sourceSize;
}

size_t SegmentLockSizeCalculator::CalculateSourceSize(const DirectoryPtr& directory) const
{
    stringstream ss;
    const SourceSchemaPtr& sourceSchema = mSchema->GetSourceSchema();
    if (!sourceSchema || sourceSchema->IsAllFieldsDisabled()) {
        return 0;
    }
    // TODO support disable source
    DirectoryPtr sourceDir = directory->GetDirectory(SOURCE_DIR_NAME, false);
    if (!sourceDir) {
        return 0;
    }
    DirectoryPtr metaDir = sourceDir->GetDirectory(SOURCE_META_DIR, false);
    if (!metaDir) {
        return 0;
    }
    size_t sourceSize = 0;
    sourceSize += metaDir->EstimateFileMemoryUse(SOURCE_DATA_FILE_NAME, FSOT_LOAD_CONFIG);
    sourceSize += metaDir->EstimateFileMemoryUse(SOURCE_OFFSET_FILE_NAME, FSOT_LOAD_CONFIG);
    for (auto iter = sourceSchema->Begin(); iter != sourceSchema->End(); iter++) {
        groupid_t groupId = (*iter)->GetGroupId();
        if ((*iter)->IsDisabled()) {
            continue;
        }

        DirectoryPtr groupDir = sourceDir->GetDirectory(SourceDefine::GetDataDir(groupId), false);
        if (!groupDir) {
            continue;
        }
        sourceSize += groupDir->EstimateFileMemoryUse(SOURCE_DATA_FILE_NAME, FSOT_LOAD_CONFIG);
        sourceSize += groupDir->EstimateFileMemoryUse(SOURCE_OFFSET_FILE_NAME, FSOT_LOAD_CONFIG);
    }
    return sourceSize;
}

size_t SegmentLockSizeCalculator::CalculateRangeIndexSize(const DirectoryPtr& directory,
                                                          const config::IndexConfigPtr& indexConfig) const
{
    if (!directory) {
        return 0;
    }
    int64_t indexSize = 0;
    const string& indexName = indexConfig->GetIndexName();
    DirectoryPtr indexDir = directory->GetDirectory(indexName, false);
    RangeIndexConfigPtr rangeConfig = DYNAMIC_POINTER_CAST(RangeIndexConfig, indexConfig);
    auto bottomConfig = rangeConfig->GetBottomLevelIndexConfig();
    auto highConfig = rangeConfig->GetHighLevelIndexConfig();

    if (indexDir) {
        indexSize += CalculateNormalIndexSize(indexDir, bottomConfig);
        indexSize += CalculateNormalIndexSize(indexDir, highConfig);
    }
    return indexSize;
}

size_t SegmentLockSizeCalculator::CalculateNormalIndexSize(const DirectoryPtr& directory,
                                                           const IndexConfigPtr& indexConfig) const
{
    if (!directory) {
        return 0;
    }

    int64_t indexSize = 0;
    const string& indexName = indexConfig->GetIndexName();
    DirectoryPtr indexDir = directory->GetDirectory(indexName, false);
    if (indexDir) {
        indexSize += indexDir->EstimateFileMemoryUse(POSTING_FILE_NAME, FSOT_LOAD_CONFIG);
        indexSize += indexDir->EstimateFileMemoryUse(BITMAP_DICTIONARY_FILE_NAME, FSOT_MEM_ACCESS);
        indexSize += indexDir->EstimateFileMemoryUse(BITMAP_POSTING_FILE_NAME, FSOT_MEM_ACCESS);

        bool disableSliceMemLock = autil::EnvUtil::getEnv<bool>("INDEXLIB_DISABLE_SLICE_MEM_LOCK", false);
        size_t memLockSize = indexDir->EstimateFileMemoryUse(DICTIONARY_FILE_NAME, FSOT_LOAD_CONFIG);
        if (!memLockSize && indexDir->IsExist(DICTIONARY_FILE_NAME) && !indexConfig->IsHashTypedDictionary()) {
            // memory not locked && use tiered dictionary
            if (!disableSliceMemLock || indexDir->EstimateFileMemoryUse(DICTIONARY_FILE_NAME, FSOT_MEM_ACCESS) > 0) {
                // enable slice memLock or block cache (ignore disable slice memLock && mem-nolock)
                size_t fileLength = indexDir->GetFileLength(DICTIONARY_FILE_NAME);
                indexSize += TieredDictionaryReader::GetPartialLockSize(fileLength);
            }
        } else {
            indexSize += memLockSize;
        }
    }

    if (HasSectionAttribute(indexConfig)) {
        DirectoryPtr sectionDir = directory->GetDirectory(indexName + "_section/", false);
        if (sectionDir) {
            indexSize += sectionDir->EstimateFileMemoryUse(ATTRIBUTE_DATA_FILE_NAME, FSOT_LOAD_CONFIG);
            indexSize += sectionDir->EstimateFileMemoryUse(ATTRIBUTE_OFFSET_FILE_NAME, FSOT_LOAD_CONFIG);
        }
    }
    return indexSize;
}

size_t SegmentLockSizeCalculator::CalculateNormalIndexDiffSize(const DirectoryPtr& directory,
                                                               const IndexConfigPtr& indexConfig,
                                                               const std::string& oldTemperature,
                                                               const std::string& newTemperature) const
{
    if (!directory) {
        return 0;
    }

    int64_t indexSize = 0;
    const string& indexName = indexConfig->GetIndexName();
    DirectoryPtr indexDir = directory->GetDirectory(indexName, false);
    if (indexDir) {
        indexSize += indexDir->EstimateFileMemoryUseChange(POSTING_FILE_NAME, oldTemperature, newTemperature);
    }

    if (HasSectionAttribute(indexConfig)) {
        DirectoryPtr sectionDir = directory->GetDirectory(indexName + "_section/", false);
        if (sectionDir) {
            indexSize +=
                sectionDir->EstimateFileMemoryUseChange(ATTRIBUTE_DATA_FILE_NAME, oldTemperature, newTemperature);
            indexSize +=
                sectionDir->EstimateFileMemoryUseChange(ATTRIBUTE_OFFSET_FILE_NAME, oldTemperature, newTemperature);
        }
    }
    return indexSize;
}

size_t SegmentLockSizeCalculator::CalculateCustomizedIndexSize(const DirectoryPtr& directory,
                                                               const IndexConfigPtr& indexConfig) const
{
    if (!directory) {
        return 0;
    }

    const string& indexName = indexConfig->GetIndexName();
    DirectoryPtr indexDir = directory->GetDirectory(indexName, false);
    if (indexDir) {
        IndexSegmentRetrieverPtr retriever(IndexPluginUtil::CreateIndexSegmentRetriever(indexConfig, mPluginManager));
        if (!retriever) {
            IE_LOG(ERROR, "cannot create IndexSegmentRetriever for index [%s]", indexConfig->GetIndexName().c_str());
            return 0;
        }
        return retriever->EstimateLoadMemoryUse(indexDir);
    }
    return 0u;
}

bool SegmentLockSizeCalculator::HasSectionAttribute(const IndexConfigPtr& indexConfig) const
{
    InvertedIndexType indexType = indexConfig->GetInvertedIndexType();
    if (indexType == it_pack || indexType == it_expack) {
        PackageIndexConfigPtr packIndexConfig = DYNAMIC_POINTER_CAST(PackageIndexConfig, indexConfig);
        if (!packIndexConfig) {
            IE_LOG(ERROR, "indexConfig must be PackageIndexConfig "
                          "for pack/expack index");
            return false;
        }
        return packIndexConfig->HasSectionAttribute();
    }
    return false;
}

size_t SegmentLockSizeCalculator::CalculateKVSize(const DirectoryPtr& directory, const MultiCounterPtr& counter) const
{
    IndexConfigPtr kvIndexConfig = CreateDataKVIndexConfig(mSchema);
    DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, false);
    if (!indexDir) {
        return 0;
    }
    DirectoryPtr kvDir = indexDir->GetDirectory(kvIndexConfig->GetIndexName(), false);
    if (!kvDir) {
        return 0;
    }
    size_t keySize = kvDir->EstimateFileMemoryUse(KV_KEY_FILE_NAME, FSOT_LOAD_CONFIG);
    size_t valueSize = kvDir->EstimateFileMemoryUse(KV_VALUE_FILE_NAME, FSOT_LOAD_CONFIG);
    if (counter) {
        counter->CreateStateCounter("key")->Set(keySize);
        counter->CreateStateCounter("value")->Set(valueSize);
    }
    return keySize + valueSize;
}

size_t SegmentLockSizeCalculator::CalculateKKVSize(const DirectoryPtr& directory, const MultiCounterPtr& counter) const
{
    KKVIndexConfigPtr kkvConfig = CreateDataKKVIndexConfig(mSchema);
    DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, false);
    if (!indexDir) {
        return 0;
    }
    DirectoryPtr kkvDir = indexDir->GetDirectory(kkvConfig->GetIndexName(), false);
    if (!kkvDir) {
        return 0;
    }
    size_t pkeySize = kkvDir->EstimateFileMemoryUse(PREFIX_KEY_FILE_NAME, FSOT_LOAD_CONFIG);
    size_t skeySize = kkvDir->EstimateFileMemoryUse(SUFFIX_KEY_FILE_NAME, FSOT_LOAD_CONFIG);
    size_t valueSize = kkvDir->EstimateFileMemoryUse(KKV_VALUE_FILE_NAME, FSOT_LOAD_CONFIG);
    if (counter) {
        counter->CreateStateCounter("pkey")->Set(pkeySize);
        counter->CreateStateCounter("skey")->Set(skeySize);
        counter->CreateStateCounter("value")->Set(valueSize);
    }
    return pkeySize + skeySize + valueSize;
}

size_t SegmentLockSizeCalculator::CalculateCustomTableSize(const DirectoryPtr& directory,
                                                           const MultiCounterPtr& counter) const
{
    DirectoryPtr dataDir = directory->GetDirectory(CUSTOM_DATA_DIR_NAME, false);
    if (!dataDir) {
        IE_LOG(WARN, "dataDir[%s] is missing in segment[%s]", CUSTOM_DATA_DIR_NAME.c_str(),
               directory->DebugString().c_str());
        return 0u;
    }
    fslib::FileList fileList;
    dataDir->ListDir("", fileList, true);
    size_t totalLoadSize = 0;
    for (const auto& path : fileList) {
        if (dataDir->IsDir(path)) {
            continue;
        }
        size_t size = dataDir->EstimateFileMemoryUse(path, FSOT_LOAD_CONFIG);
        if (counter) {
            counter->CreateStateCounter(path)->Set(size);
        }
        totalLoadSize += size;
    }
    return totalLoadSize;
}

string SegmentLockSizeCalculator::CreateIndexIdentifier(const IndexConfigPtr& indexConfig, segmentid_t segId) const
{
    return string("indexName:") + indexConfig->GetIndexName() + "@segment_" + StringUtil::toString(segId);
}

string SegmentLockSizeCalculator::CreateAttrIdentifier(const AttributeConfigPtr& attrConfig, segmentid_t segId) const
{
    return string("attributeName:") + attrConfig->GetAttrName() + "@segment_" + StringUtil::toString(segId);
}
}} // namespace indexlib::index
