#include "indexlib/index/calculator/segment_lock_size_calculator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_util.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_reader.h"
#include "indexlib/util/counter/multi_counter.h"
#include <fslib/fslib.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SegmentLockSizeCalculator);

SegmentLockSizeCalculator::SegmentLockSizeCalculator(
        const IndexPartitionSchemaPtr& schema,
        const plugin::PluginManagerPtr& pluginManager,
        bool needDedup)
    : mSchema(schema)
    , mPluginManager(pluginManager)
    , mNeedDedup(needDedup)
{
}

SegmentLockSizeCalculator::~SegmentLockSizeCalculator()
{
}

size_t SegmentLockSizeCalculator::CalculateSize(
    const DirectoryPtr& directory, const MultiCounterPtr& counter) const
{
    if (!directory)
    {
        return 0;
    }

    if (mSchema->GetTableType() == tt_customized)
    {
        return CalculateCustomTableSize(directory, counter);
    }

    size_t indexSize = CalculateIndexSize(
        directory, counter ? counter->CreateMultiCounter("index") : counter);
    size_t attributeSize = CalculateAttributeSize(
        directory, counter ? counter->CreateMultiCounter("attribute") : counter);
    size_t summarySize = CalculateSummarySize(directory);
    if (counter)
    {
        counter->CreateStateCounter("summary")->Set(summarySize);
    }
    // TODO: calculate del map size

    return indexSize + attributeSize + summarySize;
}

size_t SegmentLockSizeCalculator::CalculateIndexSize(
    const DirectoryPtr& directory, const MultiCounterPtr& counter) const
{
    segmentid_t segId = Version::GetSegmentIdByDirName(
            PathUtil::GetFileName(directory->GetPath()));
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    if (!indexSchema)
    {
        return 0;
    }

    DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, false);
    if (!indexDir)
    {
        return 0;
    }
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    size_t totalSize = 0;
    for (; iter != indexConfigs->End(); iter++)
    {
        const IndexConfigPtr& indexConfig = (*iter);
        IndexType type = indexConfig->GetIndexType();
        size_t size = 0;
        if (type == it_primarykey64 || type == it_primarykey128)
        {
            continue;
        }

        string identifyName = CreateIndexIdentifier(indexConfig, segId);
        if (mNeedDedup && mIndexItemSizeMap.find(identifyName) != mIndexItemSizeMap.end())
        {
            IE_LOG(INFO, "%s already be calculated, ignore in segment path [%s]",
                   identifyName.c_str(), directory->GetPath().c_str());
            continue;
        }

        if (type == it_customized)
        {
            size = CalculateCustomizedIndexSize(indexDir, indexConfig);
        }
        else if (type == it_range)
        {
            size = CalculateRangeIndexSize(indexDir, indexConfig);
        }
        else
        {
            size = CalculateNormalIndexSize(indexDir, indexConfig);
        }

        if (size > 0)
        {
            mIndexItemSizeMap[identifyName] = size;
        }
        totalSize += size;
        if (counter)
        {
            counter->CreateStateCounter(indexConfig->GetIndexName())->Set(size);
        }
    }
    return totalSize;
}

size_t SegmentLockSizeCalculator::CalculateAttributeSize(
    const DirectoryPtr& directory, const MultiCounterPtr& counter) const
{
    segmentid_t segId = Version::GetSegmentIdByDirName(
            PathUtil::GetFileName(directory->GetPath()));
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema)
    {
        return 0;
    }

    DirectoryPtr attributeDir = directory->GetDirectory(ATTRIBUTE_DIR_NAME, false);
    if (!attributeDir)
    {
        return 0;
    }

    auto attrConfigs = attrSchema->CreateIterator();
    auto iter = attrConfigs->Begin();
    int64_t totalSize = 0;
    for (; iter != attrConfigs->End(); iter++)
    {
        const AttributeConfigPtr &attrConfig = (*iter);
        if (attrConfig->GetPackAttributeConfig())
        {
            continue;
        }

        string identifyName = CreateAttrIdentifier(attrConfig, segId);
        if (mNeedDedup && mIndexItemSizeMap.find(identifyName) != mIndexItemSizeMap.end())
        {
            IE_LOG(INFO, "%s already be calculated, ignore in segment path [%s]",
                   identifyName.c_str(), directory->GetPath().c_str());
            continue;
        }

        const string &attrName = attrConfig->GetAttrName();
        DirectoryPtr attrDir = attributeDir->GetDirectory(attrName, false);
        size_t size = CalculateSingleAttributeSize(attrDir);
        if (size > 0)
        {
            mIndexItemSizeMap[identifyName] = size;
        }

        totalSize += size;
        if (counter)
        {
            counter->CreateStateCounter(attrConfig->GetAttrName())->Set(size);
        }
    }

    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    for (; packIter != packAttrConfigs->End(); packIter++)
    {
        const PackAttributeConfigPtr& packConfig = *(packIter);
        const string &packAttrName = packConfig->GetAttrName();
        DirectoryPtr packAttrDir = attributeDir->GetDirectory(packAttrName, false);
        size_t size = CalculateSingleAttributeSize(packAttrDir);
        totalSize += size;
        if (counter)
        {
            counter->CreateStateCounter(packConfig->GetAttrName())->Set(size);
        }
    }
    return totalSize;
}

size_t SegmentLockSizeCalculator::CalculateSingleAttributeSize(
    const DirectoryPtr& attrDir) const
{
    if (!attrDir)
    {
        return 0;
    }
    return attrDir->EstimateFileMemoryUse(ATTRIBUTE_DATA_FILE_NAME, FSOT_MMAP)
        + attrDir->EstimateFileMemoryUse(ATTRIBUTE_OFFSET_FILE_NAME, FSOT_MMAP);
}

size_t SegmentLockSizeCalculator::CalculateSummarySize(
        const DirectoryPtr& directory) const
{
    stringstream ss;
    const SummarySchemaPtr& summarySchema = mSchema->GetSummarySchema();
    if (!summarySchema || !summarySchema->NeedStoreSummary())
    {
        return 0;
    }

    DirectoryPtr summaryDir = directory->GetDirectory(SUMMARY_DIR_NAME, false);
    if (!summaryDir)
    {
        return 0;
    }

    size_t summarySize = 0;
    for (summarygroupid_t groupId = 0;
         groupId < summarySchema->GetSummaryGroupConfigCount(); ++groupId)
    {
        const SummaryGroupConfigPtr& summaryGroupConfig =
            summarySchema->GetSummaryGroupConfig(groupId);
        assert(summaryGroupConfig);
        if (!summaryGroupConfig || !summaryGroupConfig->NeedStoreSummary())
        {
            continue;
        }
        DirectoryPtr groupDir =
            summaryGroupConfig->IsDefaultGroup() ? summaryDir :
            summaryDir->GetDirectory(summaryGroupConfig->GetGroupName(), false);
        if (!groupDir)
        {
            continue;
        }
        size_t dataSize =
            groupDir->EstimateFileMemoryUse(SUMMARY_DATA_FILE_NAME, FSOT_LOAD_CONFIG);
        size_t offsetSize =
            groupDir->EstimateIntegratedFileMemoryUse(SUMMARY_OFFSET_FILE_NAME);
        summarySize += (dataSize + offsetSize);
        ss << summaryGroupConfig->GetGroupName() << ":"
           << "[" << offsetSize << "," << dataSize << "],";
    }
    IE_LOG(INFO, "CalculateSummarySize, %s total[%lu]",
           ss.str().c_str(), summarySize);
    return summarySize;
}

size_t SegmentLockSizeCalculator::CalculateRangeIndexSize(
        const DirectoryPtr& directory,
        const config::IndexConfigPtr& indexConfig) const
{
    if (!directory)
    {
        return 0;
    }
    int64_t indexSize = 0;
    const string& indexName = indexConfig->GetIndexName();
    DirectoryPtr indexDir = directory->GetDirectory(indexName, false);
    RangeIndexConfigPtr rangeConfig =
        DYNAMIC_POINTER_CAST(RangeIndexConfig, indexConfig);
    auto bottomConfig = rangeConfig->GetBottomLevelIndexConfig();
    auto highConfig = rangeConfig->GetHighLevelIndexConfig();
    auto calc = [&indexSize](const DirectoryPtr& dir, const string& indexName) {
        DirectoryPtr indexDir = dir->GetDirectory(indexName, false);
        indexSize += indexDir->EstimateIntegratedFileMemoryUse(
            DICTIONARY_FILE_NAME);
        indexSize += indexDir->EstimateIntegratedFileMemoryUse(
            POSTING_FILE_NAME);
    };
    if (indexDir)
    {
        calc(indexDir, bottomConfig->GetIndexName());
        calc(indexDir, highConfig->GetIndexName());
    }
    return indexSize;
}

size_t SegmentLockSizeCalculator::CalculateNormalIndexSize(
        const DirectoryPtr& directory, const IndexConfigPtr& indexConfig) const
{
    if (!directory)
    {
        return 0;
    }

    int64_t indexSize = 0;
    const string& indexName = indexConfig->GetIndexName();
    DirectoryPtr indexDir = directory->GetDirectory(indexName, false);
    if (indexDir)
    {
        indexSize += indexDir->EstimateFileMemoryUse(
                POSTING_FILE_NAME, FSOT_LOAD_CONFIG);
        indexSize += indexDir->EstimateIntegratedFileMemoryUse(
            BITMAP_DICTIONARY_FILE_NAME);
        indexSize += indexDir->EstimateIntegratedFileMemoryUse(
            BITMAP_POSTING_FILE_NAME);

        if(indexDir->OnlyPatialLock(DICTIONARY_FILE_NAME))
        {
            size_t fileLength = indexDir->GetFileLength(DICTIONARY_FILE_NAME);
            if (!indexConfig->IsHashTypedDictionary())
            {
                indexSize += TieredDictionaryReader::GetPatialLockSize(fileLength);
            }
        }
        else
        {
            // no patial lock, calculate the whole file
            indexSize += indexDir->EstimateIntegratedFileMemoryUse(
                    DICTIONARY_FILE_NAME);
        }
    }

    if (HasSectionAttribute(indexConfig))
    {
        DirectoryPtr sectionDir = directory->GetDirectory(
                indexName + "_section/", false);
        if (sectionDir)
        {
            indexSize += sectionDir->EstimateIntegratedFileMemoryUse(
                ATTRIBUTE_DATA_FILE_NAME);
            indexSize += sectionDir->EstimateIntegratedFileMemoryUse(
                ATTRIBUTE_OFFSET_FILE_NAME);
        }
    }
    return indexSize;
}

size_t SegmentLockSizeCalculator::CalculateCustomizedIndexSize(
        const DirectoryPtr& directory,
        const IndexConfigPtr& indexConfig) const
{
    if (!directory)
    {
        return 0;
    }

    const string& indexName = indexConfig->GetIndexName();
    DirectoryPtr indexDir = directory->GetDirectory(indexName, false);
    if (indexDir)
    {
        IndexSegmentRetrieverPtr retriever(IndexPluginUtil::CreateIndexSegmentRetriever(
                        indexConfig, mPluginManager));
        if (!retriever)
        {
            IE_LOG(ERROR, "cannot create IndexSegmentRetriever for index [%s]",
                   indexConfig->GetIndexName().c_str());
            return 0;
        }
        return retriever->EstimateLoadMemoryUse(indexDir);
    }
    return 0u;
}

bool SegmentLockSizeCalculator::HasSectionAttribute(
        const IndexConfigPtr& indexConfig) const
{
    IndexType indexType = indexConfig->GetIndexType();
    if (indexType == it_pack || indexType == it_expack)
    {
        PackageIndexConfigPtr packIndexConfig =
            DYNAMIC_POINTER_CAST(PackageIndexConfig, indexConfig);
        if (!packIndexConfig)
        {
            IE_LOG(ERROR, "indexConfig must be PackageIndexConfig "
                   "for pack/expack index");
            return false;
        }
        return packIndexConfig->HasSectionAttribute();
    }
    return false;
}

size_t SegmentLockSizeCalculator::CalculateCustomTableSize(
    const DirectoryPtr& directory, const MultiCounterPtr& counter) const
{
    DirectoryPtr dataDir = directory->GetDirectory(CUSTOM_DATA_DIR_NAME, false);
    if (!dataDir)
    {
        IE_LOG(WARN, "dataDir[%s] is missing in segment[%s]",
               CUSTOM_DATA_DIR_NAME.c_str(), directory->GetPath().c_str());
        return 0u;
    }
    fslib::FileList fileList;
    dataDir->ListFile("", fileList, true, false);
    size_t totalLoadSize = 0;
    for (const auto& path : fileList)
    {
        if (dataDir->IsDir(path))
        {
            continue;
        }
        size_t size = dataDir->EstimateFileMemoryUse(path, FSOT_LOAD_CONFIG);
        if (counter)
        {
            counter->CreateStateCounter(path)->Set(size);
        }
        totalLoadSize += size;
    }
    return totalLoadSize;
}

string SegmentLockSizeCalculator::CreateIndexIdentifier(
        const IndexConfigPtr& indexConfig, segmentid_t segId) const
{
    return string("indexName:") + indexConfig->GetIndexName() +
        "@segment_" + StringUtil::toString(segId);
}

string SegmentLockSizeCalculator::CreateAttrIdentifier(
        const AttributeConfigPtr& attrConfig, segmentid_t segId) const
{
    return string("attributeName:") + attrConfig->GetAttrName() +
        "@segment_" + StringUtil::toString(segId);
}

IE_NAMESPACE_END(index);
