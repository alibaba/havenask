#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/index/calculator/segment_lock_size_calculator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_define.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/counter/multi_counter.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PartitionSizeCalculator);

PartitionSizeCalculator::PartitionSizeCalculator(
        const DirectoryPtr& directory,
        const IndexPartitionSchemaPtr& schema,
        bool throwExceptionIfNotExist,
        const plugin::PluginManagerPtr& pluginManager)
    : mDirectory(directory)
    , mSchema(schema)
    , mThrowExceptionIfNotExist(throwExceptionIfNotExist)
    , mPluginManager(pluginManager)
{
}

PartitionSizeCalculator::~PartitionSizeCalculator() 
{
}

size_t PartitionSizeCalculator::CalculateDiffVersionLockSizeWithoutPatch(
    const Version& version, const Version& lastLoadVersion,
    const PartitionDataPtr& partitionData, const MultiCounterPtr& counter)
{
    assert(mSchema);
    if (!mDirectory)
    {
        return 0;
    }

    SegmentLockSizeCalculatorPtr segmentCalculator = 
        CreateSegmentCalculator(mSchema);
    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();
    SegmentLockSizeCalculatorPtr subSegmentCalculator = 
        CreateSegmentCalculator(subSchema);

    vector<string> segPathVec;
    GetSegmentPathVec(version, segPathVec);
    vector<string> lastVersionSegPathVec;
    GetSegmentPathVec(lastLoadVersion, lastVersionSegPathVec);
    vector<string> diffSegPathVec;
    set_difference(segPathVec.begin(), segPathVec.end(),
                   lastVersionSegPathVec.begin(), lastVersionSegPathVec.end(),
                   inserter(diffSegPathVec, diffSegPathVec.begin()));

    // sort diff segments:
    // dimension 1: segmentid from big to small
    // dimension 2: patch schemaId from big to small
    sort(diffSegPathVec.begin(), diffSegPathVec.end(),
         [](const string& lft, const string& rht) -> bool
         {
             segmentid_t lftSegId = Version::GetSegmentIdByDirName(PathUtil::GetFileName(lft));
             segmentid_t rhtSegId = Version::GetSegmentIdByDirName(PathUtil::GetFileName(rht));
             if (lftSegId == rhtSegId)
             {
                 schemavid_t lftSchemaId = 0;
                 PartitionPatchIndexAccessor::ExtractSchemaIdFromPatchRootDir(
                         PathUtil::GetParentDirName(lft), lftSchemaId);
                 schemavid_t rhtSchemaId = 0;
                 PartitionPatchIndexAccessor::ExtractSchemaIdFromPatchRootDir(
                         PathUtil::GetParentDirName(rht), rhtSchemaId);
                 return lftSchemaId > rhtSchemaId;
             }
             return lftSegId > rhtSegId;
         });
    
    size_t totalSize = 0;
    for (size_t i = 0; i < diffSegPathVec.size(); ++i)
    {
        const string& segDirName = diffSegPathVec[i];
        DirectoryPtr segDir = mDirectory->GetDirectory(segDirName, mThrowExceptionIfNotExist);
        if (segDir)
        {
            auto segCounter = counter ? counter->CreateMultiCounter(segDirName) : counter;
            totalSize += segmentCalculator->CalculateSize(segDir, segCounter);
        }
        else
        {
            IE_LOG(WARN, "segDir [%s] not exist in [%s]",
                   segDirName.c_str(), mDirectory->GetPath().c_str());
        }
        
        if (subSegmentCalculator)
        {
            DirectoryPtr subSegDir = segDir->GetDirectory(
                    SUB_SEGMENT_DIR_NAME, mThrowExceptionIfNotExist);
            if (subSegDir)
            {
                auto subSegCounter = counter ? counter->CreateMultiCounter(segDirName + "/" + SUB_SEGMENT_DIR_NAME) : counter;
                totalSize += subSegmentCalculator->CalculateSize(subSegDir, subSegCounter);
            }
            else
            {
                IE_LOG(WARN, "sub_segment not exist in [%s]", segDir->GetPath().c_str());
            }
        }
    }

    if (mSchema->GetTableType() == tt_index)
    {
        if (mSchema->GetIndexSchema()->HasPrimaryKeyIndex())
        {
            auto pkCounter = counter ? counter->CreateMultiCounter("pk") : counter;
            totalSize += CalculatePkSize(partitionData, lastLoadVersion, pkCounter);
        }
    }
    return totalSize;
}

size_t PartitionSizeCalculator::CalculatePkSize(
    const PartitionDataPtr& partitionData, const Version& lastLoadVersion,
    const MultiCounterPtr& counter)
{
    size_t totalSize = 0;
    size_t mainSize = CalculatePkReaderLoadSize(mSchema, partitionData, lastLoadVersion);
    totalSize += mainSize;
    if (counter)
    {
        counter->CreateStateCounter("main")->Set(mainSize);
    }
    const IndexPartitionSchemaPtr& subSchema = 
        mSchema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        size_t subSize = CalculatePkReaderLoadSize(
            subSchema, partitionData->GetSubPartitionData(), lastLoadVersion);
        totalSize += subSize;
        if (counter)
        {
            counter->CreateStateCounter("sub")->Set(subSize);
        }
    }
    return totalSize;
}

size_t PartitionSizeCalculator::CalculatePkReaderLoadSize(
        const config::IndexPartitionSchemaPtr& schema,
        const index_base::PartitionDataPtr& partitionData,
        const Version& lastLoadVersion)
{
    IndexConfigPtr pkIndexConfig = 
        schema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    if (!pkIndexConfig || pkIndexConfig->GetIndexType() == it_kkv ||
        pkIndexConfig->GetIndexType() == it_kv)
    {
        return 0;
    }
    IndexReaderPtr pkIndexReader(
            IndexReaderFactory::CreateIndexReader(pkIndexConfig->GetIndexType()));
    return pkIndexReader->EstimateLoadSize(
            partitionData, pkIndexConfig, lastLoadVersion);
}

SegmentLockSizeCalculatorPtr PartitionSizeCalculator::CreateSegmentCalculator(
        const IndexPartitionSchemaPtr& schema)
{
    if (!schema)
    {
        return SegmentLockSizeCalculatorPtr();
    }
    return SegmentLockSizeCalculatorPtr(
            new SegmentLockSizeCalculator(schema, mPluginManager, true));
}

void PartitionSizeCalculator::GetSegmentPathVec(const Version& version,
        vector<string>& segPathVec)
{
    assert(segPathVec.empty());
    for (size_t i = 0; i < version.GetSegmentCount(); ++i)
    {
        string segDirName = version.GetSegmentDirName(version[i]);
        segPathVec.push_back(segDirName);
    }

    if (version.GetVersionId() != INVALID_VERSION &&
        version.GetSchemaVersionId() != DEFAULT_SCHEMAID)
    {
        PartitionPatchMeta patchMeta;
        patchMeta.Load(mDirectory, version.GetVersionId());
        PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
        while (metaIter.HasNext())
        {
            SchemaPatchInfoPtr schemaInfo = metaIter.Next();
            assert(schemaInfo);
            SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
            for (; sIter != schemaInfo->End(); sIter++)
            {
                const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
                if (!version.HasSegment(segMeta.GetSegmentId()))
                {
                    continue;
                }
                string patchSegPath = PathUtil::JoinPath(
                        PartitionPatchIndexAccessor::GetPatchRootDirName(schemaInfo->GetSchemaId()),
                        version.GetSegmentDirName(segMeta.GetSegmentId()));
                segPathVec.push_back(patchSegPath);
            }
        }
    }
    sort(segPathVec.begin(), segPathVec.end());
}

IE_NAMESPACE_END(index);

