#include "indexlib/partition/remote_access/partition_patcher.h"
#include "indexlib/partition/remote_access/attribute_data_patcher_typed.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/schema_differ.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/util/path_util.h"
#include "indexlib/plugin/index_plugin_loader.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionPatcher);

bool PartitionPatcher::Init(const OfflinePartitionPtr& partition,
                            const IndexPartitionSchemaPtr& newSchema,
                            const string& patchDir,
                            const plugin::PluginManagerPtr& pluginManager)
{
    if (!partition)
    {
        return false;
    }

    IndexPartitionSchemaPtr schema = partition->GetSchema();
    assert(schema);
    TableType type = schema->GetTableType();
    if (type == tt_kkv || type == tt_kv)
    {
        IE_LOG(ERROR, "not support kkv or kkv table!");
        return false;
    }

    if (!FileSystemWrapper::IsExist(patchDir))
    {
        IE_LOG(ERROR, "target partition patch path [%s] is not exist!", patchDir.c_str());
        return false;
    }

    if (!FileSystemWrapper::IsDir(patchDir))
    {
        IE_LOG(ERROR, "target partition patch path [%s] is not dir!", patchDir.c_str());
        return false;
    }

    mPatchDir = patchDir;
    mOldSchema = schema;
    mNewSchema = newSchema;
    IndexPartitionOptions option;
    option.SetNeedRewriteFieldType(true);
    SchemaRewriter::RewriteFieldType(mOldSchema, option);
    SchemaRewriter::RewriteFieldType(mNewSchema, option);
    
    mPartitionData = partition->GetPartitionData();
    mPluginManager = pluginManager;
    assert(mPluginManager);
    auto options = partition->GetOptions();
    mMergeIOConfig = options.GetOfflineConfig().mergeConfig.mergeIOConfig;
    string errorMsg;
    auto originSchema = mOldSchema;
    if (mNewSchema->HasModifyOperations()) {
        uint32_t operationCount = mNewSchema->GetModifyOperationCount();
        if (operationCount == 1) {
            originSchema.reset(mNewSchema->CreateSchemaWithoutModifyOperations());
        } else {
            originSchema.reset(mNewSchema->CreateSchemaForTargetModifyOperation(
                            operationCount - 1));
        }
    }
    if (!SchemaDiffer::CalculateAlterFields(originSchema, mNewSchema,
                    mAlterAttributes, mAlterIndexes, errorMsg))
    {
        IE_LOG(ERROR, "calculate alter fields fail:%s!", errorMsg.c_str());
        return false;
    }
    return true;
}
            
AttributeDataPatcherPtr PartitionPatcher::CreateSingleAttributePatcher(
        const string& attrName, segmentid_t segmentId)
{
    AttributeConfigPtr attrConf = GetAlterAttributeConfig(attrName);
    if (!attrConf)
    {
        IE_LOG(ERROR, "[%s] not in alter attributes", attrName.c_str());
        return AttributeDataPatcherPtr();
    }

    if (attrConf->GetPackAttributeConfig() != NULL)
    {
        IE_LOG(ERROR, "[%s] is in pack attribute, not support", attrName.c_str());
        return AttributeDataPatcherPtr();
    }

    Version version = mPartitionData->GetOnDiskVersion();
    if (!version.HasSegment(segmentId))
    {
        IE_LOG(ERROR, "segment [%d] not in version [%d]",
               segmentId, version.GetVersionId());
        return AttributeDataPatcherPtr();
    }

    FieldType ft = attrConf->GetFieldType();
    bool isMultiValue = attrConf->IsMultiValue();
    AttributeDataPatcherPtr patcher;
    switch (ft)
    {
#define MACRO(field_type)                                               \
        case field_type:                                                \
        {                                                               \
            if (!isMultiValue)                                          \
            {                                                           \
                typedef typename FieldTypeTraits<field_type>::AttrItemType T; \
                patcher.reset(new AttributeDataPatcherTyped<T>());      \
            }                                                           \
            else                                                        \
            {                                                           \
                typedef typename FieldTypeTraits<field_type>::AttrItemType T; \
                typedef typename autil::MultiValueType<T> MT;           \
                patcher.reset(new AttributeDataPatcherTyped<MT>());     \
            }                                                           \
            break;                                                      \
        }
        NUMBER_FIELD_MACRO_HELPER(MACRO);
#undef MACRO
    case ft_string:
        if (!isMultiValue)
        {
            patcher.reset(new AttributeDataPatcherTyped<MultiChar>());

        }
        else
        {
            patcher.reset(new AttributeDataPatcherTyped<MultiString>());
        }
        break;
    default:
        assert(false);
    }

    string segmentDirPath = PathUtil::JoinPath(mPatchDir,
            version.GetSegmentDirName(segmentId));
    FileSystemWrapper::MkDirIfNotExist(segmentDirPath);

    SegmentData segData = mPartitionData->GetSegmentData(segmentId);
    if (!patcher || !patcher->Init(attrConf, mMergeIOConfig,
                                   segmentDirPath, segData.GetSegmentInfo().docCount))
    {
        return AttributeDataPatcherPtr();
    }
    IE_LOG(INFO, "create single attribute patcher [%s] for segment [%d]",
           attrName.c_str(), segmentId);
    return patcher;
}

IndexDataPatcherPtr PartitionPatcher::CreateSingleIndexPatcher(const string& indexName,
        segmentid_t segmentId, size_t initDistinctTermCount)
{
    IndexConfigPtr indexConf = GetAlterIndexConfig(indexName);
    if (!indexConf)
    {
        IE_LOG(ERROR, "[%s] not in alter indexes", indexName.c_str());
        return IndexDataPatcherPtr();
    }

    Version version = mPartitionData->GetOnDiskVersion();
    if (!version.HasSegment(segmentId))
    {
        IE_LOG(ERROR, "segment [%d] not in version [%d]",
               segmentId, version.GetVersionId());
        return IndexDataPatcherPtr();
    }
    
    IndexDataPatcherPtr patcher(new IndexDataPatcher(
                    mNewSchema, mPartitionData->GetRootDirectory(),
                    initDistinctTermCount,
                    mPluginManager));
    string segmentDirPath = PathUtil::JoinPath(mPatchDir,
            version.GetSegmentDirName(segmentId));
    FileSystemWrapper::MkDirIfNotExist(segmentDirPath);
    SegmentData segData = mPartitionData->GetSegmentData(segmentId);
    if (!patcher || !patcher->Init(indexConf, segmentDirPath,
                                   segData.GetSegmentInfo().docCount))
    {
        return IndexDataPatcherPtr();
    }

    IE_LOG(INFO, "create single index patcher [%s] for segment [%d]",
           indexName.c_str(), segmentId);
    return patcher;
}

AttributeConfigPtr PartitionPatcher::GetAlterAttributeConfig(
        const string& attrName) const
{
    AttributeConfigPtr attrConf;
    for (size_t i = 0; i < mAlterAttributes.size(); i++)
    {
        if (mAlterAttributes[i]->GetAttrName() == attrName)
        {
            attrConf = mAlterAttributes[i];
            break;
        }
    }
    return attrConf;
}

IndexConfigPtr PartitionPatcher::GetAlterIndexConfig(
        const string& indexName) const
{
    IndexConfigPtr indexConf;
    for (size_t i = 0; i < mAlterIndexes.size(); i++)
    {
        if (mAlterIndexes[i]->GetIndexName() == indexName)
        {
            indexConf = mAlterIndexes[i];
            break;
        }
    }
    return indexConf;
}

IE_NAMESPACE_END(partition);

