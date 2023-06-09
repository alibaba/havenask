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
#include "indexlib/partition/remote_access/partition_patcher.h"

#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/schema_differ.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/remote_access/attribute_data_patcher_typed.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::plugin;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionPatcher);

bool PartitionPatcher::Init(const OfflinePartitionPtr& partition, const IndexPartitionSchemaPtr& newSchema,
                            const file_system::DirectoryPtr& patchDir, const plugin::PluginManagerPtr& pluginManager)
{
    if (!partition) {
        return false;
    }

    IndexPartitionSchemaPtr schema = partition->GetSchema();
    assert(schema);
    TableType type = schema->GetTableType();
    if (type == tt_kkv || type == tt_kv) {
        IE_LOG(ERROR, "not support kkv or kkv table!");
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
            originSchema.reset(mNewSchema->CreateSchemaForTargetModifyOperation(operationCount - 1));
        }
    }
    if (!SchemaDiffer::CalculateAlterFields(originSchema, mNewSchema, mAlterAttributes, mAlterIndexes, errorMsg)) {
        IE_LOG(ERROR, "calculate alter fields fail:%s!", errorMsg.c_str());
        return false;
    }
    return true;
}

AttributeDataPatcherPtr PartitionPatcher::CreateSingleAttributePatcher(const string& attrName, segmentid_t segmentId)
{
    AttributeConfigPtr attrConf = GetAlterAttributeConfig(attrName);
    if (!attrConf) {
        IE_LOG(ERROR, "[%s] not in alter attributes", attrName.c_str());
        return AttributeDataPatcherPtr();
    }

    if (attrConf->GetPackAttributeConfig() != NULL) {
        IE_LOG(ERROR, "[%s] is in pack attribute, not support", attrName.c_str());
        return AttributeDataPatcherPtr();
    }

    Version version = mPartitionData->GetOnDiskVersion();
    if (!version.HasSegment(segmentId)) {
        IE_LOG(ERROR, "segment [%d] not in version [%d]", segmentId, version.GetVersionId());
        return AttributeDataPatcherPtr();
    }

    FieldType ft = attrConf->GetFieldType();
    bool isMultiValue = attrConf->IsMultiValue();
    AttributeDataPatcherPtr patcher;
    switch (ft) {
#define MACRO(field_type)                                                                                              \
    case field_type: {                                                                                                 \
        if (!isMultiValue) {                                                                                           \
            typedef typename FieldTypeTraits<field_type>::AttrItemType T;                                              \
            patcher.reset(new AttributeDataPatcherTyped<T>());                                                         \
        } else {                                                                                                       \
            typedef typename FieldTypeTraits<field_type>::AttrItemType T;                                              \
            typedef typename autil::MultiValueType<T> MT;                                                              \
            patcher.reset(new AttributeDataPatcherTyped<MT>());                                                        \
        }                                                                                                              \
        break;                                                                                                         \
    }
        NUMBER_FIELD_MACRO_HELPER(MACRO);
#undef MACRO
    case ft_string:
        if (!isMultiValue) {
            patcher.reset(new AttributeDataPatcherTyped<MultiChar>());

        } else {
            patcher.reset(new AttributeDataPatcherTyped<MultiString>());
        }
        break;
    default:
        assert(false);
    }

    const string segmentDirPath = version.GetSegmentDirName(segmentId);
    mPatchDir->MakeDirectory(segmentDirPath);
    auto segmentDirectory = mPatchDir->GetDirectory(segmentDirPath, false);
    if (segmentDirectory == nullptr) {
        IE_LOG(ERROR, "get emptry directory, path[%s/%s]", mPatchDir->DebugString().c_str(), segmentDirPath.c_str());
        return AttributeDataPatcherPtr();
    }

    SegmentData segData = mPartitionData->GetSegmentData(segmentId);
    if (!patcher || !patcher->Init(attrConf, mMergeIOConfig, segmentDirectory, segData.GetSegmentInfo()->docCount)) {
        return AttributeDataPatcherPtr();
    }
    IE_LOG(INFO, "create single attribute patcher [%s] for segment [%d]", attrName.c_str(), segmentId);
    return patcher;
}

IndexDataPatcherPtr PartitionPatcher::CreateSingleIndexPatcher(const string& indexName, segmentid_t segmentId,
                                                               size_t initDistinctTermCount)
{
    IndexConfigPtr indexConf = GetAlterIndexConfig(indexName);
    if (!indexConf) {
        IE_LOG(ERROR, "[%s] not in alter indexes", indexName.c_str());
        return IndexDataPatcherPtr();
    }

    Version version = mPartitionData->GetOnDiskVersion();
    if (!version.HasSegment(segmentId)) {
        IE_LOG(ERROR, "segment [%d] not in version [%d]", segmentId, version.GetVersionId());
        return IndexDataPatcherPtr();
    }

    format_versionid_t binaryFormatVersionId =
        mPartitionData->GetIndexFormatVersion().GetInvertedIndexBinaryFormatVersion();
    if (indexConf->GetIndexFormatVersionId() > binaryFormatVersionId) {
        IE_LOG(ERROR, "format_version_id [%d] for index [%s] is bigger than expected max version id[%d]",
               indexConf->GetIndexFormatVersionId(), indexConf->GetIndexName().c_str(), binaryFormatVersionId);
        return IndexDataPatcherPtr();
    }

    IndexDataPatcherPtr patcher(
        new IndexDataPatcher(mNewSchema, mPartitionData->GetRootDirectory(), initDistinctTermCount, mPluginManager));
    const string segmentDirPath = version.GetSegmentDirName(segmentId);
    mPatchDir->MakeDirectory(segmentDirPath);
    auto segmentDirectory = mPatchDir->GetDirectory(segmentDirPath, false);
    if (segmentDirectory == nullptr) {
        IE_LOG(ERROR, "get emptry directory, path[%s/%s]", mPatchDir->DebugString().c_str(), segmentDirPath.c_str());
        return IndexDataPatcherPtr();
    }

    SegmentData segData = mPartitionData->GetSegmentData(segmentId);
    if (!patcher || !patcher->Init(indexConf, segmentDirectory, segData.GetSegmentInfo()->docCount)) {
        return IndexDataPatcherPtr();
    }

    IE_LOG(INFO, "create single index patcher [%s] for segment [%d]", indexName.c_str(), segmentId);
    return patcher;
}

AttributeConfigPtr PartitionPatcher::GetAlterAttributeConfig(const string& attrName) const
{
    AttributeConfigPtr attrConf;
    for (size_t i = 0; i < mAlterAttributes.size(); i++) {
        if (mAlterAttributes[i]->GetAttrName() == attrName) {
            attrConf = mAlterAttributes[i];
            break;
        }
    }
    return attrConf;
}

IndexConfigPtr PartitionPatcher::GetAlterIndexConfig(const string& indexName) const
{
    IndexConfigPtr indexConf;
    for (size_t i = 0; i < mAlterIndexes.size(); i++) {
        if (mAlterIndexes[i]->GetIndexName() == indexName) {
            indexConf = mAlterIndexes[i];
            break;
        }
    }
    return indexConf;
}
}} // namespace indexlib::partition
