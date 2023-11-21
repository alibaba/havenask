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
#include "indexlib/index_base/patch/partition_patch_meta_creator.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace fslib;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, PartitionPatchMetaCreator);

PartitionPatchMetaCreator::PartitionPatchMetaCreator() {}

PartitionPatchMetaCreator::~PartitionPatchMetaCreator() {}

PartitionPatchMetaPtr PartitionPatchMetaCreator::Create(const DirectoryPtr& rootDir,
                                                        const IndexPartitionSchemaPtr& schema, const Version& version)
{
    if (schema->GetSchemaVersionId() != version.GetSchemaVersionId()) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "schema version not match in schema [%d] and version [%d]",
                             schema->GetSchemaVersionId(), version.GetSchemaVersionId());
    }

    if (schema->GetSchemaVersionId() == DEFAULT_SCHEMAID) {
        return PartitionPatchMetaPtr();
    }

    if (schema->HasModifyOperations()) {
        return CreatePatchMetaForModifySchema(rootDir, schema, version);
    }

    schemaid_t lastSchemaId = DEFAULT_SCHEMAID;
    PartitionPatchMetaPtr lastPatchMeta = GetLastPartitionPatchMeta(rootDir, lastSchemaId);
    if (lastSchemaId > schema->GetSchemaVersionId()) {
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "not support create PartitionPatchMeta "
                             "when last version schema id [%d] greater than current [%d],"
                             "may be just rollback",
                             lastSchemaId, schema->GetSchemaVersionId());
    }
    return CreatePatchMeta(rootDir, schema, version, lastPatchMeta, lastSchemaId);
}

PartitionPatchMetaPtr
PartitionPatchMetaCreator::GetLastPartitionPatchMeta(const file_system::DirectoryPtr& rootDirectory,
                                                     schemaid_t& lastSchemaId)
{
    Version lastVersion;
    VersionLoader::GetVersion(rootDirectory, lastVersion, INVALID_VERSIONID);
    if (lastVersion.GetVersionId() == INVALID_VERSIONID || lastVersion.GetSchemaVersionId() == DEFAULT_SCHEMAID) {
        lastSchemaId = DEFAULT_SCHEMAID;
        return PartitionPatchMetaPtr();
    }

    PartitionPatchMetaPtr patchMeta(new PartitionPatchMeta);
    try {
        patchMeta->Load(rootDirectory, lastVersion.GetVersionId());
    } catch (const ExceptionBase& e) {
        IE_LOG(ERROR, "Exception occurred [%s]", e.what());
        lastSchemaId = DEFAULT_SCHEMAID;
        return PartitionPatchMetaPtr();
    }
    lastSchemaId = lastVersion.GetSchemaVersionId();
    return patchMeta;
}

void PartitionPatchMetaCreator::GetPatchIndexInfos(const file_system::DirectoryPtr& segDir,
                                                   const IndexPartitionSchemaPtr& schema, vector<string>& indexNames,
                                                   vector<string>& attrNames)
{
    vector<string> backupIndexs;
    if (segDir->IsExist(INDEX_DIR_NAME)) {
        FileList fileList;
        segDir->ListDir(INDEX_DIR_NAME, fileList);
        backupIndexs.assign(fileList.begin(), fileList.end());
    }

    vector<string> backupAttributes;
    if (segDir->IsExist(ATTRIBUTE_DIR_NAME)) {
        FileList fileList;
        segDir->ListDir(ATTRIBUTE_DIR_NAME, fileList);
        backupAttributes.assign(fileList.begin(), fileList.end());
    }
    GetValidIndexAndAttribute(schema, backupIndexs, backupAttributes, indexNames, attrNames);
}

void PartitionPatchMetaCreator::GetValidIndexAndAttribute(const IndexPartitionSchemaPtr& schema,
                                                          const vector<string>& inputIndexNames,
                                                          const vector<string>& inputAttrNames,
                                                          vector<string>& indexNames, vector<string>& attrNames)
{
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    if (indexSchema) {
        for (size_t i = 0; i < inputIndexNames.size(); i++) {
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(inputIndexNames[i]);
            if (!indexConfig) {
                continue;
            }
            indexNames.push_back(indexConfig->GetIndexName());
            if (indexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING) {
                string indexName;
                if (!IndexConfig::GetIndexNameFromShardingIndexName(indexConfig->GetIndexName(), indexName)) {
                    INDEXLIB_FATAL_ERROR(Schema, "sharding index config [%s] get parent index name failed",
                                         indexConfig->GetIndexName().c_str());
                }
                indexNames.push_back(indexName);
            }
        }
    }
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    if (attrSchema) {
        for (size_t i = 0; i < inputAttrNames.size(); i++) {
            AttributeConfigPtr attrConf = attrSchema->GetAttributeConfig(inputAttrNames[i]);
            if (attrConf) {
                attrNames.push_back(attrConf->GetAttrName());
            }
        }
    }
}

bool ReverseCompareSchemaId(schemaid_t lft, schemaid_t rht) { return lft > rht; }

PartitionPatchMetaPtr PartitionPatchMetaCreator::CreatePatchMeta(const file_system::DirectoryPtr& rootDirectory,
                                                                 const IndexPartitionSchemaPtr& schema,
                                                                 const Version& version,
                                                                 const PartitionPatchMetaPtr& lastPatchMeta,
                                                                 schemaid_t lastSchemaId)
{
    vector<schemaid_t> schemaIdVec;
    FileList patchRootList;
    PartitionPatchIndexAccessor::ListPatchRootDirs(rootDirectory, patchRootList);
    for (size_t i = 0; i < patchRootList.size(); i++) {
        schemaid_t schemaId;
        PartitionPatchIndexAccessor::ExtractSchemaIdFromPatchRootDir(patchRootList[i], schemaId);
        if (schemaId > lastSchemaId && schemaId <= schema->GetSchemaVersionId()) {
            schemaIdVec.push_back(schemaId);
        }
    }
    sort(schemaIdVec.begin(), schemaIdVec.end(), ReverseCompareSchemaId);
    return CreatePatchMeta(rootDirectory, schema, version, schemaIdVec, lastPatchMeta);
}

PartitionPatchMetaPtr PartitionPatchMetaCreator::CreatePatchMeta(const file_system::DirectoryPtr& rootDirectory,
                                                                 const IndexPartitionSchemaPtr& schema,
                                                                 const Version& version,
                                                                 const vector<schemaid_t>& schemaIdVec,
                                                                 const PartitionPatchMetaPtr& lastPatchMeta)
{
    PartitionPatchMetaPtr patchMeta(new PartitionPatchMeta);
    for (size_t i = 0; i < schemaIdVec.size(); i++) {
        string patchPath = PartitionPatchIndexAccessor::GetPatchRootDirName(schemaIdVec[i]);
        auto patchDirectory = rootDirectory->GetDirectory(patchPath, false);
        if (patchDirectory == nullptr) {
            IE_LOG(ERROR, "get directory failed, path[%s]", patchPath.c_str());
            return PartitionPatchMetaPtr();
        }
        Version::Iterator vIter = version.CreateIterator();
        while (vIter.HasNext()) {
            segmentid_t segId = vIter.Next();
            string segPath = version.GetSegmentDirName(segId);
            auto segDir = patchDirectory->GetDirectory(segPath, false);
            if (segDir == nullptr) {
                continue;
            }

            vector<string> indexNames;
            vector<string> attrNames;
            GetPatchIndexInfos(segDir, schema, indexNames, attrNames);
            patchMeta->AddPatchIndexs(schemaIdVec[i], segId, indexNames);
            patchMeta->AddPatchAttributes(schemaIdVec[i], segId, attrNames);
        }
    }

    if (lastPatchMeta) {
        PartitionPatchMeta::Iterator iter = lastPatchMeta->CreateIterator();
        while (iter.HasNext()) {
            SchemaPatchInfoPtr patchInfo = iter.Next();
            schemaid_t schemaId = patchInfo->GetSchemaId();
            assert(schemaId != DEFAULT_SCHEMAID);
            SchemaPatchInfo::Iterator sIter = patchInfo->Begin();
            for (; sIter != patchInfo->End(); sIter++) {
                const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
                segmentid_t segId = segMeta.GetSegmentId();
                if (!version.HasSegment(segId)) {
                    continue;
                }
                vector<string> indexNames;
                vector<string> attrNames;
                GetValidIndexAndAttribute(schema, segMeta.GetPatchIndexs(), segMeta.GetPatchAttributes(), indexNames,
                                          attrNames);
                patchMeta->AddPatchIndexs(schemaId, segId, indexNames);
                patchMeta->AddPatchAttributes(schemaId, segId, attrNames);
            }
        }
    }
    return patchMeta;
}

PartitionPatchMetaPtr
PartitionPatchMetaCreator::CreatePatchMetaForModifySchema(const file_system::DirectoryPtr& rootDirectory,
                                                          const IndexPartitionSchemaPtr& schema, const Version& version)
{
    if (schema->GetSchemaVersionId() != version.GetSchemaVersionId()) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "schema id [%d] in version not match with schema [%d]",
                             version.GetSchemaVersionId(), schema->GetSchemaVersionId());
    }

    vector<schema_opid_t> notReadyOpIds = version.GetOngoingModifyOperations();
    sort(notReadyOpIds.begin(), notReadyOpIds.end());

    vector<schema_opid_t> opIds;
    schema->GetModifyOperationIds(opIds);
    sort(opIds.begin(), opIds.end());

    vector<schema_opid_t> readyOpIds;
    set_difference(opIds.begin(), opIds.end(), notReadyOpIds.begin(), notReadyOpIds.end(),
                   inserter(readyOpIds, readyOpIds.begin()));
    sort(readyOpIds.begin(), readyOpIds.end());
    PartitionPatchMetaPtr patchMeta(new PartitionPatchMeta);
    for (auto id : readyOpIds) {
        vector<string> indexNames;
        vector<string> attrNames;
        if (!GetValidIndexAndAttributeForModifyOperation(schema, id, indexNames, attrNames)) {
            continue;
        }
        // maxOpId equal to schemaId
        string patchRootDirPath = PartitionPatchIndexAccessor::GetPatchRootDirName(id);
        if (!rootDirectory->IsExist(patchRootDirPath)) {
            bool exist = false;
            auto ec =
                FslibWrapper::IsExist(PathUtil::JoinPath(rootDirectory->GetOutputPath(), patchRootDirPath), exist);
            if (ec == file_system::ErrorCode::FSEC_OK && exist) {
                THROW_IF_FS_ERROR(rootDirectory->GetFileSystem()->MountDir(rootDirectory->GetOutputPath(),
                                                                           patchRootDirPath, patchRootDirPath,
                                                                           FSMT_READ_WRITE, true),
                                  "mount dir[%s] failed", patchRootDirPath.c_str());
            } else {
                IE_LOG(INFO, "index patch dir [%s] for modify opId [%u] not exist.", patchRootDirPath.c_str(), id);
                continue;
            }
        }
        auto patchRootDirectory = rootDirectory->GetDirectory(patchRootDirPath, false);
        if (patchRootDirectory == nullptr) {
            IE_LOG(ERROR, "get directory failed, path[%s]", patchRootDirPath.c_str());
            return PartitionPatchMetaPtr();
        }
        Version::Iterator vIter = version.CreateIterator();
        while (vIter.HasNext()) {
            segmentid_t segId = vIter.Next();
            string segPath = version.GetSegmentDirName(segId);
            if (!patchRootDirectory->IsExist(segPath)) {
                continue;
            }
            patchMeta->AddPatchIndexs(id, segId, indexNames);
            patchMeta->AddPatchAttributes(id, segId, attrNames);
        }
    }
    return patchMeta;
}

bool PartitionPatchMetaCreator::GetValidIndexAndAttributeForModifyOperation(const IndexPartitionSchemaPtr& schema,
                                                                            schema_opid_t opId,
                                                                            vector<string>& indexNames,
                                                                            vector<string>& attrNames)
{
    ModifyItemVector indexs;
    ModifyItemVector attrs;
    SchemaModifyOperationPtr modifyOp = schema->GetSchemaModifyOperation(opId);
    assert(modifyOp);
    modifyOp->CollectEffectiveModifyItem(indexs, attrs);
    if (indexs.empty() && attrs.empty()) {
        IE_LOG(INFO, "modify opId [%u] not has effective new indexs or attributes.", opId);
        return false;
    }
    for (auto& item : indexs) {
        indexNames.push_back(item->GetName());
    }
    for (auto& item : attrs) {
        attrNames.push_back(item->GetName());
    }
    return true;
}
}} // namespace indexlib::index_base
