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
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"

#include "autil/StringUtil.h"
#include "indexlib/config/section_attribute_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace fslib;
using namespace autil;

using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, PartitionPatchIndexAccessor);

PartitionPatchIndexAccessor::PartitionPatchIndexAccessor() : mIsSub(false) {}

PartitionPatchIndexAccessor::~PartitionPatchIndexAccessor() {}

void PartitionPatchIndexAccessor::Init(const DirectoryPtr& rootDir, const Version& version)
{
    mRootDir = rootDir;
    mVersion = version;
    if (version.GetSchemaVersionId() == DEFAULT_SCHEMAID || version.GetVersionId() == INVALID_VERSION) {
        return;
    }

    string patchFileName = PartitionPatchMeta::GetPatchMetaFileName(version.GetVersionId());
    if (!rootDir->IsExist(patchFileName)) {
        IE_LOG(INFO, "[%s] not exist in [%s]", patchFileName.c_str(), rootDir->DebugString().c_str());
        return;
    }
    mPatchMeta.Load(rootDir, version.GetVersionId());
}

DirectoryPtr PartitionPatchIndexAccessor::GetAttributeDirectory(segmentid_t segmentId, const string& attrName,
                                                                bool throwExceptionIfNotExist)
{
    schemavid_t schemaId = mPatchMeta.GetSchemaIdByAttributeName(segmentId, attrName);
    if (schemaId == DEFAULT_SCHEMAID) {
        return DirectoryPtr();
    }

    string patchDirName = GetPatchRootDirName(schemaId);
    string segPath = PathUtil::JoinPath(patchDirName, mVersion.GetSegmentDirName(segmentId));
    if (mIsSub) {
        segPath = PathUtil::JoinPath(segPath, SUB_SEGMENT_DIR_NAME);
    }
    string attrDirPath = PathUtil::JoinPath(segPath, ATTRIBUTE_DIR_NAME);
    string attrPath = PathUtil::JoinPath(attrDirPath, attrName);
    IE_LOG(INFO, "attribute [%s] in segment [%d] in patch index path [%s]", attrName.c_str(), segmentId,
           attrPath.c_str());
    return mRootDir->GetDirectory(attrPath, throwExceptionIfNotExist);
}

DirectoryPtr PartitionPatchIndexAccessor::GetIndexDirectory(segmentid_t segmentId, const string& indexName,
                                                            bool throwExceptionIfNotExist)
{
    schemavid_t schemaId = mPatchMeta.GetSchemaIdByIndexName(segmentId, indexName);
    if (schemaId == DEFAULT_SCHEMAID) {
        return DirectoryPtr();
    }

    string patchDirName = GetPatchRootDirName(schemaId);
    string segPath = PathUtil::JoinPath(patchDirName, mVersion.GetSegmentDirName(segmentId));
    if (mIsSub) {
        segPath = PathUtil::JoinPath(segPath, SUB_SEGMENT_DIR_NAME);
    }
    string indexDirPath = PathUtil::JoinPath(segPath, INDEX_DIR_NAME);
    string indexPath = PathUtil::JoinPath(indexDirPath, indexName);
    IE_LOG(INFO, "index [%s] in segment [%d] in patch index path [%s]", indexName.c_str(), segmentId,
           indexPath.c_str());
    return mRootDir->GetDirectory(indexPath, throwExceptionIfNotExist);
}

DirectoryPtr PartitionPatchIndexAccessor::GetSectionAttributeDirectory(segmentid_t segmentId, const string& indexName,
                                                                       bool throwExceptionIfNotExist)
{
    schemavid_t schemaId = mPatchMeta.GetSchemaIdByIndexName(segmentId, indexName);
    if (schemaId == DEFAULT_SCHEMAID) {
        return DirectoryPtr();
    }

    string patchDirName = GetPatchRootDirName(schemaId);
    string segPath = PathUtil::JoinPath(patchDirName, mVersion.GetSegmentDirName(segmentId));
    if (mIsSub) {
        segPath = PathUtil::JoinPath(segPath, SUB_SEGMENT_DIR_NAME);
    }
    string indexDirPath = PathUtil::JoinPath(segPath, INDEX_DIR_NAME);
    string sectionAttrName = SectionAttributeConfig::IndexNameToSectionAttributeName(indexName);
    string indexPath = PathUtil::JoinPath(indexDirPath, sectionAttrName);
    IE_LOG(INFO, "index section attribute [%s] in segment [%d] in patch index path [%s]", sectionAttrName.c_str(),
           segmentId, indexPath.c_str());
    return mRootDir->GetDirectory(indexPath, throwExceptionIfNotExist);
}

string PartitionPatchIndexAccessor::GetPatchRootDirName(schemavid_t schemaId)
{
    if (schemaId == DEFAULT_SCHEMAID) {
        INDEXLIB_FATAL_ERROR(BadParameter, "schema id [%u] not valid for patch index", schemaId);
    }
    return PATCH_INDEX_DIR_PREFIX + StringUtil::toString(schemaId);
}

bool PartitionPatchIndexAccessor::ExtractSchemaIdFromPatchRootDir(const string& rootDir, schemavid_t& schemaId)
{
    if (rootDir.find(PATCH_INDEX_DIR_PREFIX) != 0) {
        return false;
    }
    string schemaIdStr = rootDir.substr(PATCH_INDEX_DIR_PREFIX.size());
    return StringUtil::fromString(schemaIdStr, schemaId);
}

void PartitionPatchIndexAccessor::ListPatchRootDirs(const DirectoryPtr& rootDir, FileList& patchRootList)
{
    patchRootList.clear();
    FileList fileList;
    rootDir->ListDir("", fileList, false);

    for (size_t i = 0; i < fileList.size(); i++) {
        schemavid_t schemaId;
        if (PartitionPatchIndexAccessor::ExtractSchemaIdFromPatchRootDir(fileList[i], schemaId)) {
            if (schemaId != DEFAULT_SCHEMAID) {
                patchRootList.push_back(fileList[i]);
            }
        }
    }
}
}} // namespace indexlib::index_base
