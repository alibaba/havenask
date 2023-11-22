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
#include "indexlib/index_base/patch/partition_patch_meta.h"

#include <algorithm>

#include "indexlib/file_system/Directory.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, PartitionPatchMeta);

void SchemaPatchInfo::PatchSegmentMeta::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("segment_id", mSegId, mSegId);
    json.Jsonize("indexs", mIndexNames, mIndexNames);
    json.Jsonize("attributes", mAttrNames, mAttrNames);
}

void SchemaPatchInfo::PatchSegmentMeta::AddIndex(const string& indexName)
{
    vector<string>::iterator iter = find(mIndexNames.begin(), mIndexNames.end(), indexName);
    if (iter == mIndexNames.end()) {
        mIndexNames.push_back(indexName);
    }
}

void SchemaPatchInfo::PatchSegmentMeta::AddAttribute(const string& attrName)
{
    vector<string>::iterator iter = find(mAttrNames.begin(), mAttrNames.end(), attrName);
    if (iter == mAttrNames.end()) {
        mAttrNames.push_back(attrName);
    }
}

void SchemaPatchInfo::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("schema_version", mSchemaId, mSchemaId);
    json.Jsonize("segment_metas", mPatchSegmentMetas);
}

SchemaPatchInfo::PatchSegmentMeta& SchemaPatchInfo::GetPatchSegmentMeta(segmentid_t segId)
{
    size_t i = 0;
    for (; i < mPatchSegmentMetas.size(); i++) {
        if (mPatchSegmentMetas[i].GetSegmentId() == segId) {
            return mPatchSegmentMetas[i];
        }
    }
    PatchSegmentMeta meta(segId);
    mPatchSegmentMetas.push_back(meta);
    return mPatchSegmentMetas[i];
}

void SchemaPatchInfo::AddIndex(segmentid_t segId, const string& indexName)
{
    PatchSegmentMeta& meta = GetPatchSegmentMeta(segId);
    meta.AddIndex(indexName);
}

void SchemaPatchInfo::AddAttribute(segmentid_t segId, const string& attrName)
{
    PatchSegmentMeta& meta = GetPatchSegmentMeta(segId);
    meta.AddAttribute(attrName);
}

void PartitionPatchMeta::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        json.Jsonize("patch_metas", mSchemaPatchInfos);
    } else {
        json.Jsonize("patch_metas", mSchemaPatchInfos);
        ResetSchemaIdMap();
    }
}

void PartitionPatchMeta::AddPatchIndexs(schemaid_t schemaId, segmentid_t segId, const vector<string>& indexNames)
{
    for (size_t i = 0; i < indexNames.size(); i++) {
        AddPatchIndex(schemaId, segId, indexNames[i]);
    }
}

void PartitionPatchMeta::AddPatchAttributes(schemaid_t schemaId, segmentid_t segId, const vector<string>& attrNames)
{
    for (size_t i = 0; i < attrNames.size(); i++) {
        AddPatchAttribute(schemaId, segId, attrNames[i]);
    }
}

void PartitionPatchMeta::AddPatchIndex(schemaid_t schemaId, segmentid_t segId, const string& indexName)
{
    if (schemaId == DEFAULT_SCHEMAID) {
        INDEXLIB_FATAL_ERROR(BadParameter, "invalid schemaId [%u] for partition patch meta", schemaId);
    }
    _Key key = make_pair(segId, indexName);
    SchemaIdMap::const_iterator iter = mIndexSchemaIdMap.find(key);
    if (iter != mIndexSchemaIdMap.end()) {
        IE_LOG(INFO, "index [%s] for segment [%d] already exist in schema id[%u]", indexName.c_str(), segId, schemaId);
        return;
    }
    SchemaPatchInfoPtr info = GetSchemaPatchInfo(schemaId);
    info->AddIndex(segId, indexName);
    mIndexSchemaIdMap[key] = schemaId;
}

void PartitionPatchMeta::AddPatchAttribute(schemaid_t schemaId, segmentid_t segId, const string& attrName)
{
    if (schemaId == DEFAULT_SCHEMAID) {
        INDEXLIB_FATAL_ERROR(BadParameter, "invalid schemaId [%u] for partition patch meta", schemaId);
    }

    _Key key = make_pair(segId, attrName);
    SchemaIdMap::const_iterator iter = mAttrSchemaIdMap.find(key);
    if (iter != mAttrSchemaIdMap.end()) {
        IE_LOG(INFO, "attribute [%s] for segment [%d] already exist in schema id[%u]", attrName.c_str(), segId,
               schemaId);
        return;
    }
    SchemaPatchInfoPtr info = GetSchemaPatchInfo(schemaId);
    info->AddAttribute(segId, attrName);
    mAttrSchemaIdMap[key] = schemaId;
}

schemaid_t PartitionPatchMeta::GetSchemaIdByIndexName(segmentid_t segId, const string& indexName) const
{
    _Key key = make_pair(segId, indexName);
    SchemaIdMap::const_iterator iter = mIndexSchemaIdMap.find(key);
    if (iter != mIndexSchemaIdMap.end()) {
        return iter->second;
    }
    return DEFAULT_SCHEMAID;
}

schemaid_t PartitionPatchMeta::GetSchemaIdByAttributeName(segmentid_t segId, const string& attrName) const
{
    _Key key = make_pair(segId, attrName);
    SchemaIdMap::const_iterator iter = mAttrSchemaIdMap.find(key);
    if (iter != mAttrSchemaIdMap.end()) {
        return iter->second;
    }
    return DEFAULT_SCHEMAID;
}

SchemaPatchInfoPtr PartitionPatchMeta::FindSchemaPatchInfo(schemaid_t schemaId) const
{
    for (size_t i = 0; i < mSchemaPatchInfos.size(); i++) {
        if (mSchemaPatchInfos[i]->GetSchemaId() == schemaId) {
            return mSchemaPatchInfos[i];
        }
    }
    return SchemaPatchInfoPtr();
}

SchemaPatchInfoPtr PartitionPatchMeta::GetSchemaPatchInfo(schemaid_t schemaId)
{
    SchemaPatchInfoPtr info = FindSchemaPatchInfo(schemaId);
    if (info) {
        return info;
    }
    info.reset(new SchemaPatchInfo(schemaId));
    mSchemaPatchInfos.push_back(info);
    return info;
}

void PartitionPatchMeta::ResetSchemaIdMap()
{
    mIndexSchemaIdMap.clear();
    mAttrSchemaIdMap.clear();
    Iterator iter = CreateIterator();
    while (iter.HasNext()) {
        SchemaPatchInfoPtr patchInfo = iter.Next();
        if (patchInfo->GetSchemaId() == DEFAULT_SCHEMAID) {
            INDEXLIB_FATAL_ERROR(BadParameter, "invalid schemaId [%u] for partition patch meta",
                                 patchInfo->GetSchemaId());
        }

        SchemaPatchInfo::Iterator sIter = patchInfo->Begin();
        for (; sIter != patchInfo->End(); sIter++) {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            const vector<string>& indexNames = segMeta.GetPatchIndexs();
            const vector<string>& attrNames = segMeta.GetPatchAttributes();
            for (size_t i = 0; i < indexNames.size(); i++) {
                _Key key = make_pair(segMeta.GetSegmentId(), indexNames[i]);
                mIndexSchemaIdMap[key] = patchInfo->GetSchemaId();
            }
            for (size_t i = 0; i < attrNames.size(); i++) {
                _Key key = make_pair(segMeta.GetSegmentId(), attrNames[i]);
                mAttrSchemaIdMap[key] = patchInfo->GetSchemaId();
            }
        }
    }
}

string PartitionPatchMeta::ToString() const { return ToJsonString(*this); }

void PartitionPatchMeta::FromString(const string& content) { FromJsonString(*this, content); }

void PartitionPatchMeta::Store(const DirectoryPtr& dir, versionid_t versionId)
{
    string fileContent = ToString();
    string fileName = GetPatchMetaFileName(versionId);
    if (dir->IsExist(fileName)) {
        dir->RemoveFile(fileName);
    }
    dir->Store(fileName, fileContent);
}

void PartitionPatchMeta::Load(const DirectoryPtr& dir, versionid_t versionId)
{
    string fileName = GetPatchMetaFileName(versionId);
    if (!dir->IsExist(fileName)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "[%s] not exist in dir[%s]!", fileName.c_str(),
                             dir->DebugString().c_str());
    }

    IE_LOG(INFO, "load partition patch meta file [%s]", fileName.c_str());
    string fileContent;
    dir->Load(fileName, fileContent, true);
    FromString(fileContent);
}

string PartitionPatchMeta::GetPatchMetaFileName(versionid_t versionId)
{
    return string(PARTITION_PATCH_META_FILE_PREFIX) + "." + StringUtil::toString(versionId);
}

void PartitionPatchMeta::GetSchemaIdsBySegmentId(segmentid_t segId, vector<schemaid_t>& schemaIds) const
{
    assert(schemaIds.empty());
    Iterator iter = CreateIterator();
    while (iter.HasNext()) {
        SchemaPatchInfoPtr schemaInfo = iter.Next();
        assert(schemaInfo);
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++) {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            if (segMeta.GetSegmentId() == segId) {
                schemaIds.push_back(schemaInfo->GetSchemaId());
                break;
            }
        }
    }
}
}} // namespace indexlib::index_base
