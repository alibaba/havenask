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
#ifndef __INDEXLIB_PARTITION_PATCH_INDEX_ACCESSOR_H
#define __INDEXLIB_PARTITION_PATCH_INDEX_ACCESSOR_H

#include <memory>

#include "fslib/common/common_type.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/patch/partition_patch_meta.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index_base {

class PartitionPatchIndexAccessor
{
public:
    PartitionPatchIndexAccessor();
    ~PartitionPatchIndexAccessor();

public:
    void Init(const file_system::DirectoryPtr& rootDir, const index_base::Version& version);

    void SetSubSegmentDir() { mIsSub = true; }

    file_system::DirectoryPtr GetIndexDirectory(segmentid_t segmentId, const std::string& indexName,
                                                bool throwExceptionIfNotExist);

    file_system::DirectoryPtr GetAttributeDirectory(segmentid_t segmentId, const std::string& attrName,
                                                    bool throwExceptionIfNotExist);

    file_system::DirectoryPtr GetSectionAttributeDirectory(segmentid_t segmentId, const std::string& indexName,
                                                           bool throwExceptionIfNotExist);

    versionid_t GetVersionId() const { return mVersion.GetVersionId(); }

    const PartitionPatchMeta& GetPartitionPatchMeta() const { return mPatchMeta; }
    const file_system::DirectoryPtr& GetRootDirectory() const { return mRootDir; }
    const index_base::Version& GetVersion() const { return mVersion; }

public:
    static std::string GetPatchRootDirName(schemavid_t schemaId);
    static bool ExtractSchemaIdFromPatchRootDir(const std::string& rootDir, schemavid_t& schemaId);

    static void ListPatchRootDirs(const file_system::DirectoryPtr& rootDir, fslib::FileList& patchRootList);

private:
    PartitionPatchMeta mPatchMeta;
    index_base::Version mVersion;
    file_system::DirectoryPtr mRootDir;
    bool mIsSub;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionPatchIndexAccessor);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_PARTITION_PATCH_INDEX_ACCESSOR_H
