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
#include "indexlib/partition/deploy_index_validator.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/patch/partition_patch_meta.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, DeployIndexValidator);

void DeployIndexValidator::ValidateDeploySegments(const DirectoryPtr& rootDir, const Version& version)
{
    IE_LOG(INFO, "Begin validate path [%s], version [%d]", rootDir->DebugString().c_str(), version.GetVersionId());
    assert(rootDir);
    // Version::Iterator it = version.CreateIterator();
    // while (it.HasNext())
    // {
    //     segmentid_t curSegId = it.Next();
    //     string segDirName = version.GetSegmentDirName(curSegId);
    //     ValidateSingleSegmentDeployFiles(rootDir->GetDirectory(segDirName, true));
    // }
    // ValidatePartitionPatchSegmentDeployFiles(rootDir, version);
    // IE_LOG(INFO, "End validate path [%s], version [%d]",
    //        rootDir->DebugString().c_str(), version.GetVersionId());
}

void DeployIndexValidator::ValidateSingleSegmentDeployFiles(const DirectoryPtr& segDir)
{
    // TODO(xingwo): Validate dp files

    // assert(segDir);
    // IndexFileList indexFileList;
    // if (!SegmentFileListWrapper::Load(segDir, indexFileList))
    // {
    //     INDEXLIB_FATAL_ERROR(FileIO, "deploy index file is missing in segment[%s]",
    //                          segDir->DebugString().c_str());
    // }
    // for (const FileInfo& fileInfo : indexFileList.deployFileMetas)
    // {
    //     segDir->Validate(fileInfo.filePath, fileInfo.fileLength);
    // }
}

void DeployIndexValidator::ValidatePartitionPatchSegmentDeployFiles(const DirectoryPtr& rootDir, const Version& version)
{
    if (version.GetSchemaVersionId() == DEFAULT_SCHEMAID) {
        return;
    }

    PartitionPatchMeta patchMeta;
    patchMeta.Load(rootDir, version.GetVersionId());
    PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
    while (metaIter.HasNext()) {
        SchemaPatchInfoPtr schemaInfo = metaIter.Next();
        assert(schemaInfo);
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++) {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            if (!version.HasSegment(segMeta.GetSegmentId())) {
                continue;
            }
            string patchSegPath =
                util::PathUtil::JoinPath(PartitionPatchIndexAccessor::GetPatchRootDirName(schemaInfo->GetSchemaId()),
                                         version.GetSegmentDirName(segMeta.GetSegmentId()));
            ValidateSingleSegmentDeployFiles(rootDir->GetDirectory(patchSegPath, true));
        }
    }
}
}} // namespace indexlib::partition
