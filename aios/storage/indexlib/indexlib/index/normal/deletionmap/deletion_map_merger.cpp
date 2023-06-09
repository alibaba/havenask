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
#include "indexlib/index/normal/deletionmap/deletion_map_merger.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DeletionMapMerger);

void DeletionMapMerger::BeginMerge(const SegmentDirectoryBasePtr& segDir) { mSegmentDirectory = segDir; }

void DeletionMapMerger::Merge(const MergerResource& resource, const SegmentMergeInfos& segMergeInfos,
                              const OutputSegmentMergeInfos& outputSegMergeInfos)
{
    for (const auto& outputInfo : outputSegMergeInfos) {
        if ((size_t)outputInfo.targetSegmentIndex + 1 == resource.targetSegmentCount) {
            return InnerMerge(outputInfo.directory, segMergeInfos, resource.reclaimMap);
        }
    }
}

void DeletionMapMerger::SortByWeightMerge(const MergerResource& resource, const SegmentMergeInfos& segMergeInfos,
                                          const OutputSegmentMergeInfos& outputSegMergeInfos)
{
    Merge(resource, segMergeInfos, outputSegMergeInfos);
}

void DeletionMapMerger::InnerMerge(const DirectoryPtr& directory, const SegmentMergeInfos& segMergeInfos,
                                   const ReclaimMapPtr& reclaimMap)
{
    PartitionDataPtr partData = mSegmentDirectory->GetPartitionData();
    assert(partData);
    PatchFileFinderPtr patchFileFinder = PatchFileFinderCreator::Create(partData.get());

    DeletePatchInfos patchInfos;
    patchFileFinder->FindDeletionMapFiles(patchInfos);

    DeletePatchInfos::const_iterator iter;
    for (iter = patchInfos.begin(); iter != patchInfos.end(); iter++) {
        segmentid_t targetSegId = iter->first;
        const PatchFileInfo& patchInfo = iter->second;

        if (InMergePlan(targetSegId, segMergeInfos)) {
            continue;
        }

        if (InMergePlan(patchInfo.srcSegment, segMergeInfos)) {
            CopyOnePatchFile(patchInfo, directory);
        }
    }
}

bool DeletionMapMerger::InMergePlan(segmentid_t segId, const SegmentMergeInfos& segMergeInfos)
{
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        if (segMergeInfos[i].segmentId == segId) {
            return true;
        }
    }
    return false;
}

int64_t DeletionMapMerger::EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                                             const index_base::SegmentMergeInfos& segMergeInfos,
                                             const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                             bool isSortedMerge) const
{
    return 0;
}

void DeletionMapMerger::CopyOnePatchFile(const index_base::PatchFileInfo& patchFileInfo,
                                         const file_system::DirectoryPtr& directory)
{
    RemoveOption removeOption = RemoveOption::MayNonExist();
    uint32_t bufferSize = file_system::WriterOption::DEFAULT_BUFFER_SIZE;
    unique_ptr<char[]> buffer(new char[bufferSize]);

    FileReaderPtr fileReader =
        patchFileInfo.patchDirectory->CreateFileReader(patchFileInfo.patchFileName, FSOT_BUFFERED);
    directory->RemoveFile(patchFileInfo.patchFileName, removeOption);
    const file_system::FileWriterPtr& fileWriter = directory->CreateFileWriter(patchFileInfo.patchFileName);
    size_t blockCount = (fileReader->GetLength() + bufferSize - 1) / bufferSize;
    for (size_t i = 0; i < blockCount; i++) {
        size_t readSize = fileReader->Read(buffer.get(), bufferSize).GetOrThrow();
        assert(readSize > 0);
        fileWriter->Write(buffer.get(), readSize).GetOrThrow();
    }
    fileReader->Close().GetOrThrow();
    fileWriter->Close().GetOrThrow();
}
}} // namespace indexlib::index
