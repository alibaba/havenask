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
#pragma once

#include <memory>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/ImportOptions.h"

namespace indexlibv2::table {

struct InternalFileInfo : public autil::legacy::Jsonizable {
    void Jsonize(JsonWrapper& json) override
    {
        json.Jsonize("external_file_path", externalFilePath, externalFilePath);
        json.Jsonize("internal_file_path", internalFilePath, internalFilePath);
        json.Jsonize("target_segment_id", targetFileName, targetFileName);
        json.Jsonize("target_level_idx", targetFileName, targetFileName);
        json.Jsonize("target_relative_path_in_segment", targetRelativePathInSegment, targetRelativePathInSegment);
        json.Jsonize("target_file_name", targetFileName, targetFileName);
        json.Jsonize("sequence_number", sequenceNumber, sequenceNumber);
        json.Jsonize("is_dir", isDir, isDir);
        json.Jsonize("copy_file", copyFile, copyFile);
    }

    std::string externalFilePath;
    // temp file path that picked for file in op fence dir.
    std::string internalFilePath;
    std::string targetRelativePathInSegment;
    std::string targetFileName;
    segmentid_t targetSegmentId = INVALID_SEGMENTID;
    uint32_t targetLevelIdx = 0;
    uint64_t sequenceNumber = 0;
    bool isDir = false;
    bool copyFile = true;
};

class KVTableExternalFileImportJob
{
public:
    KVTableExternalFileImportJob(const std::string& workRoot, const framework::ImportExternalFileOptions& options,
                                 size_t expectShardCount);
    ~KVTableExternalFileImportJob() = default;

    Status Prepare(const std::vector<std::string>& externalFilesPaths);
    std::vector<InternalFileInfo> GetInternalFiles() const { return _internalFileInfos; }

private:
    Status Convert(const std::string& versionPath, std::vector<InternalFileInfo>* internalFiles);

    std::string _workRoot;
    framework::ImportExternalFileOptions _importOptions;
    std::vector<InternalFileInfo> _internalFileInfos;
    size_t _expectShardCount = 1;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
