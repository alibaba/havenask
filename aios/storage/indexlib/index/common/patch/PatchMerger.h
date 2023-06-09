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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"

namespace indexlib::file_system {
class IDirectory;
class FileWriter;
} // namespace indexlib::file_system

namespace indexlibv2::index {

class PatchMerger : private autil::NoCopyable
{
public:
    using PatchFileList = std::vector<std::pair<std::string, segmentid_t>>;

public:
    PatchMerger() = default;
    virtual ~PatchMerger() = default;

public:
    virtual std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
    CreateTargetPatchFileWriter(std::shared_ptr<indexlib::file_system::IDirectory> targetSegmentDir,
                                segmentid_t srcSegmentId, segmentid_t targetSegmentId) = 0;

    virtual Status Merge(const PatchFileInfos& patchFileInfos,
                         const std::shared_ptr<indexlib::file_system::FileWriter>& targetPatchFileWriter) = 0;

protected:
    static Status CopyFile(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                           const std::string& srcFileName,
                           const std::shared_ptr<indexlib::file_system::FileWriter>& dstFileWriter);

    static std::shared_ptr<indexlib::file_system::FileWriter>
    ConvertToCompressFileWriter(const std::shared_ptr<indexlib::file_system::FileWriter>& destPatchFile,
                                bool compressPatch);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
