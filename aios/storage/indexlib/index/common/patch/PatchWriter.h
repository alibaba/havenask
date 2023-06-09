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

#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "indexlib/base/Status.h"
#include "indexlib/index/common/DictKeyInfo.h"

namespace indexlib { namespace file_system {
class IDirectory;
}} // namespace indexlib::file_system

namespace indexlibv2::index {
class PatchFileInfos;

class PatchWriter
{
public:
    PatchWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& workDir, segmentid_t srcSegmentId)
        : _workDir(workDir)
        , _srcSegmentId(srcSegmentId)
    {
    }
    virtual ~PatchWriter() = default;

    virtual Status Close() = 0;

    // This needs to happen after patch files are written and closed, i.e. after PatchWriter completes its task.
    virtual Status GetPatchFileInfos(PatchFileInfos* patchFileInfos) = 0;

protected:
    // _workDir is the directory where the patch files are written directly, whose logical path is something like
    // "segment_0/patch/attribute".
    std::shared_ptr<indexlib::file_system::IDirectory> _workDir;
    segmentid_t _srcSegmentId;
    // write mutex for parallel op2patch
    mutable std::mutex _writeMutex;
};

} // namespace indexlibv2::index
