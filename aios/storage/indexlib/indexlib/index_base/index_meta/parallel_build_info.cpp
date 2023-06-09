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
#include "indexlib/index_base/index_meta/parallel_build_info.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"
using namespace std;
using namespace autil::legacy;
using namespace autil;

using namespace indexlib::file_system;
namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, ParallelBuildInfo);

const std::string ParallelBuildInfo::PARALLEL_BUILD_INFO_FILE = "parallel_build_info_file";
const std::string ParallelBuildInfo::PARALLEL_BUILD_PREFIX = "parallel_";
const std::string ParallelBuildInfo::PARALLEL_BUILD_INSTANCE_PREFIX = "instance_";
ParallelBuildInfo::ParallelBuildInfo() : parallelNum(0u), batchId(0u), instanceId(0u), baseVersion(INVALID_VERSION) {}

ParallelBuildInfo::ParallelBuildInfo(uint32_t _parallelNum, uint32_t _batchId, uint32_t _instanceId)
    : parallelNum(_parallelNum)
    , batchId(_batchId)
    , instanceId(_instanceId)
    , baseVersion(INVALID_VERSION)
{
}

ParallelBuildInfo::~ParallelBuildInfo() {}

void ParallelBuildInfo::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("parallel_num", parallelNum, parallelNum);
    json.Jsonize("instance_id", instanceId, instanceId);
    json.Jsonize("batch_id", batchId, batchId);
    json.Jsonize("base_version", baseVersion, baseVersion);
}

bool ParallelBuildInfo::IsParallelBuild() const
{
    // return !(1 == parallelNum && 0 == instanceId && parentDir.empty());
    return parallelNum || instanceId;
}

void ParallelBuildInfo::CheckValid()
{
    if (!IsParallelBuild()) {
        return;
    }

    if (parallelNum < 1 || instanceId >= parallelNum) {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "parallel build instanceid or parallelnum is incorrect. "
                             "parallelNum [%u], instance id[%u]",
                             parallelNum, instanceId);
    }
}

bool ParallelBuildInfo::IsExist(const file_system::DirectoryPtr& dir) { return dir->IsExist(PARALLEL_BUILD_INFO_FILE); }

void ParallelBuildInfo::Load(const file_system::DirectoryPtr& dir)
{
    if (IsExist(dir)) {
        std::string content;
        dir->Load(PARALLEL_BUILD_INFO_FILE, content);
        FromJsonString(*this, content);
    } else {
        INDEXLIB_FATAL_ERROR(NonExist, "file [%s/%s] not exist!", dir->DebugString().c_str(),
                             PARALLEL_BUILD_INFO_FILE.c_str());
    }
}

void ParallelBuildInfo::Load(const std::string& dirPath)
{
    string filePath = FslibWrapper::JoinPath(dirPath, PARALLEL_BUILD_INFO_FILE);
    std::string content;
    auto ec = FslibWrapper::AtomicLoad(filePath, content).Code();
    THROW_IF_FS_ERROR(ec, "load parallel build info file from [%s] failed", dirPath.c_str());
    FromJsonString(*this, content);
}

std::string ParallelBuildInfo::GetParallelInstancePath(const string& rootPath) const
{
    return util::PathUtil::JoinPath(rootPath, GetParallelInstanceRelativePath());
}

std::string ParallelBuildInfo::GetParallelPath(const string& rootPath) const
{
    return util::PathUtil::JoinPath(rootPath, GetParallelRelativePath(parallelNum, batchId));
}

std::string ParallelBuildInfo::GetParallelInstanceRelativePath() const
{
    return util::PathUtil::JoinPath(GetParallelRelativePath(parallelNum, batchId),
                                    PARALLEL_BUILD_INSTANCE_PREFIX + StringUtil::toString(instanceId));
}

std::string ParallelBuildInfo::GetParallelRelativePath(uint32_t parallelNum, uint32_t batchId)
{
    return PARALLEL_BUILD_PREFIX + StringUtil::toString(parallelNum) + "_" + StringUtil::toString(batchId);
}

std::string ParallelBuildInfo::GetParallelPath(const string& rootPath, uint32_t parallelNum, uint32_t batchId)
{
    return ParallelBuildInfo(parallelNum, batchId, 0).GetParallelPath(rootPath);
}

bool ParallelBuildInfo::operator==(const ParallelBuildInfo& other) const
{
    return parallelNum == other.parallelNum && batchId == other.batchId && instanceId == other.instanceId &&
           baseVersion == other.baseVersion;
}

void ParallelBuildInfo::StoreIfNotExist(const file_system::DirectoryPtr& dir)
{
    if (!IsExist(dir)) {
        // TODO(qingran & zhenqi) replace with new policy
        dir->Store(PARALLEL_BUILD_INFO_FILE, ToJsonString(*this), file_system::WriterOption::BufferAtomicDump());
    } else {
        ParallelBuildInfo info;
        info.Load(dir);
        if (*this != info) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "parallelBuildInfo [%s] not equal to local file [%s]!",
                                 ToJsonString(*this).c_str(), ToJsonString(info).c_str());
        }
    }
}

bool ParallelBuildInfo::IsParallelBuildPath(const std::string& path)
{
    // start with paralll_
    if (0 != path.find(PARALLEL_BUILD_PREFIX)) {
        return false;
    }
    std::string numberString = path.substr(PARALLEL_BUILD_PREFIX.length());
    vector<std::string> numbers = StringUtil::split(numberString, "_");
    if (2 != numbers.size()) {
        return false;
    }
    uint32_t parallelNum;
    uint32_t batchId;
    return StringUtil::fromString(numbers[0], parallelNum) && StringUtil::fromString(numbers[1], batchId);
}
}} // namespace indexlib::index_base
