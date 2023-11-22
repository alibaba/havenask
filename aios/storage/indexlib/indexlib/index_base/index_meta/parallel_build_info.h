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

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
namespace indexlib { namespace index_base {

class ParallelBuildInfo : public autil::legacy::Jsonizable
{
public:
    ParallelBuildInfo();
    ParallelBuildInfo(uint32_t _parallelNum, uint32_t _batch, uint32_t _instanceId);
    ~ParallelBuildInfo();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void CheckValid();
    bool IsParallelBuild() const;
    void StoreIfNotExist(const file_system::DirectoryPtr& dir); //, segmentid_t lastSegmentId);
    void Load(const file_system::DirectoryPtr& dir);
    void Load(const std::string& dirPath);
    std::string GetParallelInstancePath(const std::string& rootPath) const;
    std::string GetParallelPath(const std::string& rootPath) const;
    std::string GetParallelInstanceRelativePath() const;
    static bool IsExist(const file_system::DirectoryPtr& dir);
    // for test
    static std::string GetParallelPath(const std::string& rootPath, uint32_t parallelNum, uint32_t batchId);
    static std::string GetParallelRelativePath(uint32_t parallelNum, uint32_t batchId);
    static bool IsParallelBuildPath(const std::string& path);

    void SetBaseVersion(versionid_t _baseVersion) { baseVersion = _baseVersion; }
    versionid_t GetBaseVersion() const { return baseVersion; }

    bool operator==(const ParallelBuildInfo& info) const;
    bool operator!=(const ParallelBuildInfo& info) const { return !(*this == info); }

public:
    uint32_t parallelNum;
    uint32_t batchId;
    uint32_t instanceId;

public:
    static const std::string PARALLEL_BUILD_INFO_FILE;

private:
    versionid_t baseVersion;
    static const std::string PARALLEL_BUILD_PREFIX;
    static const std::string PARALLEL_BUILD_INSTANCE_PREFIX;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ParallelBuildInfo);
}} // namespace indexlib::index_base
