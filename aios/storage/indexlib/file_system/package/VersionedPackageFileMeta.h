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

#include <set>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/package/PackageFileMeta.h"

namespace indexlib { namespace file_system {

class VersionedPackageFileMeta : public PackageFileMeta
{
public:
    VersionedPackageFileMeta(int32_t version = 0);
    ~VersionedPackageFileMeta();

public:
    FSResult<size_t> Store(const std::string& root, const std::string& description, FenceContext* fenceContext);
    FSResult<int64_t> Load(const std::string& metaFilePath);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    int32_t GetVersionId() { return _versionId; }

public:
    static void Recognize(const std::string& description, int32_t recoverMetaId,
                          const std::vector<std::string>& fileNames, std::set<std::string>& dataFileSet,
                          std::set<std::string>& uselessMetaFileSet, std::string& recoverMetaPath) noexcept;
    static int32_t GetVersionId(const std::string& fileName);
    static bool IsValidFileName(const std::string& fileName);
    static std::string GetDescription(const std::string& fileName);
    static std::string GetPackageDataFileName(const std::string& description, uint32_t dataFileIdx);
    static std::string GetPackageMetaFileName(const std::string& description, int32_t metaVersionId);

private:
    int32_t _versionId;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<VersionedPackageFileMeta> VersionedPackageFileMetaPtr;
}} // namespace indexlib::file_system
