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
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index_base {

class IndexFormatVersion : public autil::legacy::Jsonizable
{
public:
    IndexFormatVersion(const std::string& version = std::string());
    ~IndexFormatVersion();

public:
    void Set(const std::string& version);

    // can't be removed currently, used in multi_partition_merger.cpp
    bool Load(const std::string& path, bool mayNonExist = false);
    bool Load(const file_system::DirectoryPtr& directory, bool mayNonExist = false);
    // can't be removed currently, used in multi_partition_merger.cpp
    void Store(const std::string& path, file_system::FenceContext* fenceContext);
    void Store(const file_system::DirectoryPtr& directory);

    bool operator==(const IndexFormatVersion& other) const;
    bool operator!=(const IndexFormatVersion& other) const;

    void CheckCompatible(const std::string& binaryVersion);
    bool IsLegacyFormat() const;

    static IndexFormatVersion LoadFromString(const std::string& content);
    static void StoreBinaryVersion(const file_system::DirectoryPtr& rootDirectory);

    bool operator<(const IndexFormatVersion& other) const;
    bool operator>(const IndexFormatVersion& other) const { return *this == other ? false : !(*this < other); }

    bool operator<=(const IndexFormatVersion& other) const { return *this < other || *this == other; }

    bool operator>=(const IndexFormatVersion& other) const { return !(*this < other); }

    format_versionid_t GetInvertedIndexBinaryFormatVersion() const { return mInvertedIndexFormatVersion; }

protected:
    void Jsonize(JsonWrapper& json) override;

private:
    void SplitVersionIds(const std::string& versionStr, std::vector<int32_t>& versionIds) const;

private:
    std::string mIndexFormatVersion;
    format_versionid_t mInvertedIndexFormatVersion;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexFormatVersion);
}} // namespace indexlib::index_base
