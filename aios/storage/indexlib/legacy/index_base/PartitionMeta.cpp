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
#include "indexlib/legacy/index_base/PartitionMeta.h"

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlib::file_system;

using FSEC = indexlib::file_system::ErrorCode;

namespace indexlib::legacy::index_base {
AUTIL_LOG_SETUP(indexlib.index_base, PartitionMeta);

PartitionMeta::PartitionMeta() {}

PartitionMeta::PartitionMeta(const indexlibv2::config::SortDescriptions& sortDescs) : _sortDescriptions(sortDescs) {}

PartitionMeta::~PartitionMeta() {}

void PartitionMeta::Jsonize(JsonWrapper& json) { json.Jsonize("PartitionMeta", _sortDescriptions, _sortDescriptions); }

indexlibv2::config::SortPattern PartitionMeta::GetSortPattern(const string& sortField)
{
    for (size_t i = 0; i < _sortDescriptions.size(); ++i) {
        if (_sortDescriptions[i].GetSortFieldName() == sortField) {
            return _sortDescriptions[i].GetSortPattern();
        }
    }
    return indexlibv2::config::sp_nosort;
}

const indexlibv2::config::SortDescription& PartitionMeta::GetSortDescription(size_t idx) const
{
    assert(idx < _sortDescriptions.size());
    return _sortDescriptions[idx];
}

void PartitionMeta::AddSortDescription(const string& sortField, const indexlibv2::config::SortPattern& sortPattern)
{
    indexlibv2::config::SortDescription meta(sortField, sortPattern);
    _sortDescriptions.push_back(meta);
}

void PartitionMeta::AddSortDescription(const string& sortField, const string& sortPatternStr)
{
    indexlibv2::config::SortPattern sp = indexlibv2::config::SortDescription::SortPatternFromString(sortPatternStr);
    AddSortDescription(sortField, sp);
}

void PartitionMeta::AssertEqual(const PartitionMeta& other)
{
    if (_sortDescriptions.size() != other._sortDescriptions.size()) {
        INDEXLIB_THROW(util::AssertEqualException, "SortDescription Count is not equal");
    }
    for (size_t i = 0; i < _sortDescriptions.size(); i++) {
        if (_sortDescriptions[i] != other._sortDescriptions[i]) {
            stringstream ss;
            ss << "%s is not equal to %s" << _sortDescriptions[i].ToString().c_str()
               << other._sortDescriptions[i].ToString().c_str();
            INDEXLIB_THROW(util::AssertEqualException, "%s", ss.str().c_str());
        }
    }
}

bool PartitionMeta::HasSortField(const string& sortField) const
{
    for (size_t i = 0; i < _sortDescriptions.size(); i++) {
        if (_sortDescriptions[i].GetSortFieldName() == sortField) {
            return true;
        }
    }
    return false;
}

void PartitionMeta::Store(const file_system::DirectoryPtr& directory) const
{
    string jsonStr = autil::legacy::ToJsonString(*this);

    if (directory->IsExist(INDEX_PARTITION_META_FILE_NAME)) {
        AUTIL_LOG(DEBUG, "old index partition meta file exist, auto delete!");
        directory->RemoveFile(INDEX_PARTITION_META_FILE_NAME);
    }

    directory->Store(INDEX_PARTITION_META_FILE_NAME, jsonStr, WriterOption::AtomicDump());
}

void PartitionMeta::Store(const string& rootDir, FenceContext* fenceContext) const
{
    string fileName = util::PathUtil::JoinPath(rootDir, INDEX_PARTITION_META_FILE_NAME);
    string jsonStr = autil::legacy::ToJsonString(*this);
    auto ec = FslibWrapper::AtomicStore(fileName, jsonStr, true, fenceContext).Code();
    THROW_IF_FS_ERROR(ec, "atomic store failed, path[%s]", fileName.c_str());
}

void PartitionMeta::Load(const string& rootDir)
{
    string fileName = util::PathUtil::JoinPath(rootDir, INDEX_PARTITION_META_FILE_NAME);
    string jsonStr;
    auto ec = FslibWrapper::AtomicLoad(fileName, jsonStr).Code();
    if (ec == FSEC_NOENT) {
        return;
    }
    THROW_IF_FS_ERROR(ec, "atomic load failed, path[%s]", fileName.c_str());
    *this = LoadFromString(jsonStr);
}

PartitionMeta PartitionMeta::LoadFromString(const string& jsonStr)
{
    PartitionMeta partMeta;
    try {
        autil::legacy::FromJsonString(partMeta, jsonStr);
    } catch (const JsonParserError& e) {
        stringstream ss;
        ss << "Parse index partition meta file FAILED,"
           << "jsonStr: " << jsonStr << "exception: " << e.what();

        INDEXLIB_FATAL_ERROR(IndexCollapsed, "%s", ss.str().c_str());
    }
    return partMeta;
}

void PartitionMeta::Load(const file_system::DirectoryPtr& directory)
{
    string jsonStr;
    if (directory->LoadMayNonExist(INDEX_PARTITION_META_FILE_NAME, jsonStr, true)) {
        *this = LoadFromString(jsonStr);
    }
}

PartitionMeta PartitionMeta::LoadPartitionMeta(const file_system::DirectoryPtr& rootDirectory)
{
    PartitionMeta meta;
    meta.Load(rootDirectory);
    return meta;
}

bool PartitionMeta::IsExist(const string& indexRoot)
{
    string fileName = util::PathUtil::JoinPath(indexRoot, INDEX_PARTITION_META_FILE_NAME);
    bool isExist = false;
    auto ec = FslibWrapper::IsExist(fileName, isExist);
    THROW_IF_FS_ERROR(ec, "check partition meta [%s] exist failed", fileName.c_str());
    return isExist;
}
} // namespace indexlib::legacy::index_base
