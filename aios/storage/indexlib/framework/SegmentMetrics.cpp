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
#include "indexlib/framework/SegmentMetrics.h"

#include <algorithm>
#include <cstddef>

#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "indexlib/base/Constant.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace indexlib::framework {
AUTIL_LOG_SETUP(indexlib.framework, SegmentMetrics);

const string SegmentMetrics::BUILT_IN_TERM_COUNT_METRICS = "__@__term_count";
const string SegmentMetrics::SEGMENT_STATISTICS = "__@__segment_statistics";

SegmentMetrics::SegmentMetrics() {}
SegmentMetrics::~SegmentMetrics() {}

void SegmentMetrics::ListAllGroupNames(std::vector<std::string>& groupNames) const
{
    for (auto iter = _groupMap.begin(); iter != _groupMap.end(); iter++) {
        groupNames.push_back(iter->first);
    }
}

void SegmentMetrics::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { json.Jsonize("groups", _groupMap); }

size_t SegmentMetrics::GetDistinctTermCount(const std::string& indexName) const
{
    size_t ret = 0;
    Get<size_t>(BUILT_IN_TERM_COUNT_METRICS, indexName, ret);
    return ret;
}

void SegmentMetrics::SetKeyCount(size_t keyCount) { Set<size_t>(BUILT_IN_TERM_COUNT_METRICS, "KEY_COUNT", keyCount); }

bool SegmentMetrics::GetKeyCount(size_t& keyCount) const
{
    size_t ret = 0;
    ret = Get<size_t>(BUILT_IN_TERM_COUNT_METRICS, "KEY_COUNT", keyCount);
    return ret;
}

void SegmentMetrics::SetDistinctTermCount(const std::string& indexName, size_t distinctTermCount)
{
    Set<size_t>(BUILT_IN_TERM_COUNT_METRICS, indexName, distinctTermCount);
}

string SegmentMetrics::ToString() const { return json::ToString(_groupMap); }

void SegmentMetrics::FromString(const string& str)
{
    Any a = json::ParseJson(str);
    FromJson(_groupMap, a);
}

void SegmentMetrics::Store(const std::shared_ptr<file_system::Directory>& directory) const
{
    file_system::RemoveOption removeOption = file_system::RemoveOption::MayNonExist();
    const string& content = ToString();
    directory->RemoveFile(SEGMETN_METRICS_FILE_NAME, removeOption);
    directory->Store(SEGMETN_METRICS_FILE_NAME, content, file_system::WriterOption::AtomicDump());
}

Status SegmentMetrics::StoreSegmentMetrics(const std::shared_ptr<file_system::IDirectory>& directory) const
{
    const string& content = ToString();
    RETURN_IF_STATUS_ERROR(
        directory->RemoveFile(SEGMETN_METRICS_FILE_NAME, file_system::RemoveOption::MayNonExist()).Status(),
        "remove old segment metrics failed");
    return directory->Store(SEGMETN_METRICS_FILE_NAME, content, file_system::WriterOption::AtomicDump()).Status();
}

Status SegmentMetrics::LoadSegmentMetrics(const std::shared_ptr<file_system::IDirectory>& directory)
{
    auto result = directory->IsExist(SEGMETN_METRICS_FILE_NAME);
    RETURN_IF_STATUS_ERROR(result.Status(), "check segment metrics exist failed");
    if (!result.result) {
        // legacy code for no segment metrics
        return Status::OK();
    }
    auto readerOption = file_system::ReaderOption::PutIntoCache(file_system::FSOT_MEM);
    string content;
    auto loadResult = directory->Load(SEGMETN_METRICS_FILE_NAME, readerOption, content);
    RETURN_IF_STATUS_ERROR(loadResult.Status(), "load segment metrics failed");
    try {
        FromString(content);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "parse segment metrics failed [%s]", e.what());
        return Status::Corruption();
    }
    return Status::OK();
}

Status SegmentMetrics::Load(const std::string& filePath)
{
    std::string content;
    auto st = indexlib::file_system::FslibWrapper::AtomicLoad(filePath, content);
    if (!st.OK()) {
        return st.Status();
    }
    try {
        FromString(content);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "parse segment metrics failed [%s]", e.what());
        return Status::Corruption();
    }
    return Status::OK();
}

void SegmentMetrics::Load(const std::shared_ptr<file_system::Directory>& directory)
{
    string content;
    directory->Load(SEGMETN_METRICS_FILE_NAME, content, true);
    FromString(content);
}

bool SegmentMetrics::MergeSegmentGroupMetrics(const std::string groupName, const Group& otherGroup)
{
    GroupMap::iterator groupIt = _groupMap.find(groupName);
    if (groupIt == _groupMap.end()) {
        AUTIL_LOG(ERROR, "group name not found, merge segment group metrics failed");
        return false;
    }
    Group& group = *autil::legacy::AnyCast<Group>(&groupIt->second);
    for (auto iter = otherGroup.begin(); iter != otherGroup.end(); iter++) {
        size_t otherValue;
        autil::legacy::FromJson(otherValue, iter->second);
        if (group.find(iter->first) != group.end()) {
            size_t value;
            autil::legacy::FromJson(value, group.at(iter->first));
            const autil::legacy::Any& anyValue = autil::legacy::ToJson(value + otherValue);
            group[iter->first] = anyValue;
        } else {
            const autil::legacy::Any& anyValue = autil::legacy::ToJson(otherValue);
            group[iter->first] = anyValue;
        }
    }
    return true;
}

bool SegmentMetrics::MergeSegmentMetrics(const SegmentMetrics& otherSegmentMetrics)
{
    if (IsEmpty()) {
        FromString(otherSegmentMetrics.ToString());
        return true;
    }
    vector<string> groupNames;
    otherSegmentMetrics.ListAllGroupNames(groupNames);
    for (const auto& groupName : groupNames) {
        GroupMap::iterator groupIt = _groupMap.find(groupName);
        Group otherGroup;
        if (!otherSegmentMetrics.GetSegmentGroup(groupName, otherGroup)) {
            AUTIL_LOG(ERROR, "get segment group [%s] failed", groupName.c_str());
            return false;
        }
        if (groupIt != _groupMap.end()) {
            if (groupName == BUILT_IN_TERM_COUNT_METRICS) {
                if (!MergeSegmentGroupMetrics(groupName, otherGroup)) {
                    AUTIL_LOG(ERROR, "merge segment group [%s] failed", groupName.c_str());
                    return false;
                }
            } else {
                AUTIL_LOG(ERROR, "not support merge same group name's segment metrics");
                return false;
            }
        } else {
            SetSegmentGroupMetrics(groupName, otherGroup);
        }
    }
    return true;
}
} // namespace indexlib::framework
