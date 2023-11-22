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
#include "build_service/admin/taskcontroller/ProcessorWriterVersion.h"

#include <assert.h>
#include <ext/alloc_traits.h>
#include <memory>
#include <type_traits>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/HippoProtoHelper.h"
#include "build_service/proto/BasicDefs.pb.h"

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ProcessorWriterVersion);

void WriterUpdateInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("updated_major_version", updatedMajorVersion, updatedMajorVersion);
    json.Jsonize("updated_minor_versions", updatedMinorVersions, updatedMinorVersions);
}
bool WriterUpdateInfo::operator==(const WriterUpdateInfo& rhs) const
{
    return updatedMajorVersion == rhs.updatedMajorVersion && updatedMinorVersions == rhs.updatedMinorVersions;
}

ProcessorWriterVersion::ProcessorWriterVersion(uint32_t majorVersion, const std::vector<uint32_t>& minorVersions,
                                               const std::vector<std::string>& identifier)
    : _majorVersion(majorVersion)
    , _minorVersions(minorVersions)
    , _identifier(identifier)
{
}

void ProcessorWriterVersion::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("partition_count", _partitionCount, _partitionCount);
    json.Jsonize("parallel_num", _parallelNum, _parallelNum);
    json.Jsonize("major_version", _majorVersion, _majorVersion);
    json.Jsonize("identifier", _identifier, _identifier);
    json.Jsonize("minor_versions", _minorVersions, _minorVersions);
}

WriterUpdateInfo ProcessorWriterVersion::forceUpdateMajorVersion(size_t workerCount)
{
    WriterUpdateInfo updateInfo;
    _minorVersions = std::vector<uint32_t>(workerCount, 1);
    _identifier = std::vector<std::string>(workerCount, "");
    _majorVersion++;
    updateInfo.updatedMajorVersion = _majorVersion;
    updateInfo.updatedMinorVersions.clear();
    for (size_t i = 0; i < workerCount; ++i) {
        updateInfo.updatedMinorVersions.push_back(std::make_pair(i, 1));
    }
    BS_LOG(INFO, "update major version to [%u], worker count [%lu]", _majorVersion, workerCount);
    return updateInfo;
}

std::optional<WriterUpdateInfo> ProcessorWriterVersion::update(const proto::ProcessorNodes& processorNodes,
                                                               uint32_t partitionCount, uint32_t parallelNum)
{
    assert(!processorNodes.empty());
    assert(partitionCount * parallelNum == processorNodes.size());
    if (_partitionCount != partitionCount || _parallelNum != parallelNum) {
        BS_LOG(INFO, "processor parallel from [%u, %u] to [%u, %u] update major version", _partitionCount, _parallelNum,
               partitionCount, parallelNum);
        _partitionCount = partitionCount;
        _parallelNum = parallelNum;
        return forceUpdateMajorVersion(processorNodes.size());
    }
    bool isUpdated = false;
    WriterUpdateInfo updateInfo;
    updateInfo.updatedMajorVersion = _majorVersion;
    for (size_t i = 0; i < processorNodes.size(); i++) {
        auto processorId = HippoProtoHelper::getNodeIdentifier(*processorNodes[i]);
        if (processorId.empty()) {
            continue;
        }
        auto pidStr = processorNodes[i]->getPartitionId().ShortDebugString();
        if (_identifier[i].empty()) {
            _identifier[i] = processorId;
            BS_LOG(INFO, "pid [%s] first identifier set, current [%u], idx [%lu], identifier [%s -> %s] total [%lu]",
                   pidStr.c_str(), _minorVersions[i], i, _identifier[i].c_str(), processorId.c_str(),
                   processorNodes.size());
            continue;
        }
        if (processorId != _identifier[i]) {
            BS_LOG(INFO, "pid [%s] minor version change, current [%u], idx [%lu], identifier [%s -> %s] total [%lu]",
                   pidStr.c_str(), _minorVersions[i], i, _identifier[i].c_str(), processorId.c_str(),
                   processorNodes.size());
            _identifier[i] = processorId;
            _minorVersions[i]++;
            isUpdated = true;
            updateInfo.updatedMinorVersions.emplace_back(i, _minorVersions[i]);
        }
    }
    if (!isUpdated) {
        return std::nullopt;
    }
    return updateInfo;
}

uint32_t ProcessorWriterVersion::getMajorVersion() const { return _majorVersion; }
uint32_t ProcessorWriterVersion::getMinorVersion(size_t idx) const
{
    assert(idx >= 0 && idx < _minorVersions.size());
    return _minorVersions[idx];
}
size_t ProcessorWriterVersion::size() const
{
    assert(_minorVersions.size() == _identifier.size());
    return _minorVersions.size();
}
const std::string& ProcessorWriterVersion::getIdentifier(size_t idx) const
{
    assert(idx >= 0 && idx < _identifier.size());
    return _identifier[idx];
}

std::string ProcessorWriterVersion::serilize() const { return autil::legacy::ToJsonString(*this); }
bool ProcessorWriterVersion::deserilize(const std::string& str)
{
    ProcessorWriterVersion other;
    try {
        FromJsonString(other, str);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "invalid version writer str [%s]", str.c_str());
        return false;
    }
    *this = other;
    return true;
}

}} // namespace build_service::admin
