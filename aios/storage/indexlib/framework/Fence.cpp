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
#include "indexlib/framework/Fence.h"

#include "fslib/common/common_type.h"
#include "indexlib/file_system/ErrorCode.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, Fence);

size_t Fence::TIMESTAMP_BYTES = autil::StringUtil::toString(autil::TimeUtility::currentTimeInMicroSeconds()).size();
size_t Fence::UINT32_STR_BYTES = autil::StringUtil::toString(std::numeric_limits<uint32_t>::max()).size();
size_t Fence::UINT16_STR_BYTES = autil::StringUtil::toString(std::numeric_limits<uint16_t>::max()).size();
size_t Fence::IP_BYTES = UINT32_STR_BYTES;
size_t Fence::PORT_BYTES = UINT32_STR_BYTES;
size_t Fence::FROM_BYTES = UINT16_STR_BYTES;
size_t Fence::TO_BYTES = UINT16_STR_BYTES;

Fence::Fence(const std::string& globalRoot, const std::string& fenceName,
             const std::shared_ptr<indexlib::file_system::IFileSystem>& fileSystem)
    : _globalRoot(globalRoot)
    , _fenceName(fenceName)
    , _fileSystem(fileSystem)
{
}

Fence::~Fence()
{
    if (_leaseFile != nullptr) {
        [[maybe_unused]] auto status = _leaseFile->Close().Status();
    }
}

Status Fence::RenewFenceLease(bool createIfNotExist)
{
    std::lock_guard<std::mutex> guard(_leaseMutex);
    std::string leaseFilePath = PathUtil::JoinPath(_globalRoot, _fenceName, FENCE_LEASE_FILE_NAME);
    // Pangu EC file does not support flush or append write non-empty file.
    // make sure LEASE_FILE is not Pangu EC file
    leaseFilePath = DisablePanguECFile(leaseFilePath);
    auto [status, isExist] = indexlib::file_system::FslibWrapper::IsExist(leaseFilePath).StatusWith();
    if (!status.IsOK()) {
        return status;
    }
    if (isExist == false && createIfNotExist == false) {
        return Status::OK();
    }
    int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
    // prevent renew lease file too often.
    if (currentTime - _lastRenewTs < 60) {
        return Status::OK();
    }
    std::string currentTimeStr = std::to_string(currentTime) + "\n";
    if (_leaseFile == nullptr) {
        auto [ec, file] = indexlib::file_system::FslibWrapper::OpenFile(leaseFilePath, fslib::APPEND);
        if (ec != indexlib::file_system::FSEC_OK) {
            return Status::IOError("open renew fence lease [%s] failed", leaseFilePath.c_str());
        }
        _leaseFile = std::move(file);
    }
    status = _leaseFile->NiceWrite(currentTimeStr.c_str(), currentTimeStr.size()).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "%s, write renew fence lease [%s] failed", status.ToString().c_str(), leaseFilePath.c_str());
        return status;
    }
    status = _leaseFile->Flush().Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "%s, flush renew fence lease [%s] failed", status.ToString().c_str(), leaseFilePath.c_str());
        return status;
    }
    _lastRenewTs = currentTime;
    return Status::OK();
}

} // namespace indexlibv2::framework
