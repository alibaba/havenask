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
#include <bits/stdint-uintn.h>
#include <cstddef>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/URLParser.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/TabletId.h"
#include "indexlib/util/Base62.h"
#include "indexlib/util/EpochIdUtil.h"
#include "indexlib/util/IpConvertor.h"

namespace indexlib::file_system {
class IFileSystem;
} // namespace indexlib::file_system

namespace indexlibv2::framework {

class Fence
{
public:
    struct FenceMeta : public autil::legacy::Jsonizable {
        std::string fenceName;
        std::string address = "0.0.0.0:0";
        int64_t timestamp = 0;
        std::pair<uint16_t, uint16_t> range = {0, 65535};
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("fence_name", fenceName, fenceName);
            json.Jsonize("timestamp", timestamp, timestamp);
            json.Jsonize("address", address, address);
            json.Jsonize("from", range.first, range.first);
            json.Jsonize("to", range.second, range.second);
        }
    };
    static std::string GetFenceName(const std::string& epochId)
    {
        // eg. __FENCE__1234567
        static std::string prefix = FENCE_DIR_NAME_PREFIX;
        return prefix + epochId;
    }
    static std::string GetTaskFenceName(const std::string& taskEpochId, const std::string& epochId)
    {
        // eg. __FENCE__TASK_12346_1234567
        static std::string taskPrefix = TASK_FENCE_DIR_NAME_PREFIX;
        return GetFenceName(taskPrefix + taskEpochId + "_" + epochId);
    }
    static bool IsFenceName(const std::string& name)
    {
        static std::string prefix = FENCE_DIR_NAME_PREFIX;
        return autil::StringUtil::startsWith(name, prefix);
    }

    static std::string GetFenceNameFromSegDir(const std::string& path)
    {
        auto parentDir = PathUtil::GetParentDirPath(path);
        auto [rootDir, fenceDir] = PathUtil::ExtractFenceName(parentDir);
        return fenceDir;
    }
    static std::string GetPrivateFenceName() { return GetFenceName(RT_INDEX_PARTITION_DIR_NAME); }

    static std::string GenerateNewFenceName(bool isPublicFence, const indexlib::framework::TabletId& tid)
    {
        return GenerateNewFenceName(isPublicFence, autil::TimeUtility::currentTimeInMicroSeconds(), tid);
    }

    static std::pair<bool, FenceMeta> DecodePublicFence(const std::string& fenceName)
    {
        if (!IsFenceName(fenceName)) {
            return std::make_pair(false, FenceMeta());
        }
        FenceMeta fenceMeta;
        size_t fencePrefixLen = std::strlen(FENCE_DIR_NAME_PREFIX);
        std::string epochIdBase62 = fenceName.substr(fencePrefixLen, fenceName.size() - fencePrefixLen);
        std::string epochId;
        if (!util::Base62::DecodeInteger(epochIdBase62, epochId).IsOK()) {
            return std::make_pair(false, FenceMeta());
        }
        if (epochId.size() < TIMESTAMP_BYTES) {
            return std::make_pair(false, FenceMeta());
        }

        if (!autil::StringUtil::fromString(epochId.substr(0, TIMESTAMP_BYTES).c_str(), fenceMeta.timestamp)) {
            return std::make_pair(false, FenceMeta());
        }
        if (epochId.size() == TIMESTAMP_BYTES) {
            // pass
        } else if (epochId.size() > TIMESTAMP_BYTES && epochId.size() <= TIMESTAMP_BYTES + IP_BYTES + PORT_BYTES) {
            // legacy instance id is not filled with zero
            uint64_t ipPortNum = 0;
            if (!autil::StringUtil::fromString(
                    epochId.substr(TIMESTAMP_BYTES, epochId.size() - TIMESTAMP_BYTES).c_str(), ipPortNum)) {
                return std::make_pair(false, FenceMeta());
            }
            fenceMeta.address = util::IpConvertor::NumToIpPort(ipPortNum);
        } else if (epochId.size() == TIMESTAMP_BYTES + IP_BYTES + PORT_BYTES + FROM_BYTES + TO_BYTES) {
            uint64_t ipPortNum = 0;
            if (!autil::StringUtil::fromString(epochId.substr(TIMESTAMP_BYTES, IP_BYTES + PORT_BYTES).c_str(),
                                               ipPortNum)) {
                return std::make_pair(false, FenceMeta());
            }
            fenceMeta.address = util::IpConvertor::NumToIpPort(ipPortNum);
            if (!autil::StringUtil::fromString(
                    epochId.substr(TIMESTAMP_BYTES + IP_BYTES + PORT_BYTES, FROM_BYTES).c_str(),
                    fenceMeta.range.first)) {
                return std::make_pair(false, FenceMeta());
            }
            if (!autil::StringUtil::fromString(
                    epochId.substr(TIMESTAMP_BYTES + IP_BYTES + PORT_BYTES + FROM_BYTES, TO_BYTES).c_str(),
                    fenceMeta.range.second)) {
                return std::make_pair(false, FenceMeta());
            }
        } else {
            // use default value
        }
        fenceMeta.fenceName = fenceName;
        return std::make_pair(true, fenceMeta);
    }

    static Status GetLatestLeaseTs(const std::string& rootDir, const std::string& fenceName, int64_t* ts)
    {
        std::string fenceFilePath = PathUtil::JoinPath(rootDir, fenceName, FENCE_LEASE_FILE_NAME);
        auto [status, isExist] = indexlib::file_system::FslibWrapper::IsExist(fenceFilePath).StatusWith();
        if (!status.IsOK()) {
            return status;
        }
        if (!isExist) {
            (*ts) = 0;
            return Status::OK();
        }
        std::string content;
        status = indexlib::file_system::FslibWrapper::Load(fenceFilePath, content).Status();
        if (!status.IsOK()) {
            return status;
        }
        std::string value;
        FindLastLine(content, value);
        // make sure time is in seconds.
        if (value.length() != 10u) {
            return Status::InternalError("invalid value [%s] was read from lease file", value.c_str());
        }
        if (!autil::StringUtil::fromString(value, *ts)) {
            return Status::InternalError("invalid value [%s] was read from lease file", value.c_str());
        }
        return Status::OK();
    }

    Fence() = default;
    Fence(const std::string& globalRoot, const std::string& fenceName,
          const std::shared_ptr<indexlib::file_system::IFileSystem>& fileSystem);
    ~Fence();

    Fence(const Fence& other)
    {
        _lastRenewTs = other._lastRenewTs;
        _globalRoot = other._globalRoot;
        _fenceName = other._fenceName;
        _fileSystem = other._fileSystem;
    }

    Fence(Fence&& other)
    {
        _lastRenewTs = other._lastRenewTs;
        _globalRoot = other._globalRoot;
        _fenceName = other._fenceName;
        _fileSystem = other._fileSystem;
    }

    Fence& operator=(const Fence& other)
    {
        _lastRenewTs = other._lastRenewTs;
        _globalRoot = other._globalRoot;
        _fenceName = other._fenceName;
        _fileSystem = other._fileSystem;
        return *this;
    }

    const std::string& GetGlobalRoot() const { return _globalRoot; }
    const std::string& GetFenceName() const { return _fenceName; }
    std::shared_ptr<indexlib::file_system::IFileSystem> GetFileSystem() const { return _fileSystem; }

    Status RenewFenceLease(bool createIfNotExist);

private:
    // public fence name contains three parts [timestamp][ip:port][range] with base62 encode
    static std::string GenerateNewFenceName(bool isPublicFence, int64_t timestamp,
                                            const indexlib::framework::TabletId& tid)
    {
        if (isPublicFence) {
            std::string epochId = indexlib::util::EpochIdUtil::GenerateEpochId(timestamp);
            if (tid.GetIp() == "0.0.0.0" && tid.GetPort() == 0 &&
                tid.GetRange() == std::make_pair<uint16_t, uint16_t>(0, 65535)) {
                // do nothing
            } else {
                uint64_t ipPortNum;
                std::string address = tid.GetIp() + ":" + std::to_string(tid.GetPort());
                if (!util::IpConvertor::IpPortToNum(address, ipPortNum)) {
                    ipPortNum = 0;
                }
                epochId += FillZeroAndToStr(ipPortNum);
                auto range = tid.GetRange();
                uint16_t from = range.first;
                uint16_t to = range.second;
                if (!(from == 0 && to == 65535)) {
                    epochId += FillZeroAndToStr(from);
                    epochId += FillZeroAndToStr(to);
                }
            }
            std::string str;
            auto status = util::Base62::EncodeInteger(epochId, str);
            assert(status.IsOK());
            return GetFenceName(str);
        } else {
            return GetPrivateFenceName();
        }
    }

    static std::string DisablePanguECFile(const std::string& filePath)
    {
        auto fsType = fslib::fs::FileSystem::getFsType(filePath);
        if (fsType != "pangu" && fsType != "dfs") {
            return filePath;
        }
        std::string pathWithParam(filePath);
        pathWithParam = fslib::util::URLParser::appendParam(pathWithParam, "ec", "false", /*overwrite=*/true);
        if (pathWithParam == "") {
            return filePath;
        }
        pathWithParam = fslib::util::URLParser::appendParam(pathWithParam, "rep", "3", /*overwrite=*/true);
        if (pathWithParam == "") {
            return filePath;
        }
        return pathWithParam;
    }

    static void FindLastLine(const std::string& text, std::string& str)
    {
        std::string sep("\n");
        size_t n = std::string::npos;
        size_t old = text.size();
        do {
            n = text.rfind(sep, n);
            if (n != std::string::npos && n + 1 != old) {
                str = text.substr(n + sep.length(), old - (n + sep.length()));
                return;
            }
            if (n != std::string::npos) {
                old = n;
                n -= sep.length();
            }
        } while (n != std::string::npos);
        if (n == std::string::npos) {
            str = text.substr(0, old);
        }
    }

    template <typename T>
    static std::string FillZeroAndToStr(T num)
    {
        std::stringstream ss;
        ss << std::setw(autil::StringUtil::toString(std::numeric_limits<T>::max()).size()) << std::setfill('0') << num;
        return ss.str();
    }

    static size_t TIMESTAMP_BYTES;
    static size_t UINT32_STR_BYTES;
    static size_t UINT16_STR_BYTES;
    static size_t IP_BYTES;
    static size_t PORT_BYTES;
    static size_t FROM_BYTES;
    static size_t TO_BYTES;

    mutable std::mutex _leaseMutex;
    int64_t _lastRenewTs = 0;
    std::string _globalRoot;
    std::string _fenceName;
    std::shared_ptr<indexlib::file_system::IFileSystem> _fileSystem;
    std::unique_ptr<indexlib::file_system::FslibFileWrapper> _leaseFile;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
