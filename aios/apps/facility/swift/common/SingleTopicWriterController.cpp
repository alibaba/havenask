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
#include "swift/common/SingleTopicWriterController.h"

#include <memory>
#include <ostream>
#include <stdint.h>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/FileSystem.h"
#include "swift/common/PathDefine.h"
#include "swift/protocol/Common.pb.h"

using namespace autil;
using namespace swift::protocol;

namespace swift {
namespace common {
AUTIL_LOG_SETUP(swift, SingleTopicWriterController);

SingleTopicWriterController::SingleTopicWriterController(util::ZkDataAccessorPtr zkDataAccessor,
                                                         const std::string &zkRoot,
                                                         const std::string &topicName)
    : _zkDataAccessor(zkDataAccessor) {
    std::string zkPath = PathDefine::getPathFromZkPath(zkRoot);
    _path = fslib::fs::FileSystem::joinFilePath(common::PathDefine::getWriteVersionControlPath(zkPath), topicName);
}

SingleTopicWriterController::~SingleTopicWriterController() {}

bool SingleTopicWriterController::init() {
    std::string content;
    if (!_zkDataAccessor->getData(_path, content)) {
        AUTIL_LOG(ERROR, "check[%s] exist error", _path.c_str());
        return false;
    }
    if (content.empty()) {
        AUTIL_LOG(INFO, "version file[%s] is not exist or content empty", _path.c_str());
        return true;
    }
    TopicWriterVersionInfo versionInfo;
    if (!versionInfo.ParseFromString(content)) {
        AUTIL_LOG(ERROR, "fail to parse string[%s] to writer version", content.c_str());
        return false;
    }
    _majorVersion = versionInfo.majorversion();
    for (const auto &writerVersion : versionInfo.writerversions()) {
        _writerVersion[writerVersion.name()] = writerVersion.version();
    }
    AUTIL_LOG(INFO,
              "[%s] allow write version[%s]",
              _path.c_str(),
              getWriterVersionStr(_majorVersion, _writerVersion).c_str());
    return true;
}

ErrorInfo
SingleTopicWriterController::updateWriterVersion(const protocol::TopicWriterVersionInfo &topicWriterVersionInfo) {
    ScopedLock lock(_lock);
    ErrorInfo errorInfo;
    if (topicWriterVersionInfo.majorversion() < _majorVersion) {
        auto errorMsg = StringUtil::formatString(
            "major version[%lu] less than current[%lu]", topicWriterVersionInfo.majorversion(), _majorVersion);
        errorInfo.set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
        errorInfo.set_errmsg(errorMsg);
        return errorInfo;
    }
    std::unordered_map<std::string, uint32_t> tmpWriterVersion;
    if (topicWriterVersionInfo.majorversion() == _majorVersion) {
        tmpWriterVersion = _writerVersion;
    }
    for (const auto &writerVersion : topicWriterVersionInfo.writerversions()) {
        auto iter = tmpWriterVersion.find(writerVersion.name());
        if (iter != tmpWriterVersion.end()) {
            if (iter->second > writerVersion.version()) {
                auto errorMsg = autil::StringUtil::formatString("writer version invalid, [%s %u < %u]",
                                                                writerVersion.name().c_str(),
                                                                writerVersion.version(),
                                                                iter->second);
                errorInfo.set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
                errorInfo.set_errmsg(errorMsg);
                return errorInfo;
            }
            iter->second = writerVersion.version();
        } else {
            tmpWriterVersion[writerVersion.name()] = writerVersion.version();
        }
    }
    ErrorCode ec = updateVersionFile(topicWriterVersionInfo.majorversion(), tmpWriterVersion);
    if (ERROR_NONE == ec) {
        _majorVersion = topicWriterVersionInfo.majorversion();
        _writerVersion = tmpWriterVersion;
    }
    errorInfo.set_errcode(ec);
    return errorInfo;
}

ErrorCode
SingleTopicWriterController::updateVersionFile(uint32_t majorVersion,
                                               const std::unordered_map<std::string, uint32_t> &writerVersion) {
    protocol::TopicWriterVersionInfo wvInfo;
    wvInfo.set_majorversion(majorVersion);
    for (const auto &verison : writerVersion) {
        auto *writerVerion = wvInfo.add_writerversions();
        writerVerion->set_name(verison.first);
        writerVerion->set_version(verison.second);
    }
    if (_zkDataAccessor->writeZk(_path, wvInfo, false, false)) {
        AUTIL_LOG(INFO,
                  "update[%s] writer version[%s->%s] success",
                  _path.c_str(),
                  getWriterVersionStr(_majorVersion, _writerVersion).c_str(),
                  getWriterVersionStr(majorVersion, writerVersion).c_str());
        return ERROR_NONE;
    } else {
        AUTIL_LOG(ERROR,
                  "update[%s] writer version[%s->%s] fail, zk error",
                  _path.c_str(),
                  getWriterVersionStr(_majorVersion, _writerVersion).c_str(),
                  getWriterVersionStr(majorVersion, writerVersion).c_str());
        return ERROR_BROKER_WRITE_FILE;
    }
}

bool SingleTopicWriterController::validateThenUpdate(const std::string &name,
                                                     uint32_t majorVersion,
                                                     uint32_t minorVersion) {
    ScopedLock lock(_lock);
    if (majorVersion < _majorVersion) {
        return false;
    } else if (majorVersion > _majorVersion) {
        _writerVersion.clear();
        _majorVersion = majorVersion;
        _writerVersion[name] = minorVersion;
        return true;
    }
    auto iter = _writerVersion.find(name);
    if (iter != _writerVersion.end()) {
        if (likely(iter->second == minorVersion)) {
            return true;
        } else if (iter->second > minorVersion) {
            return false;
        } else {
            _writerVersion[name] = minorVersion;
        }
    } else {
        _writerVersion[name] = minorVersion;
    }
    return true;
}

std::string SingleTopicWriterController::getDebugVersionInfo(const std::string &writerName) {
    std::string retStr;
    uint32_t majorVersion = 0;
    uint32_t minorVersion = 0;
    {
        ScopedLock lock(_lock);
        majorVersion = _majorVersion;
        auto it = _writerVersion.find(writerName);
        if (it != _writerVersion.end()) {
            minorVersion = it->second;
        }
    }
    return StringUtil::formatString("%u-%u", majorVersion, minorVersion);
}

std::string
SingleTopicWriterController::getWriterVersionStr(uint32_t majorVersion,
                                                 const std::unordered_map<std::string, uint32_t> &writerVersion) {
    std::ostringstream oss;
    oss << majorVersion << "-";
    StringUtil::toStream(oss, writerVersion);
    return oss.str();
}

} // namespace common
} // namespace swift
