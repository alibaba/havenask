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
#include "swift/util/MultiProcessUtil.h"

#include "autil/StringUtil.h"
#include "hippo/DriverCommon.h"

using namespace swift::protocol;
using namespace autil;

namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, MultiProcessUtil);

const std::string MultiProcessUtil::SWIFT_BROKER_DIR = "${HIPPO_APP_WORKDIR}/swift_broker/";
const std::string MultiProcessUtil::SWIFT_ADMIN_DIR = "${HIPPO_APP_WORKDIR}/swift_admin/";
const std::string MultiProcessUtil::SWIFT_BROKER_ROLE_NAME_SUFFIX = "_broker";
const std::string MultiProcessUtil::SWIFT_ADMIN_ROLE_NAME_SUFFIX = "_admin";

const std::string MultiProcessUtil::LOG_SENDER_CMD = "log_sender";
const std::string MultiProcessUtil::DEFAULT_SEPARATOR = "#";

const std::string MultiProcessUtil::ENV_SEPARATOR = "separator";
const std::string MultiProcessUtil::ENV_SWIFT_TOPIC_PREFIX = "swiftTopicPrefix";
const std::string MultiProcessUtil::ENV_SWIFT_LOG_SUFFIX = "swiftLogSuffix";
const std::string MultiProcessUtil::ENV_SWIFT_LOG_DIR_SUFFIX = "swiftLogDirSuffix";
const std::string MultiProcessUtil::ENV_TOPIC_NAME = "topicName";
const std::string MultiProcessUtil::ENV_STREAM_ACCESS_LOG_PATH = "streamAccessLogPath";
const std::string MultiProcessUtil::ENV_ACCESS_LOG_DIR = "accessLogDir";

MultiProcessUtil::MultiProcessUtil() {}

MultiProcessUtil::~MultiProcessUtil() {}

#define REPLACE_ENV_VALUE(envs, envName, replaceValue)                                                                 \
    {                                                                                                                  \
        bool isFound = false;                                                                                          \
        for (auto &env : envs) {                                                                                       \
            if (env.first == envName) {                                                                                \
                env.second = replaceValue;                                                                             \
                isFound = true;                                                                                        \
            }                                                                                                          \
        }                                                                                                              \
        if (!isFound) {                                                                                                \
            envs.emplace_back(envName, replaceValue);                                                                  \
        }                                                                                                              \
    }

void MultiProcessUtil::generateLogSenderProcess(std::vector<hippo::ProcessInfo> &processInfos,
                                                protocol::RoleType roleType) {
    std::string swiftTopicSuffix;
    std::string swiftLogDir;
    if (roleType == ROLE_TYPE_BROKER) {
        swiftTopicSuffix = SWIFT_BROKER_ROLE_NAME_SUFFIX;
        swiftLogDir = SWIFT_BROKER_DIR;
    } else if (roleType == ROLE_TYPE_ADMIN) {
        swiftTopicSuffix = SWIFT_ADMIN_ROLE_NAME_SUFFIX;
        swiftLogDir = SWIFT_ADMIN_DIR;
    } else {
        AUTIL_LOG(ERROR, "invalid role type");
        return;
    }
    for (auto &processInfo : processInfos) {
        if (processInfo.cmd != LOG_SENDER_CMD) {
            continue;
        }
        std::string separator = DEFAULT_SEPARATOR;
        std::string swiftTopicPrefix;
        std::string swiftLogSuffix;
        std::string swiftLogDirSuffix;
        auto &envs = processInfo.envs;
        for (auto &env : envs) {
            if (env.first == ENV_SEPARATOR) {
                separator = env.second;
            } else if (env.first == ENV_SWIFT_TOPIC_PREFIX) {
                swiftTopicPrefix = env.second;
            } else if (env.first == ENV_SWIFT_LOG_SUFFIX) {
                swiftLogSuffix = env.second;
            } else if (env.first == ENV_SWIFT_LOG_DIR_SUFFIX) {
                swiftLogDirSuffix = env.second;
            }
        }
        if (!swiftTopicPrefix.empty()) {
            std::string topicName;
            std::vector<std::string> splitVals = StringUtil::split(swiftTopicPrefix, separator, false);
            for (size_t i = 0; i < splitVals.size(); i++) {
                if (!splitVals[i].empty()) {
                    topicName += splitVals[i] + swiftTopicSuffix;
                }
                if (i != splitVals.size() - 1) {
                    topicName += separator;
                }
            }

            REPLACE_ENV_VALUE(envs, ENV_TOPIC_NAME, topicName);
        }
        if (!swiftLogSuffix.empty()) {
            std::string streamAccessLogPath;
            std::vector<std::string> splitVals = StringUtil::split(swiftLogSuffix, separator, false);
            for (size_t i = 0; i < splitVals.size(); i++) {
                if (!splitVals[i].empty()) {
                    streamAccessLogPath += swiftLogDir + splitVals[i];
                }
                if (i != splitVals.size() - 1) {
                    streamAccessLogPath += separator;
                }
            }
            REPLACE_ENV_VALUE(envs, ENV_STREAM_ACCESS_LOG_PATH, streamAccessLogPath);
        }
        if (!swiftLogDirSuffix.empty()) {
            std::string accessLogDir;
            std::vector<std::string> splitVals = StringUtil::split(swiftLogDirSuffix, separator, false);
            for (size_t i = 0; i < splitVals.size(); i++) {
                if (!splitVals[i].empty()) {
                    accessLogDir += swiftLogDir + splitVals[i];
                }
                if (i != splitVals.size() - 1) {
                    accessLogDir += separator;
                }
            }
            REPLACE_ENV_VALUE(envs, ENV_ACCESS_LOG_DIR, accessLogDir);
        }
    }
}
} // namespace util
} // namespace swift
