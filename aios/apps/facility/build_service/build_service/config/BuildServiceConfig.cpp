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
#include "build_service/config/BuildServiceConfig.h"

#include "fslib/util/FileUtil.h"

using namespace std;
namespace build_service { namespace config {
BS_LOG_SETUP(config, BuildServiceConfig);

BuildServiceConfig::BuildServiceConfig() : amonitorPort(DEFAULT_AMONITOR_PORT) {}

BuildServiceConfig::~BuildServiceConfig() {}

BuildServiceConfig::BuildServiceConfig(const BuildServiceConfig& other)
    : amonitorPort(other.amonitorPort)
    , userName(other.userName)
    , serviceName(other.serviceName)
    , hippoRoot(other.hippoRoot)
    , zkRoot(other.zkRoot)
    , suezVersionZkRoots(other.suezVersionZkRoots)
    , heartbeatType(other.heartbeatType)
    , counterConfig(other.counterConfig)
    , _swiftRoot(other._swiftRoot)
    , _fullSwiftRoot(other._fullSwiftRoot)
    , _fullBuilderTmpRoot(other._fullBuilderTmpRoot)
    , _indexRoot(other._indexRoot)
{
}

void BuildServiceConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("user_name", userName, userName);
    json.Jsonize("service_name", serviceName, serviceName);
    json.Jsonize("index_root", _indexRoot, _indexRoot);
    json.Jsonize("swift_root", _swiftRoot, _swiftRoot);
    json.Jsonize("full_swift_root", _fullSwiftRoot, _fullSwiftRoot);
    json.Jsonize("hippo_root", hippoRoot, hippoRoot);
    json.Jsonize("zookeeper_root", zkRoot, zkRoot);
    json.Jsonize("suez_version_zookeeper_roots", suezVersionZkRoots, suezVersionZkRoots);
    json.Jsonize("amonitor_port", amonitorPort, amonitorPort);
    json.Jsonize("heartbeat_type", heartbeatType, heartbeatType);
    json.Jsonize("counter_config", counterConfig, counterConfig);
    json.Jsonize("full_builder_tmp_root", _fullBuilderTmpRoot, _fullBuilderTmpRoot);
}

bool BuildServiceConfig::operator==(const BuildServiceConfig& other) const
{
    return (userName == other.userName && serviceName == other.serviceName && _indexRoot == other._indexRoot &&
            _swiftRoot == other._swiftRoot && _fullSwiftRoot == other._fullSwiftRoot && hippoRoot == other.hippoRoot &&
            zkRoot == other.zkRoot && suezVersionZkRoots == other.suezVersionZkRoots &&
            amonitorPort == other.amonitorPort && heartbeatType == other.heartbeatType &&
            counterConfig == other.counterConfig && _fullBuilderTmpRoot == other._fullBuilderTmpRoot);
}

string BuildServiceConfig::getIndexRoot() const { return _indexRoot; }

std::string BuildServiceConfig::getSwiftRoot(bool isFull) const
{
    if (_fullSwiftRoot.empty()) {
        return _swiftRoot;
    }
    return isFull ? _fullSwiftRoot : _swiftRoot;
}

string BuildServiceConfig::getBuilderIndexRoot(bool isFullBuilder) const
{
    if (_fullBuilderTmpRoot.empty()) {
        return _indexRoot;
    }
    return isFullBuilder ? _fullBuilderTmpRoot : _indexRoot;
}

string BuildServiceConfig::getApplicationId() const { return userName + "_" + serviceName; }

string BuildServiceConfig::getMessageIndexName() const { return userName + "_" + serviceName; }

bool BuildServiceConfig::validate() const
{
    if (userName.empty()) {
        string errorMsg = "UserName can't be empty.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (serviceName.empty()) {
        string errorMsg = "ServiceName can't be empty.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (_indexRoot.empty()) {
        string errorMsg = "IndexRoot can't be empty.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (_swiftRoot.empty()) {
        string errorMsg = "SwiftRoot can't be empty.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (hippoRoot.empty()) {
        string errorMsg = "HippoRoot can't be empty.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (zkRoot.empty()) {
        string errorMsg = "Zookeeper can't be empty.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!counterConfig.validate()) {
        BS_LOG(ERROR, "%s", "invalid counter_config");
        return false;
    }
    if (!suezVersionZkRoots.empty()) {
        std::string zkHost;
        for (const auto& [_, zkRoot] : suezVersionZkRoots) {
            std::string host = fslib::util::FileUtil::getHostFromZkPath(zkRoot);
            if (host == "") {
                BS_LOG(ERROR, "invalid zkRoot [%s]", zkRoot.c_str());
                return false;
            }
            if (zkHost == "") {
                zkHost = host;
                continue;
            }
            if (zkRoot != host) {
                BS_LOG(ERROR, "different zk root is not support [%s] [%s]", zkRoot.c_str(), host.c_str());
                return false;
            }
        }
    }
    return true;
}

}} // namespace build_service::config
