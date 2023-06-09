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
#include "build_service/proto/SwiftRootUpgrader.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "build_service/config/CLIOptionNames.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::util;

namespace build_service { namespace proto {
BS_LOG_SETUP(proto, SwiftRootUpgrader);

SwiftRootUpgrader::SwiftRootUpgrader()
{
    string swiftOrgStr = EnvUtil::getEnv("upgrade_swift_root_from", string(""));
    string swiftNewStr = EnvUtil::getEnv("upgrade_swift_root_to", string(""));

    StringUtil::fromString(swiftOrgStr, _swiftOrgVec, "#");
    StringUtil::fromString(swiftNewStr, _swiftNewVec, "#");

    if (_swiftOrgVec.size() != _swiftNewVec.size()) {
        BS_LOG(ERROR, "upgrade swift root from [%s] to [%s], size of upgrade pair not equal, ignore upgrade",
               swiftOrgStr.c_str(), swiftNewStr.c_str());
        _swiftOrgVec.clear();
        _swiftNewVec.clear();
    }

    bool ambiguous = false;
    if (_swiftOrgVec.size() > 1) {
        for (size_t i = 0; i < _swiftOrgVec.size() - 1; i++) {
            for (size_t j = i + 1; j < _swiftOrgVec.size(); j++) {
                if (_swiftOrgVec[j].find(_swiftOrgVec[i]) != string::npos) {
                    BS_LOG(ERROR, "swift root [%s] is part of [%s], upgrade will be ambiguous, ignore upgrade",
                           _swiftOrgVec[i].c_str(), _swiftOrgVec[j].c_str());
                    ambiguous = true;
                }
            }
        }
    }

    if (ambiguous) {
        _swiftOrgVec.clear();
        _swiftNewVec.clear();
    }
    string tmp = EnvUtil::getEnv("upgrade_swift_root_target_generations", string(""));
    vector<uint32_t> gids;
    StringUtil::fromString(tmp, gids, ",");
    _targetGenerations.insert(gids.begin(), gids.end());
}

SwiftRootUpgrader::~SwiftRootUpgrader() {}

bool SwiftRootUpgrader::needUpgrade(uint32_t generationId) const
{
    if (_swiftOrgVec.empty()) {
        return false;
    }
    if (_targetGenerations.empty()) {
        return true;
    }
    return _targetGenerations.find(generationId) != _targetGenerations.end();
}

void SwiftRootUpgrader::upgrade(proto::DataDescription& ds)
{
    if (_swiftOrgVec.empty()) {
        return;
    }

    auto iter = ds.find(SWIFT_ZOOKEEPER_ROOT);
    if (iter == ds.end()) {
        return;
    }

    for (size_t i = 0; i < _swiftOrgVec.size(); i++) {
        StringUtil::replaceAll(iter->second, _swiftOrgVec[i], _swiftNewVec[i]);
    }
}

void SwiftRootUpgrader::upgrade(proto::DataDescriptions& dsVec)
{
    std::vector<DataDescription>& ds = dsVec.toVector();
    if (ds.empty()) {
        return;
    }
    DataDescription& lastDs = ds[ds.size() - 1];
    upgrade(lastDs);
}

string SwiftRootUpgrader::upgradeDataDescriptions(const string& dataSource)
{
    DataDescriptions dataDescriptions;
    ;
    try {
        FromJsonString(dataDescriptions.toVector(), dataSource);
    } catch (const autil::legacy::ExceptionBase& e) {
        stringstream ss;
        ss << "parse data descriptions [" << dataSource << "] failed, exception[" << string(e.what()) << "]";
        BS_LOG(ERROR, "%s", ss.str().c_str());
        return dataSource;
    }
    upgrade(dataDescriptions);
    auto dsVec = dataDescriptions.toVector();
    return ToJsonString(dsVec, true);
}

}} // namespace build_service::proto
