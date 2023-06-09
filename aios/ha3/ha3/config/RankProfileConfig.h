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

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"
#include "build_service/plugin/ModuleInfo.h"
#include "ha3/config/RankProfileInfo.h"

namespace isearch {
namespace config {

class RankProfileConfig : public autil::legacy::Jsonizable
{
public:
    RankProfileConfig();
    ~RankProfileConfig();
private:
    RankProfileConfig(const RankProfileConfig &);
    RankProfileConfig& operator=(const RankProfileConfig &);
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const RankProfileConfig &other) const;
public:
    const build_service::plugin::ModuleInfos& getModuleInfos() const {
        return _modules;
    }
    const RankProfileInfos& getRankProfileInfos() const {
        return _rankProfileInfos;
    }
private:
    build_service::plugin::ModuleInfos _modules;
    RankProfileInfos _rankProfileInfos;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RankProfileConfig> RankProfileConfigPtr;

} // namespace config
} // namespace isearch

