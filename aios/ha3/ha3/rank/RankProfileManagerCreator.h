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


#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/config/RankProfileInfo.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/rank/RankProfileManager.h"

namespace suez {
namespace turing {
class CavaPluginManager;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace config {
class RankProfileConfig;
}  // namespace config
}  // namespace isearch
namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

namespace isearch {
namespace rank {

class RankProfileManagerCreator
{
public:
    RankProfileManagerCreator(const config::ResourceReaderPtr &resourceReaderPtr,
                              const suez::turing::CavaPluginManager *cavaPluginManager,
                              kmonitor::MetricsReporter *metricsReporter);
    ~RankProfileManagerCreator();
public:
    RankProfileManagerPtr create(const config::RankProfileConfig &config);
public:
    // for test
    RankProfileManagerPtr createFromFile(const std::string &filePath);
    RankProfileManagerPtr createFromString(const std::string &configStr);
private:
    void addRecordProfileInfo(config::RankProfileInfos& rankProfileInfos);
private:
    config::ResourceReaderPtr _resourceReaderPtr;
    const suez::turing::CavaPluginManager *_cavaPluginManager;
    kmonitor::MetricsReporter *_metricsReporter;
    AUTIL_LOG_DECLARE();
};

} // namespace rank
} // namespace isearch
