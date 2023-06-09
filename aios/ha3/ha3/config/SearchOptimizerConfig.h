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
#include "ha3/config/OptimizerConfigInfo.h"

namespace isearch {
namespace config {

class SearchOptimizerConfig : public autil::legacy::Jsonizable
{
public:
    SearchOptimizerConfig();
    ~SearchOptimizerConfig();
private:
    SearchOptimizerConfig(const SearchOptimizerConfig &);
    SearchOptimizerConfig& operator=(const SearchOptimizerConfig &);
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    const build_service::plugin::ModuleInfos& getModuleInfos() const {
        return _modules;
    }
    const OptimizerConfigInfos& getOptimizerConfigInfos() const {
        return _optimizerConfigInfos;
    }
private:
    build_service::plugin::ModuleInfos _modules;
    OptimizerConfigInfos _optimizerConfigInfos;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearchOptimizerConfig> SearchOptimizerConfigPtr;

} // namespace config
} // namespace isearch

