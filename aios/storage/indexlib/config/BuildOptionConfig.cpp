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
#include "indexlib/config/BuildOptionConfig.h"

#include "indexlib/config/SortDescription.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, BuildOptionConfig);

struct BuildOptionConfig::Impl {
    bool sortBuild = false;
    SortDescriptions sortDescriptions;
};

BuildOptionConfig::BuildOptionConfig() : _impl(std::make_unique<BuildOptionConfig::Impl>()) {}
BuildOptionConfig::~BuildOptionConfig() {}
BuildOptionConfig::BuildOptionConfig(const BuildOptionConfig& other)
    : _impl(std::make_unique<BuildOptionConfig::Impl>(*other._impl))
{
}

BuildOptionConfig& BuildOptionConfig::operator=(const BuildOptionConfig& other)
{
    if (this != &other) {
        _impl = std::make_unique<BuildOptionConfig::Impl>(*other._impl);
    }
    return *this;
}

void BuildOptionConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("sort_build", _impl->sortBuild, _impl->sortBuild);
    json.Jsonize("sort_descriptions", _impl->sortDescriptions, _impl->sortDescriptions);
}

bool BuildOptionConfig::IsSortBuild() const { return _impl->sortBuild; }

const SortDescriptions& BuildOptionConfig::GetSortDescriptions() const { return _impl->sortDescriptions; }

} // namespace indexlibv2::config
