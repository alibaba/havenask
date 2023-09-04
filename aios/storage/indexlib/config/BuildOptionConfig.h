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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlibv2::config {
class SortDescription;
typedef std::vector<SortDescription> SortDescriptions;

class BuildOptionConfig : public autil::legacy::Jsonizable
{
public:
    BuildOptionConfig();
    ~BuildOptionConfig();
    BuildOptionConfig(const BuildOptionConfig& other);
    BuildOptionConfig& operator=(const BuildOptionConfig& other);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool IsSortBuild() const;
    // virtual for mock
    virtual const SortDescriptions& GetSortDescriptions() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
