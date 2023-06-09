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

#include <algorithm>
#include <memory>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "build_service/plugin/ModuleInfo.h"
#include "ha3/sql/ops/tvf/SqlTvfProfileInfo.h"

namespace isearch {
namespace sql {

class SqlTvfPluginConfig : public autil::legacy::Jsonizable {
public:
    SqlTvfPluginConfig() {}
    ~SqlTvfPluginConfig() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("modules", modules, modules);
        json.Jsonize("tvf_profiles", tvfProfileInfos, tvfProfileInfos);
    }

public:
    build_service::plugin::ModuleInfos modules;
    SqlTvfProfileInfos tvfProfileInfos;
};

typedef std::shared_ptr<SqlTvfPluginConfig> SqlTvfPluginConfigPtr;
} // namespace sql
} // namespace isearch
