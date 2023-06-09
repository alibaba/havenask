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
#include "build_service/config/GraphConfig.h"

#include "fslib/util/FileUtil.h"

using namespace std;
using namespace build_service::util;

namespace build_service { namespace config {
BS_LOG_SETUP(config, GraphConfig);

GraphConfig::GraphConfig() {}

GraphConfig::~GraphConfig() {}

void GraphConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("graph_name", _graphName, _graphName);
    // json.Jsonize("id", _graphId, _graphId);
    json.Jsonize("path", _path, _path);
    json.Jsonize("params", _parameters, _parameters);
}

}} // namespace build_service::config
