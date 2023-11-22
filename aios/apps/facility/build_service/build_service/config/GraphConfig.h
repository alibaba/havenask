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

#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class GraphConfig : public autil::legacy::Jsonizable
{
public:
    GraphConfig();
    ~GraphConfig();

private:
    GraphConfig(const GraphConfig&);
    GraphConfig& operator=(const GraphConfig&);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    const std::string& getGraphName() const { return _graphName; }
    const KeyValueMap& getGraphParam() const { return _parameters; }
    const std::string& getGraphPath() const { return _path; }

private:
    std::string _graphName;
    // std::string _luaVersion;
    // std::string _graphId;
    std::string _path;
    KeyValueMap _parameters;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GraphConfig);

}} // namespace build_service::config
