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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "ha3/isearch.h"

namespace sql {

class SqlTvfProfileInfo : public autil::legacy::Jsonizable {
public:
    SqlTvfProfileInfo() {}
    SqlTvfProfileInfo(const std::string &tvfName_,
                      const std::string &funcName_,
                      const KeyValueMap &parameters_ = {})
        : tvfName(tvfName_)
        , funcName(funcName_)
        , parameters(parameters_) {}
    ~SqlTvfProfileInfo() {}

public:
    bool empty() const {
        return tvfName.empty() || funcName.empty();
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("tvf_name", tvfName, tvfName);
        json.Jsonize("func_name", funcName, funcName);
        json.Jsonize("parameters", parameters, parameters);
    }

public:
    std::string tvfName;
    std::string funcName;
    KeyValueMap parameters;
};
typedef std::vector<SqlTvfProfileInfo> SqlTvfProfileInfos;

typedef std::shared_ptr<SqlTvfProfileInfo> SqlTvfProfileInfoPtr;
} // namespace sql
