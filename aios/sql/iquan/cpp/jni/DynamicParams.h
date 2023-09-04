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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/json.h"
#include "iquan/common/Common.h"

namespace iquan {

class DynamicParams {
public:
    DynamicParams();
    ~DynamicParams();

public:
    const autil::legacy::Any &at(std::size_t idx, std::size_t sql_idx = 0) const;
    const autil::legacy::Any &at(std::string key, std::size_t sql_idx = 0) const;
    bool
    addKVParams(const std::string &key, const autil::legacy::Any &val, std::size_t sql_idx = 0);
    bool findParamWithKey(const std::string &key, std::size_t sql_idx = 0) const;
    bool empty(std::size_t sql_idx = 0) const;
    bool isHintParamsEmpty() const;
    bool findHintParam(const std::string &key) const;
    std::string getHintParam(const std::string &key) const;
    void reserveOneSqlParams();

public:
    std::vector<std::vector<autil::legacy::Any>> _array;
    std::vector<std::map<std::string, autil::legacy::Any>> _map;
    std::map<std::string, std::string> _hint;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace iquan
