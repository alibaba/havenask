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

#include "autil/legacy/jsonizable.h"

namespace iquan {

const static std::string SUMMARY_TABLE_SUFFIX = "_summary_";

class TableConfig : public autil::legacy::Jsonizable {
public:
    TableConfig()
        : summaryTableSuffix(SUMMARY_TABLE_SUFFIX) {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("summary_suffix", summaryTableSuffix, summaryTableSuffix);
    }

    bool isValid() const {
        if (summaryTableSuffix.empty()) {
            return false;
        }
        return true;
    }

public:
    std::string summaryTableSuffix;
};

class JniConfig : public autil::legacy::Jsonizable {
public:
    JniConfig() {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("table_config", tableConfig, tableConfig);
    }

    bool isValid() const {
        return tableConfig.isValid();
    }

public:
    TableConfig tableConfig;
};

} // namespace iquan
