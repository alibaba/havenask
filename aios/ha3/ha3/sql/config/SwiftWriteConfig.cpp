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
#include "ha3/sql/config/SwiftWriteConfig.h"

using namespace std;

namespace isearch {
namespace sql {

SwiftWriteConfig::SwiftWriteConfig() {
}

bool  SwiftWriteConfig::getTableReadWriteConfig(const std::string &tableName, SwiftReaderWriterConfig &config) {
    auto iter = _tableReadWriteConfigMap.find(tableName);
    if (iter != _tableReadWriteConfigMap.end()) {
        config = iter->second;
        return true;
    }
    return false;
}

bool SwiftWriteConfig::empty() const {
    if (_swiftClientConfig.empty() || _tableReadWriteConfigMap.empty()) {
        return true;
    }
    return false;
}


} //end namespace sql
} //end namespace isearch
