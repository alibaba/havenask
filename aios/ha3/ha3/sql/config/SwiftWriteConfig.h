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

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"

namespace isearch {
namespace sql {

class SwiftReaderWriterConfig : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("swift_topic_name", swiftTopicName, swiftTopicName);
        json.Jsonize("swift_reader_config", swiftReaderConfig, swiftReaderConfig);
        json.Jsonize("swift_writer_config", swiftWriterConfig, swiftWriterConfig);
    }
public:
    std::string swiftTopicName;
    std::string swiftReaderConfig;
    std::string swiftWriterConfig;
};

class SwiftWriteConfig : public autil::legacy::Jsonizable
{
public:
    SwiftWriteConfig();
    ~SwiftWriteConfig() = default;
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("swift_client_config", _swiftClientConfig, _swiftClientConfig);
        json.Jsonize("table_read_write_config", _tableReadWriteConfigMap);
    }

    const std::string& getSwiftClientConfig() const {
        return _swiftClientConfig;
    }

    const std::map<std::string, SwiftReaderWriterConfig>& getTableReadWriteConfigMap() const{
        return _tableReadWriteConfigMap;
    }

    bool getTableReadWriteConfig(const std::string &tableName, SwiftReaderWriterConfig &config);
    bool empty() const;
private:
    std::string _swiftClientConfig;
    std::map<std::string, SwiftReaderWriterConfig> _tableReadWriteConfigMap;
};

typedef std::shared_ptr<SwiftWriteConfig> SwiftWriteConfigPtr;

} //end namespace sql
} //end namespace isearch
