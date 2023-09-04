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

namespace suez {
namespace turing {

class VersionConfig : public autil::legacy::Jsonizable {
public:
    VersionConfig() {}
    ~VersionConfig() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("protocol_version", _protocolVersion, _protocolVersion);
        json.Jsonize("data_version", _dataVersion, _dataVersion);
    }
    const std::string &getProtocolVersion() const { return _protocolVersion; }
    const std::string &getDataVersion() const { return _dataVersion; }

private:
    std::string _protocolVersion;
    std::string _dataVersion;
};

} // namespace turing
} // namespace suez
