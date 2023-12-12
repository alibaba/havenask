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

#include "autil/legacy/jsonizable.h"

namespace suez {

class WALConfig final : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

public:
    bool hasSink() const { return !sinkDescription.empty(); }

public:
    std::string strategy;
    std::map<std::string, std::string> sinkDescription;
    int64_t timeoutMs = 2 * 1000; // 2s
    std::string desc;
    std::pair<uint32_t, uint32_t> range; // no need jsonize
};

} // namespace suez
