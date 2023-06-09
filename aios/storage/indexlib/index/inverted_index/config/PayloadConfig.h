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

#include "autil/Log.h"

namespace indexlib::config {

class PayloadConfig
{
public:
    PayloadConfig();
    ~PayloadConfig();
    PayloadConfig(const PayloadConfig& other);

public:
    const std::string& GetName() const { return _name; }
    uint64_t RewriteTermHash(uint64_t termHash) const;
    bool UseFP16() const { return _fp16; }
    bool IsInitialized() const { return _name != DEFAULT_PAYLOAD_NAME; }
    void SetName(const std::string& name) { _name = name; }
    void SetFP16(bool flag) { _fp16 = flag; }

private:
    static constexpr std::string_view DEFAULT_PAYLOAD_NAME = "DEFAULT_PAYLOAD_NAME";
    std::string _name;
    bool _fp16;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::config
