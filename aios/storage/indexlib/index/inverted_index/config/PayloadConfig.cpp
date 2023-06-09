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
#include "indexlib/index/inverted_index/config/PayloadConfig.h"

#include "autil/HashAlgorithm.h"

namespace indexlib::config {
AUTIL_LOG_SETUP(config, PayloadConfig);

PayloadConfig::PayloadConfig() : _name(DEFAULT_PAYLOAD_NAME), _fp16(false) {}
PayloadConfig::~PayloadConfig() {}

PayloadConfig::PayloadConfig(const PayloadConfig& other) : _name(other._name), _fp16(other._fp16) {}

uint64_t PayloadConfig::RewriteTermHash(uint64_t termHash) const
{
    if (_name == DEFAULT_PAYLOAD_NAME) {
        return termHash;
    }
    return autil::HashAlgorithm::hashString64((_name + "#" + std::to_string(termHash)).c_str());
}

} // namespace indexlib::config
