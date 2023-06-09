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

#include <stdint.h>
#include <string>

namespace suez {

class AccessCounterLog {
public:
    AccessCounterLog(){};
    ~AccessCounterLog(){};

private:
    AccessCounterLog(const AccessCounterLog &);
    AccessCounterLog &operator=(const AccessCounterLog &);

public:
    void reportIndexCounter(const std::string &indexName, uint32_t count);
    void reportAttributeCounter(const std::string &attributeName, uint32_t count);

private:
    void log(const std::string &prefix, const std::string &name, uint32_t count);
};

} // namespace suez
