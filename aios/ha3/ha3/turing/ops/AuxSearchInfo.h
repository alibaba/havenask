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
#include <memory>


namespace isearch {
namespace turing {

class AuxSearchInfo
{
public:
    AuxSearchInfo() : _elapsedTime(0), _resultCount(0) {}
    ~AuxSearchInfo() {}
private:
    AuxSearchInfo(const AuxSearchInfo &);
    AuxSearchInfo& operator=(const AuxSearchInfo &);
public:
    std::string toString();
public:
    int64_t _elapsedTime;
    uint32_t _resultCount;
};

typedef std::shared_ptr<AuxSearchInfo> AuxSearchInfoPtr;

} // namespace turing
} // namespace isearch
