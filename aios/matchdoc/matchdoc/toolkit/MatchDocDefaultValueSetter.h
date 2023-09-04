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

#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/toolkit/FieldDefaultValueSetter.h"

namespace matchdoc {

class MatchDocDefaultValueSetter {
public:
    MatchDocDefaultValueSetter(matchdoc::MatchDocAllocatorPtr &allocator, autil::mem_pool::Pool *pool)
        : _allocator(allocator), _pool(pool) {}
    ~MatchDocDefaultValueSetter() {}

public:
    bool init(const std::map<std::string, std::string> &defaultValues);
    void setDefaultValues(matchdoc::MatchDoc doc);

private:
    bool createFieldSetters(const std::map<std::string, std::string> &defaultValues);

    bool initMountBuffer(std::map<uint32_t, char *> &mountIdToMountBuffer);

    bool initDataBuffer(std::map<const matchdoc::ReferenceBase *, char *> &dataBuffer);

private:
    matchdoc::MatchDocAllocatorPtr _allocator = nullptr;
    autil::mem_pool::Pool *_pool = nullptr;
    std::vector<FieldDefaultValueSetter> _fieldSetters;

    AUTIL_LOG_DECLARE();
};

} // namespace matchdoc
