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

namespace matchdoc {

class FieldDefaultValueSetter {
public:
    FieldDefaultValueSetter(autil::mem_pool::Pool *pool, ReferenceBase *refBase)
        : _pool(pool), _refBase(refBase), _dataBuffer(nullptr) {}

    ~FieldDefaultValueSetter() {}

    bool init(const std::string &defaultValue, char *mountBuffer = nullptr);

    void setDefaultValue(matchdoc::MatchDoc doc) const;

    inline const std::string &getFieldName() const { return _refBase->getName(); }

private:
    static char *
    constructTypedBuffer(autil::mem_pool::Pool *pool, ReferenceBase *refBase, const std::string &defaultValue);

    void setNonMountValue(matchdoc::MatchDoc doc) const;

private:
    autil::mem_pool::Pool *_pool;
    ReferenceBase *_refBase;
    char *_dataBuffer;

    AUTIL_LOG_DECLARE();
};

} // namespace matchdoc