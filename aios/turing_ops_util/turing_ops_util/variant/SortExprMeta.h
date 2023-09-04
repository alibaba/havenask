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

#include <stddef.h>
#include <string>

#include "autil/DataBuffer.h"

namespace matchdoc {
class ReferenceBase;
}  // namespace matchdoc

namespace suez {
namespace turing {

class SortExprMeta {
public:
    SortExprMeta() : sortRef(NULL), sortFlag(false) {}
    SortExprMeta(const std::string &name_, const  matchdoc::ReferenceBase *ref_, bool isAsc_ = true)
        : sortExprName(name_), sortRef(ref_), sortFlag(isAsc_) {}
    ~SortExprMeta() {}

public:
    std::string sortExprName;
    const matchdoc::ReferenceBase *sortRef;
    bool sortFlag;
    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(sortExprName);
        dataBuffer.write(sortFlag);
    }
    void deserialize(autil::DataBuffer &dataBuffer) {
        dataBuffer.read(sortExprName);
        dataBuffer.read(sortFlag);
    }
    bool operator==(const SortExprMeta &other) const {
        if (this == &other) {
            return true;
        }
        return sortExprName == other.sortExprName && sortRef == other.sortRef &&
               sortFlag == other.sortFlag;
    }
    bool operator!=(const SortExprMeta &other) const {
        return !(*this == other);
    }
};
}
}

