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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/rank/Comparator.h"
#include "matchdoc/MatchDoc.h"

namespace matchdoc {
template <typename T>
class Reference;
} // namespace matchdoc

namespace isearch {
namespace rank {

template <typename T>
class ReferenceComparator : public Comparator {
public:
    ReferenceComparator(const matchdoc::Reference<T> *variableReference, bool flag = false) {
        _reference = variableReference;
        _flag = flag;
    }
    ~ReferenceComparator() {}

public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const override {
        if (_flag) {
            return *_reference->getPointer(b) < *_reference->getPointer(a);
        } else {
            return *_reference->getPointer(a) < *_reference->getPointer(b);
        }
    }
    void setReverseFlag(bool flag) {
        _flag = flag;
    }
    bool getReverseFlag() const {
        return _flag;
    }
    std::string getType() const override {
        return "reference";
    }

protected:
    const matchdoc::Reference<T> *_reference;
    bool _flag;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(ha3, ReferenceComparator, T);

} // namespace rank
} // namespace isearch
