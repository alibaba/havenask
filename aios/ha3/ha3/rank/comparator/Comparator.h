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
#include "matchdoc/MatchDoc.h"

namespace isearch {
namespace rank {

class Comparator {
public:
    Comparator();
    virtual ~Comparator();

public:
    virtual bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const = 0;
    virtual std::string getType() const {
        return "base";
    }

private:
    AUTIL_LOG_DECLARE();
};

class MatchDocComp {
public:
    MatchDocComp(const Comparator *comp) {
        _comp = comp;
    }

public:
    bool operator()(matchdoc::MatchDoc lft, matchdoc::MatchDoc rht) {
        return _comp->compare(rht, lft);
    }

private:
    const Comparator *_comp;
};

} // namespace rank
} // namespace isearch
