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

#include <assert.h>

#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

enum CompareFlagEnum { CF_BETTER = 1, CF_WORSE = 0, CF_UNKNOWN = -1 };

class CompareFlag
{
public:
    CompareFlag(CompareFlagEnum flag) : _flag(flag) {
    }
    CompareFlag(bool flag) : _flag(CompareFlagEnum(flag)) {
    }
    bool isValid() const {
        return CF_UNKNOWN != _flag;
    }
    operator bool() const {
        assert(_flag != CF_UNKNOWN);
        return _flag == CF_BETTER;
    }
    bool operator==(const CompareFlag &rhs) const {
        return _flag == rhs._flag;
    }
    bool operator!=(const CompareFlag &rhs) const {
        return _flag != rhs._flag;
    }

private:
    CompareFlagEnum _flag;
};

extern const CompareFlag BetterCompareFlag;
extern const CompareFlag WorseCompareFlag;
extern const CompareFlag UnknownCompareFlag;

} // namespace multi_call
