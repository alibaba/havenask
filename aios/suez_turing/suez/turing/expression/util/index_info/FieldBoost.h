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
#include <stdint.h>

#include "autil/Log.h"

namespace suez {
namespace turing {

typedef int32_t fieldboost_t;

class FieldBoost {
public:
    FieldBoost();
    ~FieldBoost();

    inline fieldboost_t &operator[](int idx) {
        assert(0 <= idx);
        assert(idx < FIELDS_LIMIT);
        return _boosts[idx];
    }

    inline const fieldboost_t &operator[](int idx) const {
        assert(0 <= idx);
        assert(idx < FIELDS_LIMIT);
        return _boosts[idx];
    }

public:
    static const int FIELDS_LIMIT = 32;

private:
    fieldboost_t _boosts[FIELDS_LIMIT];

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
