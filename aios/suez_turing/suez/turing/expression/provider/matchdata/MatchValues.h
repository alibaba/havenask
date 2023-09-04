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
#include <new>
#include <stdint.h>

#include "autil/Log.h"
#include "indexlib/indexlib.h"
#include "matchdoc/Trait.h" // IWYU pragma: keep

namespace suez {
namespace turing {

class MatchValues {
public:
    MatchValues();
    ~MatchValues();

private:
    MatchValues(const MatchValues &) = delete;
    MatchValues &operator=(const MatchValues &) = delete;

public:
    static uint32_t sizeOf(uint32_t numTerms) {
        return (uint32_t)(sizeof(MatchValues) + (numTerms - 1) * sizeof(matchvalue_t));
    }

public:
    matchvalue_t &nextFreeMatchValue() {
        assert(_numTerms < _maxNumTerms);
        if (_numTerms > 0) {
            new (&_termMatchValue[_numTerms]) matchvalue_t;
        }
        return _termMatchValue[_numTerms++];
    }

    const matchvalue_t &getTermMatchValue(uint32_t idx) const {
        assert(idx < _numTerms);
        return _termMatchValue[idx];
    }

    matchvalue_t &getTermMatchValue(uint32_t idx) {
        assert(idx < _numTerms);
        return _termMatchValue[idx];
    }

    void setMaxNumTerms(uint32_t maxNumTerms) { _maxNumTerms = maxNumTerms; }
    uint32_t getNumTerms() const { return _numTerms; }

private:
    uint32_t _numTerms;
    uint32_t _maxNumTerms;
    matchvalue_t _termMatchValue[1];

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
