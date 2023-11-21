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

#include "autil/Log.h" // IWYU pragma: keep
#include "matchdoc/Trait.h"
#include "suez/turing/expression/provider/TermMatchData.h"

namespace suez {
namespace turing {

class MatchData {
public:
    MatchData();
    ~MatchData();

private:
    MatchData(const MatchData &) = delete;
    MatchData &operator=(const MatchData &) = delete;

public:
    static uint32_t sizeOf(uint32_t numTerms) {
        return (uint32_t)(sizeof(MatchData) + (numTerms - 1) * sizeof(TermMatchData));
    }

public:
    std::string toString() const;
    void reset() {
        TermMatchData *curMatchData = _termMatchData + 1;
        TermMatchData *end = _termMatchData + _numTerms;
        while (curMatchData < end) {
            curMatchData->~TermMatchData();
            ++curMatchData;
        }
        _numTerms = 0;
    }
    TermMatchData &nextFreeMatchData() {
        assert(_numTerms < _maxNumTerms);
        if (_numTerms > 0) {
            new (&_termMatchData[_numTerms]) TermMatchData;
        }
        return _termMatchData[_numTerms++];
    }

    const TermMatchData &getTermMatchData(uint32_t idx) const {
        assert(idx < _numTerms);
        return _termMatchData[idx];
    }

    TermMatchData &getTermMatchData(uint32_t idx) {
        assert(idx < _numTerms);
        return _termMatchData[idx];
    }

    void setMaxNumTerms(uint32_t maxNumTerms) { _maxNumTerms = maxNumTerms; }
    uint32_t getNumTerms() const { return _numTerms; }

private:
    uint32_t _numTerms;
    uint32_t _maxNumTerms;
    TermMatchData _termMatchData[1];

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
