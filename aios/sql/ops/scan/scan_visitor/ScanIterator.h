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

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "autil/result/Result.h"
#include "ha3/common/TimeoutTerminator.h"
#include "matchdoc/MatchDocAllocator.h"

namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

namespace sql {

class ScanIterator {
public:
    ScanIterator(const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                 isearch::common::TimeoutTerminator *timeoutTerminator)
        : _matchDocAllocator(matchDocAllocator)
        , _timeoutTerminator(timeoutTerminator)
        , _totalScanCount(0)
        , _isTimeout(false) {}

    virtual ~ScanIterator() {}

public:
    virtual autil::Result<bool> batchSeek(size_t batchSize,
                                          std::vector<matchdoc::MatchDoc> &matchDocs)
        = 0;

    matchdoc::MatchDocAllocatorPtr getMatchDocAllocator() const {
        return _matchDocAllocator;
    }
    virtual bool useTruncate() {
        return false;
    }
    virtual uint32_t getTotalScanCount() const {
        return _totalScanCount;
    }
    virtual uint32_t getTotalSeekedCount() const = 0;
    virtual uint32_t getTotalWholeDocCount() const = 0;
    bool isTimeout() const {
        if (_isTimeout) {
            return true;
        } else if (_timeoutTerminator) {
            return _timeoutTerminator->isTimeout();
        } else {
            return false;
        }
    }

protected:
    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    isearch::common::TimeoutTerminator *_timeoutTerminator;
    uint32_t _totalScanCount;
    bool _isTimeout;
};

typedef std::shared_ptr<ScanIterator> ScanIteratorPtr;
} // namespace sql
