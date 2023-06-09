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
#include <cassert>

#include "autil/Log.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/index/operation_log/OperationCursor.h"
#include "indexlib/index/operation_log/OperationMeta.h"

namespace indexlib::index {
class OperationLogIndexer;
class OperationLogConfig;
class OperationBase;
class SegmentOperationIterator;

class OperationIterator
{
public:
    OperationIterator(const std::vector<std::shared_ptr<OperationLogIndexer>>& indexers,
                      const std::shared_ptr<OperationLogConfig>& indexConfig);
    ~OperationIterator();
    // skipCursor will not be iteratered
    Status Init(const OperationCursor& skipCursor, const indexlibv2::framework::Locator& locator);
    std::pair<Status, bool> HasNext();
    std::pair<Status, OperationBase*> Next();
    OperationCursor GetLastCursor() const;
    segmentid_t GetCurrentSegmentId() const;

private:
    std::pair<Status, std::shared_ptr<SegmentOperationIterator>> LoadSegIterator(int32_t segmentIdx, int32_t offset);
    void UpdateLastCursor(const std::shared_ptr<SegmentOperationIterator>& iter);

private:
    indexlibv2::framework::Locator _locator;
    std::shared_ptr<SegmentOperationIterator> _currentSegIter;
    int32_t _currentSegIdx;
    std::vector<std::shared_ptr<OperationLogIndexer>> _indexers;
    std::shared_ptr<OperationLogConfig> _indexConfig;
    std::vector<OperationMeta> _segMetaVec;
    OperationCursor _lastOpCursor;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
