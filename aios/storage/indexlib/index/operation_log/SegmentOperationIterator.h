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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/index/operation_log/OperationCursor.h"
#include "indexlib/index/operation_log/OperationFactory.h"
#include "indexlib/index/operation_log/OperationMeta.h"

namespace indexlib::index {
class OperationLogConfig;
class SegmentOperationIterator : public autil::NoCopyable
{
public:
    SegmentOperationIterator(const std::shared_ptr<OperationLogConfig>& opConfig, const OperationMeta& operationMeta,
                             segmentid_t segmentId, size_t offset, const indexlibv2::framework::Locator& locator);
    virtual ~SegmentOperationIterator() {}

public:
    bool HasNext() const { return _lastCursor.pos < int32_t(_opMeta.GetOperationCount()) - 1; }
    virtual std::pair<Status, OperationBase*> Next();
    segmentid_t GetSegmentId() { return _lastCursor.segId; }
    OperationCursor GetLastCursor() const { return _lastCursor; }

protected:
    OperationMeta _opMeta;
    size_t _beginPos; // points to the first operation to be read
    indexlibv2::framework::Locator _locator;
    OperationCursor _lastCursor;
    OperationFactory _opFactory;
};

} // namespace indexlib::index
