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
#include "indexlib/base/Status.h"
namespace indexlibv2 { namespace framework {
class Locator;
}} // namespace indexlibv2::framework

namespace indexlib::index {
class SegmentOperationIterator;
class OperationMeta;
class BatchOpLogIterator;
class OperationLogIndexer
{
public:
    OperationLogIndexer() {}
    virtual ~OperationLogIndexer() {}

public:
    virtual segmentid_t GetSegmentId() const = 0;
    virtual bool GetOperationMeta(OperationMeta& operationMeta) const = 0;
    virtual std::pair<Status, std::shared_ptr<SegmentOperationIterator>>
    CreateSegmentOperationIterator(size_t offset, const indexlibv2::framework::Locator& locator) = 0;
    virtual std::pair<Status, std::shared_ptr<BatchOpLogIterator>> CreateBatchIterator() = 0;
    virtual bool IsSealed() const = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
