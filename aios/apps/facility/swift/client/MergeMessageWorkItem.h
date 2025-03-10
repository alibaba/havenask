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
#include "autil/WorkItem.h"
#include "swift/client/MessageWriteBuffer.h"
#include "swift/common/Common.h"

namespace swift {
namespace client {
class MergeMessageWorkItem : public autil::WorkItem {
public:
    MergeMessageWorkItem(MessageWriteBuffer *writeBuffer) {
        _writeBuffer = writeBuffer;
        _writeBuffer->setInMerge(true);
    }
    ~MergeMessageWorkItem() { _writeBuffer->setInMerge(false); }

private:
    MergeMessageWorkItem(const MergeMessageWorkItem &);
    MergeMessageWorkItem &operator=(const MergeMessageWorkItem &);

public:
    virtual void process() { _writeBuffer->convertMessage(); }

private:
    MessageWriteBuffer *_writeBuffer;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MergeMessageWorkItem);

} // namespace client
} // namespace swift
