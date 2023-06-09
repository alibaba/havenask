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
#include "indexlib/file_system/flush/MkdirFlushOperation.h"

#include <assert.h>
#include <iosfwd>
#include <memory>

#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, MkdirFlushOperation);

MkdirFlushOperation::MkdirFlushOperation(const FlushRetryStrategy& flushRetryStrategy,
                                         const std::shared_ptr<FileNode>& fileNode)
    : _flushRetryStrategy(flushRetryStrategy)
    , _destPath(fileNode->GetPhysicalPath())
    , _fileNode(fileNode)
{
    assert(fileNode);
}

MkdirFlushOperation::~MkdirFlushOperation() {}

ErrorCode MkdirFlushOperation::Execute() noexcept
{
    assert(!_destPath.empty());
    int32_t maxRetryTimes = _flushRetryStrategy.retryTimes >= 0 ? _flushRetryStrategy.retryTimes : 0;
    for (int32_t time = 0; time <= maxRetryTimes; ++time) {
        auto [ec, exist] = FslibWrapper::IsExist(_destPath);
        if (ec != FSEC_OK) {
            AUTIL_LOG(WARN, "is exist [%s] failed, ec[%d], retry[%d/%d]", _destPath.c_str(), ec, time + 1,
                      maxRetryTimes);
            sleep(_flushRetryStrategy.retryInterval);
            continue;
        }
        if (!exist) {
            ec = FslibWrapper::MkDir(_destPath, true, true).Code();
            if (ec != FSEC_OK) {
                AUTIL_LOG(WARN, "mkdir [%s] failed, ec[%d], retry[%d/%d]", _destPath.c_str(), ec, time + 1,
                          maxRetryTimes);
                sleep(_flushRetryStrategy.retryInterval);
                continue;
            }
        }
        return FSEC_OK;
    }
    AUTIL_LOG(ERROR, "execute failed, path[%s]", _destPath.c_str());
    return FSEC_ERROR;
}

}} // namespace indexlib::file_system
