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
#include "indexlib/file_system/flush/FileFlushOperation.h"

#include "autil/Log.h"
#include "indexlib/util/RetryUtil.h"

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FileFlushOperation);

ErrorCode FileFlushOperation::Execute() noexcept
{
    ErrorCode ec = FSEC_OK;
    auto execute = [this, &ec]() -> bool {
        ec = DoExecute();
        if (likely(ec == FSEC_OK)) {
            return true;
        }
        AUTIL_LOG(WARN, "execute failed, will retry");
        return false;
    };
    util::RetryUtil::RetryOptions options;
    options.retryIntervalInSecond = _flushRetryStrategy.retryInterval;
    options.maxRetryTime = _flushRetryStrategy.retryTimes;
    bool ret = util::RetryUtil::Retry(execute, options, std::bind(&FileFlushOperation::CleanDirtyFile, this));
    if (!ret) {
        AUTIL_LOG(ERROR, "execute failed, retry times[%d]", options.maxRetryTime);
    }
    return ec;
}

ErrorCode FileFlushOperation::SplitWrite(const std::unique_ptr<FslibFileWrapper>& file, const void* buffer,
                                         size_t length) noexcept
{
    size_t leftToWriteLen = length;
    size_t totalWriteLen = 0;
    while (leftToWriteLen > 0) {
        size_t writeLen = (leftToWriteLen > DEFAULT_FLUSH_BUF_SIZE) ? DEFAULT_FLUSH_BUF_SIZE : leftToWriteLen;
        RETURN_IF_FS_ERROR(file->NiceWrite((uint8_t*)buffer + totalWriteLen, writeLen), "");
        leftToWriteLen -= writeLen;
        totalWriteLen += writeLen;
    }
    return FSEC_OK;
}

}} // namespace indexlib::file_system
