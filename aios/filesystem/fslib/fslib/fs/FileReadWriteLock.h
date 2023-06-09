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
#ifndef FSLIB_FILEREADWRITELOCK_H
#define FSLIB_FILEREADWRITELOCK_H

#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"

FSLIB_BEGIN_NAMESPACE(fs);

class FileReadWriteLock
{
public:
    virtual ~FileReadWriteLock() {}

public:
    /**
     * rdlock request read lock for the file
     * @param timeout [in] timeout for waiting for the lock, default timeout is 0, 
     *                     0 means wait forever.
     * @return int, -1 indicates get lock fail, otherwise mean succeed
     */
    virtual int rdlock(uint32_t timeout = 0) = 0;

    /**
     * wrlock request write lock for the file
     * @param timeout [in] timeout for waiting for the lock, default timeout is 0, 
     *                     0 means wait forever.
     * @return int, -1 indicates get lock fail, otherwise mean succeed
     */
    virtual int wrlock(uint32_t timeout = 0) = 0;

    /**
     * unlock release the lock currently hold
     * @return int, -1 indicates release lock fail, otherwise mean succeed
     */
    virtual int unlock() = 0;
    
};

FSLIB_TYPEDEF_AUTO_PTR(FileReadWriteLock);

FSLIB_END_NAMESPACE(fs);

#endif //FSLIB_FILEREADWRITELOCK_H
