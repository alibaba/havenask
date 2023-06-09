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
#include "fslib/fs/local/LocalFileLockCreator.h"
#include "fslib/fs/local/LocalFileReadWriteLock.h"
#include "fslib/fs/local/LocalFileLock.h"

using namespace std;
FSLIB_BEGIN_NAMESPACE(fs);
//AUTIL_DECLARE_AND_SETUP_LOGGER(fs, LocalFileLockCreator);

FileReadWriteLock* LocalFileLockCreator::createFileReadWriteLock(
        const string& fileName) 
{
    return new LocalFileReadWriteLock(fileName);
}

FileLock* LocalFileLockCreator::createFileLock(const string& fileName) 
{
    return new LocalFileLock(fileName);
}

FSLIB_END_NAMESPACE(fs);

