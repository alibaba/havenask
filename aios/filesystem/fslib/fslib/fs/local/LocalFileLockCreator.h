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
#ifndef FSLIB_LOCALFILELOCKCREATOR_H
#define FSLIB_LOCALFILELOCKCREATOR_H

#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileLockCreator.h"

FSLIB_BEGIN_NAMESPACE(fs);

class LocalFileLockCreator : public FileLockCreator
{
public:
    /*override*/ FileReadWriteLock* createFileReadWriteLock(
            const std::string& fileName);
    /*override*/ FileLock* createFileLock(const std::string& fileName);
};

FSLIB_TYPEDEF_AUTO_PTR(LocalFileLockCreator);

FSLIB_END_NAMESPACE(fs);

#endif //FSLIB_LOCALFILELOCKCREATOR_H
