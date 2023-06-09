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
#ifndef FSLIB_FILESYSTEMMANAGER_H
#define FSLIB_FILESYSTEMMANAGER_H

#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"

FSLIB_BEGIN_NAMESPACE(fs);

class AbstractFileSystem;

class FileSystemManager
{
public:
    static AbstractFileSystem* getFs(const FsType& type, bool safe = true);
    static void setFs(const FsType& type, AbstractFileSystem* fs, bool safe = true);

    static AbstractFileSystem* getRawFs(const FsType& type, bool safe = true);
    static void setRawFs(const FsType& type, AbstractFileSystem* fs, bool safe = true);

    static AbstractFileSystem* getRawFs(AbstractFileSystem* fs);
    static void destroyFs(const FsType& type);

    static void close();
    static bool init();
};

FSLIB_END_NAMESPACE(fs);

#endif //FSLIB_FILESYSTEMMANAGER_H
