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
#include "fslib/fs/FileSystemManager.h"
#include "fslib/fs/FileSystemFactory.h"
#include "fslib/fs/AbstractFileSystem.h"

using namespace std;
using namespace autil;
FSLIB_BEGIN_NAMESPACE(fs);

AUTIL_DECLARE_AND_SETUP_LOGGER(fs, FileSystemManager);

AbstractFileSystem* FileSystemManager::getFs(const FsType& type, bool safe)
{
    return FileSystemFactory::getInstance()->getFs(type, safe);
}

void FileSystemManager::setFs(const FsType& type, AbstractFileSystem* fs, bool safe)
{
    FileSystemFactory::getInstance()->setFs(type, fs, safe);
}

AbstractFileSystem* FileSystemManager::getRawFs(const FsType& type, bool safe)
{
    return FileSystemFactory::getInstance()->getRawFs(type, safe);
}

void FileSystemManager::setRawFs(const FsType& type, AbstractFileSystem* fs, bool safe)
{
    FileSystemFactory::getInstance()->setFs(type, fs, safe);
}

AbstractFileSystem* FileSystemManager::getRawFs(AbstractFileSystem* fs)
{
    return FileSystemFactory::getRawFs(fs);
}

void FileSystemManager::destroyFs(const FsType& type)
{
    FileSystemFactory::getInstance()->destroyFs(type);
}

void FileSystemManager::close()
{
    FileSystemFactory::getInstance()->close();
}

bool FileSystemManager::init()
{
    return FileSystemFactory::getInstance()->init();
}

FSLIB_END_NAMESPACE(fs);
