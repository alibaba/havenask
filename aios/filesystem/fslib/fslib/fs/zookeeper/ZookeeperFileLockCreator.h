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
#ifndef FSLIB_PLUGIN_ZOOKEEPERFILELOCKCREATOR_H
#define FSLIB_PLUGIN_ZOOKEEPERFILELOCKCREATOR_H

#include "autil/Log.h"
#include "fslib/common.h"
#include "fslib/fs/FileLockCreator.h"

using namespace fslib::fs;
FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);

class ZookeeperFileLockCreator : public FileLockCreator {
public:
    /*override*/ FileReadWriteLock *createFileReadWriteLock(const std::string &fileName);

    /*override*/ FileLock *createFileLock(const std::string &fileName);
};

typedef std::unique_ptr<ZookeeperFileLockCreator> ZookeeperFileLockCreatorPtr;

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);

#endif // FSLIB_PLUGIN_ZOOKEEPERFILELOCKCREATOR_H
