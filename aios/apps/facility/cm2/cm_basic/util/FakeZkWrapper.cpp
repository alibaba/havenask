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
#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "fslib/fs/FileSystem.h"

using namespace fslib;
using namespace fslib::fs;

namespace cm_basic {

ZkWrapper::ZkWrapper(const std::string& host, unsigned int timeout_s, bool isMillisecond) {}

bool ZkWrapper::isBad() const { return false; }

bool ZkWrapper::open() { return true; }

bool ZkWrapper::set(const std::string& path, const std::string& value)
{
    return EC_OK != FileSystem::writeFile(path, value);
}

bool ZkWrapper::touch(const std::string& path, const std::string& value, bool permanent)
{
    std::string parentDir = FileSystem::getParentPath(path);
    auto ec = FileSystem::isExist(parentDir);
    if (ec != EC_TRUE) {
        if (ec != EC_FALSE) {
            return false;
        }
        if (FileSystem::mkDir(parentDir, true) != EC_OK) {
            return false;
        }
    }
    return FileSystem::writeFile(path, value) == EC_OK;
}

ZOO_ERRORS ZkWrapper::getData(const std::string& path, std::string& str, bool watch)
{
    if (FileSystem::readFile(path, str) == EC_OK) {
        return ZOK;
    }
    auto ec = FileSystem::isExist(path);
    if (ec == EC_FALSE) {
        return ZNONODE;
    }
    return ZCONNECTIONLOSS;
}

bool ZkWrapper::check(const std::string& path, bool& bExist, bool watch)
{
    auto ec = FileSystem::isExist(path);
    if (ec == EC_TRUE) {
        bExist = true;
    } else if (ec == EC_FALSE) {
        bExist = false;
    } else {
        return false;
    }
    return true;
}

ZOO_ERRORS ZkWrapper::getChild(const std::string& path, std::vector<std::string>& vString, bool watch)
{
    if (FileSystem::listDir(path, vString) == EC_OK) {
        return ZOK;
    }
    return ZCONNECTIONLOSS;
}

void ZkWrapper::close() {}

} // namespace cm_basic
