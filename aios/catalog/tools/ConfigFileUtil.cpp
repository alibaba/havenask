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
#include "catalog/tools/ConfigFileUtil.h"

#include "fslib/fs/FileSystem.h"

using namespace std;

namespace catalog {

Status ConfigFileUtil::uploadFile(const string &path, const string &content) {
    if (fslib::fs::FileSystem::isExist(path) == fslib::EC_TRUE) {
        CATALOG_CHECK_OK(rmPath(path));
    }
    auto ec = fslib::fs::FileSystem::writeFile(path, content);
    CATALOG_CHECK(ec == fslib::EC_OK,
                  Status::INTERNAL_ERROR,
                  "write file [",
                  path,
                  "] failed, errorMessage [",
                  fslib::fs::FileSystem::getErrorString(ec).c_str(),
                  "], content size [",
                  content.size(),
                  "]");
    return StatusBuilder::ok();
}

Status ConfigFileUtil::assertPathExist(const string &path, const string &errorMessagePrefix) {
    CATALOG_CHECK(fslib::fs::FileSystem::isExist(path) == fslib::EC_TRUE,
                  Status::INTERNAL_ERROR,
                  errorMessagePrefix,
                  " [",
                  path,
                  "] not exist");
    return StatusBuilder::ok();
}

Status ConfigFileUtil::rmPath(const string &path) {
    if (fslib::fs::FileSystem::isExist(path) == fslib::EC_FALSE) {
        return StatusBuilder::ok();
    }
    auto ec = fslib::fs::FileSystem::remove(path);
    CATALOG_CHECK(ec == fslib::EC_OK,
                  Status::INTERNAL_ERROR,
                  "remove [",
                  path,
                  "] failed, errorMessage [",
                  fslib::fs::FileSystem::getErrorString(ec).c_str(),
                  "]");
    return StatusBuilder::ok();
}
Status ConfigFileUtil::mvPath(const string &srcPath, const string &targetPath) {
    auto ec = fslib::fs::FileSystem::rename(srcPath, targetPath);
    CATALOG_CHECK(ec == fslib::EC_OK,
                  Status::INTERNAL_ERROR,
                  "rename [",
                  srcPath,
                  "] to [",
                  targetPath,
                  "] failed, errorMessage [",
                  fslib::fs::FileSystem::getErrorString(ec).c_str(),
                  "]");
    return StatusBuilder::ok();
}

Status ConfigFileUtil::mkDir(const string &path, bool recursive) {
    auto ec = fslib::fs::FileSystem::mkDir(path, recursive);
    CATALOG_CHECK(ec == fslib::EC_OK,
                  Status::INTERNAL_ERROR,
                  "create directory [",
                  path,
                  "] failed, errorMessage [",
                  fslib::fs::FileSystem::getErrorString(ec).c_str(),
                  "]");
    return StatusBuilder::ok();
}

Status ConfigFileUtil::readFile(const string &filePath, string *content) {
    auto ec = fslib::fs::FileSystem::readFile(filePath, *content);
    CATALOG_CHECK(ec == fslib::EC_OK,
                  Status::INTERNAL_ERROR,
                  "read file [",
                  filePath,
                  "] failed, errorMessage [",
                  fslib::fs::FileSystem::getErrorString(ec).c_str(),
                  "]");
    return StatusBuilder::ok();
}

Status ConfigFileUtil::copyDir(const string &srcPath, const string &targetPath) {
    auto ec = fslib::fs::FileSystem::copy(srcPath, targetPath);
    CATALOG_CHECK(ec == fslib::EC_OK,
                  Status::INTERNAL_ERROR,
                  "copy [",
                  srcPath,
                  "] to [",
                  targetPath,
                  "] failed, errorMessage [",
                  fslib::fs::FileSystem::getErrorString(ec).c_str(),
                  "]");
    return StatusBuilder::ok();
}

} // namespace catalog
