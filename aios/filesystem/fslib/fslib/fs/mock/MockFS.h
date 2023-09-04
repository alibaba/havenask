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
#ifndef FSLIB_MOCKFS_H
#define FSLIB_MOCKFS_H

#include <unordered_map>

#include "autil/Log.h"
#include "fslib/fs/mock/common.h"
#include "fslib/fslib.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(mock);
class MockFileSystem;

class MockFS {
public:
    typedef std::function<ErrorCode(const std::string &path, const std::string &args, std::string &output)> ForwardFunc;

public:
    MockFS() = delete;
    ~MockFS() = delete;

public:
    static void mockForward(const std::string &command, const ForwardFunc &func);
    static void setLocalRoot(const std::string &root);
    static std::string getLocalRoot();

private:
    static MockFileSystem *getMockFs();
};

FSLIB_TYPEDEF_AUTO_PTR(MockFS);

FSLIB_PLUGIN_END_NAMESPACE(mock);

#endif // FSLIB_MOCKFS_H
