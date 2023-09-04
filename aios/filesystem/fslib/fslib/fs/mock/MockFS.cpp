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
#include "fslib/fs/mock/MockFS.h"

#include "fslib/fs/FileSystemManager.h"
#include "fslib/fs/mock/MockFileSystem.h"

using namespace std;
using namespace autil;
using namespace fslib::fs;

FSLIB_PLUGIN_BEGIN_NAMESPACE(mock);
AUTIL_DECLARE_AND_SETUP_LOGGER(mock, MockFS);

MockFileSystem *MockFS::getMockFs() { return dynamic_cast<MockFileSystem *>(FileSystemManager::getRawFs("mock")); }

void MockFS::setLocalRoot(const string &root) { return getMockFs()->setLocalRoot(root); }

string MockFS::getLocalRoot() { return getMockFs()->getLocalRoot(); }

void MockFS::mockForward(const string &command, const ForwardFunc &func) {
    return getMockFs()->mockForward(command, func);
}

FSLIB_PLUGIN_END_NAMESPACE(mock);
