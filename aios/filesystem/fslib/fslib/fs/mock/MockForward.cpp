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
#include <stdlib.h>

#include "fslib/fs/mock/MockFS.h"
#include "fslib/fslib.h"
using namespace std;
using namespace fslib;

FSLIB_PLUGIN_BEGIN_NAMESPACE(mock);

typedef std::function<ErrorCode(const std::string &path, const std::string &args, std::string &output)> ForwardFunc;
extern "C" void mockForward(const std::string &command, const ForwardFunc &func) {
    return MockFS::mockForward(command, func);
}

FSLIB_PLUGIN_END_NAMESPACE(mock);
