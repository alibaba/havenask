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
#include <iostream>
#include <string>

#include "autil/Scope.h"
#include "build_service/common_define.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/LogSetupGuard.h"
#include "fslib/fs/FileSystem.h"

using namespace std;
using namespace build_service;
using namespace build_service::util;
using build_service::util::LogSetupGuard;

static const string usage = "generation_scanner indexRoot clusterName";

int main(int argc, char** argv)
{
    LogSetupGuard logGuard(LogSetupGuard::FILE_LOG_CONF);
    autil::ScopeGuard dtor([]() { fslib::fs::FileSystem::close(); });
    if (argc != 3) {
        cerr << usage << endl;
        return -1;
    }
    string indexRoot(argv[1]);
    string clusterName(argv[2]);

    vector<generationid_t> generations = IndexPathConstructor::getGenerationList(indexRoot, clusterName);
    if (generations.empty()) {
        cout << -1;
    } else {
        cout << generations.back();
    }

    return 0;
}
