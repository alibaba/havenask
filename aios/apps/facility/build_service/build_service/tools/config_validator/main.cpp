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

#include "build_service/admin/ConfigValidator.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/LogSetupGuard.h"

using namespace std;
using build_service::admin::ConfigValidator;
using build_service::util::LogSetupGuard;

static const string usage = "config_validator [configPath]";

int main(int argc, char** argv)
{
    LogSetupGuard logGuard;
    if (argc != 2) {
        cerr << usage << endl;
        BS_LOG_FLUSH();
        return -1;
    }
    string configPath = argv[1];

    ConfigValidator validator;
    if (!validator.validate(configPath, false)) {
        cerr << "validate config failed." << endl;
        return -1;
    }
    return 0;
}
