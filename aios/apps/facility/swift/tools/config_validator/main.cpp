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

#include "swift/tools/LogSetup.h"
#include "swift/tools/config_validator/ConfigValidator.h"

using namespace std;
static const string usage = "config_validator [configPath]";
int main(int argc, char **argv) {
    swift::tools::LogSetup logger;
    if (argc != 2) {
        cerr << usage << endl;
        return -1;
    }
    string configPath = argv[1];

    swift::tools::ConfigValidator validator;
    if (!validator.validate(configPath)) {
        cerr << "validate config failed." << endl;
        return -1;
    }
    cout << "validate config success!" << endl;
    return 0;
}
