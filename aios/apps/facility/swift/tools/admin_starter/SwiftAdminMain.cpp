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
#include <memory>
#include <stddef.h>
#include <string>

#include "fslib/fs/FileSystem.h"
#include "swift/config/AdminConfig.h"
#include "swift/config/AuthorizerInfo.h"
#include "swift/tools/LogSetup.h"
#include "swift/tools/admin_starter/DispatchHelper.h"
#include "swift/tools/admin_starter/LocalHelper.h"

using namespace std;
using namespace swift::config;
using namespace swift::tools;
static const string usage = "admin_starter [configPath] [action] [workDir] [binaryPath]";

int main(int argc, char **argv) {
    swift::tools::LogSetup logger;
    if (argc < 3) {
        cout << usage << endl;
        return -1;
    }

    const string &configPath = argv[1];
    const string &action = argv[2];
    string workDir;
    string binaryPath;
    if (argc > 3) {
        workDir = argv[3];
    }
    if (argc > 4) {
        binaryPath = argv[4];
    }
    AdminConfig *config = AdminConfig::loadConfig(configPath);
    if (NULL == config) {
        cout << "load config failed" << endl;
        return -1;
    }
    DispatchHelperPtr dispatcher;
    if (config->isLocalMode()) {
        dispatcher.reset(new LocalHelper(binaryPath, workDir));
    } else {
    }

    if (dispatcher == nullptr || !dispatcher->init(config)) {
        cout << "HippoHelper init failed." << endl;
        return -1;
    }
    if (action == "start") {
        if (!dispatcher->startApp()) {
            cout << "start admin failed" << endl;
            return -1;
        }
    } else if (action == "stop") {
        if (!dispatcher->stopApp()) {
            cout << "stop admin failed" << endl;
            return -1;
        }
    } else if (action == "status") {
        if (!dispatcher->getAppStatus()) {
            cout << "get admin status failed" << endl;
            return -1;
        }
    } else if (action == "updateAdminConf") {
        if (!dispatcher->updateAdminConf()) {
            cout << "update admin conf failed" << endl;
            return -1;
        }
    } else {
        cout << usage << endl;
        return -1;
    }
    fslib::fs::FileSystem::close(); // unload fslib plugin
    return 0;
}
