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
#include <signal.h>
#include <string>
#include <unistd.h>

#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "swift/tools/LogSetup.h"
#include "swift/tools/local_admin_daemon/LocalAdminDaemon.h"

using namespace std;
using namespace autil;
using namespace swift::tools;
static const string usage = "local_admin_daemon [configPath] [workDir] [binaryPath]";

volatile static bool isStop = false;

static void handleSignal(int sig) {
    // avoid log after global dtor
    if (!isStop) {
        cout << "receive signal " << sig << ", stop." << endl;
    }
    isStop = true;
}

static void registerSignalHandler() {
    signal(SIGINT, handleSignal);
    signal(SIGTERM, handleSignal);
    signal(SIGUSR1, handleSignal);
    signal(SIGUSR2, handleSignal);
}

int main(int argc, char **argv) {
    swift::tools::LogSetup logger;
    isStop = false;
    registerSignalHandler();
    if (argc < 4) {
        cout << usage << endl;
        return -1;
    }

    string configPath = argv[1];
    string workDir = argv[2];
    string binaryPath = argv[3];

    LocalAdminDaemon adminDaemon;
    if (!adminDaemon.init(configPath, workDir, binaryPath)) {
        string errStr = autil::StringUtil::formatString(
            "init admin daemon failed, config path [%s], work dir[%s], binary path [%s]",
            configPath.c_str(),
            workDir.c_str(),
            binaryPath.c_str());
        cout << errStr << endl;
        return -1;
    }
    while (!isStop) {
        if (!adminDaemon.daemonRun()) {
            cout << "deamon run failed, stop daemon" << endl;
            isStop = true;
        }
        usleep(200 * 1000);
    }
    cout << "stop local admin daemon." << endl;
    return 0;
}
