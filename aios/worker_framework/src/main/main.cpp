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
#include <sys/prctl.h>

#include "autil/Log.h"
#include "fslib/fslib.h"
#include "kmonitor/client/KMonitorFactory.h"
#include "worker_base/DisplayVersion.h"
#include "worker_framework/WorkerBase.h"

using namespace std;
using namespace worker_framework;

int main(int argc, char **argv) {
    prctl(PR_SET_DUMPABLE, 1);
    WorkerBase *workerBase = createWorker();
    if (DisplayVersion::display(argc, argv, workerBase->getVersion())) {
        return 0;
    }
    bool succ = workerBase->init(argc, argv);

    if (!succ) {
        cout << "app init failed" << endl;
        delete workerBase;
        return -1;
    }

    succ = workerBase->run();
    delete workerBase;

    fslib::fs::FileSystem::close();
    kmonitor::KMonitorFactory::Shutdown();
    AUTIL_LOG_SHUTDOWN();
    return succ ? 0 : -1;
}
