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

#include "autil/EnvUtil.h"
#include "build_service/tools/remote_logger/RemoteLogger.h"
#include "build_service/util/LogSetupGuard.h"
#include "fslib/fs/FileSystem.h"
#include "kmonitor/client/KMonitorFactory.h"

using namespace std;

static const string usage = "remote_logger_agent";

build_service::tools::RemoteLogger uploader;

static void handleSignal(int sig) { uploader.stop(); }

int main(int argc, char** argv)
{
    signal(SIGTERM, handleSignal);
    {
        build_service::util::LogSetupGuard logGuard(build_service::util::LogSetupGuard::FILE_LOG_CONF);
        char cwdPath[PATH_MAX];
        char* ret = getcwd(cwdPath, PATH_MAX);
        if (!ret) {
            std::cerr << "get cwd failed" << std::endl;
            return -1;
        }
        string srcDir = string(cwdPath) + "/../build_service_worker";
        string pipeLineFile = autil::EnvUtil::getEnv("BS_LOG_PIPE_FILE", string("bs_log_pipeline_file"));
        int64_t keepLogHour = autil::EnvUtil::getEnv("BS_REMOTE_LOG_KEEP_HOUR", 72);
        pipeLineFile = srcDir + string("/") + pipeLineFile;
        if (!uploader.run(srcDir, pipeLineFile, keepLogHour)) {
            std::cerr << "init failed" << std::endl;
            return -1;
        }
        std::cerr << "log uploader exit" << std::endl;
    }
    fslib::fs::FileSystem::close();
    kmonitor::KMonitorFactory::Shutdown();
    AUTIL_LOG_SHUTDOWN();
    return 0;
}
