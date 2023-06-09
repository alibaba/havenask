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
#include "autil/OptionParser.h"
#include "autil/Scope.h"
#include "build_service/local_job/LocalJobWorker.h"
#include "build_service/util/LogSetupGuard.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fslib.h"
#include "fslib/util/FileUtil.h"
#include "kmonitor/client/KMonitorFactory.h"
#include "kmonitor/client/core/MetricsConfig.h"

using namespace std;
using namespace autil;
using namespace build_service::local_job;
using namespace build_service::util;
using build_service::util::LogSetupGuard;

static const string usage =
    "local_job -s [build|merge|endmerge] -m [mapCount] -r [reduceCount] -p [jobParams] -l logConf";

bool readConfig(const string confPath, string& confContent)
{
    fslib::ErrorCode errorCode = fslib::fs::FileSystem::isExist(confPath);

    if (fslib::EC_TRUE == errorCode) {
        if (!fslib::util::FileUtil::readFile(confPath, confContent)) {
            cerr << "Failed to read Logger config file [" << confPath << "], maybe it's a directory" << endl;
            return false;
        }
        return true;
    } else {
        cerr << "config file [" << confPath << "] doesn't exist. errorCode: [" << errorCode << "], errorMsg: ["
             << fslib::fs::FileSystem::getErrorString(errorCode) << "]" << endl;
        return false;
    }
}

int main(int argc, char* argv[])
{
    autil::OptionParser optionParser;
    optionParser.addOption("-s", "--step", "step", OptionParser::OPT_STRING, true);
    optionParser.addOption("-m", "--mapcount", "mapCount", OptionParser::OPT_UINT32, true);
    optionParser.addOption("-r", "--reducecount", "reduceCount", OptionParser::OPT_UINT32, true);
    optionParser.addOption("-p", "--jobparam", "jobParam", OptionParser::OPT_STRING, true);
    optionParser.addOption("-l", "--logConf", "logConf", OptionParser::OPT_STRING, false);

    if (!optionParser.parseArgs(argc, argv)) {
        cerr << "option parse failed." << endl;
        cerr << usage << endl;
        return -1;
    }

    string step;
    uint32_t mapCount = 0;
    uint32_t reduceCount = 0;
    string jobParam;
    string logConfPath;

    optionParser.getOptionValue("step", step);
    optionParser.getOptionValue("mapCount", mapCount);
    optionParser.getOptionValue("reduceCount", reduceCount);
    optionParser.getOptionValue("jobParam", jobParam);
    optionParser.getOptionValue("logConf", logConfPath);
    string logConfContent;
    if (!logConfPath.empty()) {
        if (!readConfig(logConfPath, logConfContent)) {
            return -1;
        }
    } else {
        logConfContent = LogSetupGuard::FILE_LOG_CONF;
    }
    LogSetupGuard logGuard(logConfContent);

    kmonitor::MetricsConfig config;
    config.set_use_common_tag(false);

    autil::ScopeGuard dtor([]() {
        fslib::fs::FileSystem::close();
        kmonitor::KMonitorFactory::Shutdown();
    });

    if (!kmonitor::KMonitorFactory::Init(config)) {
        cerr << "init kmontior factory failed" << endl;
        return -1;
    }

    {
        LocalJobWorker worker;
        if (!worker.run(step, mapCount, reduceCount, jobParam)) {
            return -1;
        }
    }
    return 0;
}
