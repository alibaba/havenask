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
#include <sstream>
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "Server.h"

using namespace std;
using namespace autil;

void constructUsageDesc(stringstream &ss) {
    ss << "Usage: havenask -r <root_dir> -k start|stop|restart [-l <log_conf_path>] [-d]" << endl;
    ss << "Options:" << endl;
    ss << "  -r|--root <root_dir>     : specify install root dir"  << endl;
    ss << "  -l|--log <log_conf_path> : specify an alternate log serup conf file" << endl;
//    ss << "  -k start|stop|restart    : specify an alternate server operator" << endl;
    ss << "  -e|--env <KEY=VALUE>     : set environ" << endl;
    ss << "  -d                       : set self process as daemon" << endl;
    ss << "  -h|--help                : list available command line options (this page)" << endl;
}

bool initOptions(autil::OptionParser &optionParser, int argc, char **argv) {
    stringstream ss;
    constructUsageDesc(ss);
    optionParser.updateUsageDescription(ss.str());

//    optionParser.addOption("-k", "", "cmd", autil::OptionParser::OPT_STRING, true);
    optionParser.addOption("-r", "--root", "rootDir", autil::OptionParser::OPT_STRING, true);

    optionParser.addOption("-l", "--logConf", "logConf", autil::OptionParser::OPT_STRING, false);
    optionParser.addOption("-d", "", "daemon", false);
    //optionParser.addOption("-n", "", "noPidFile", autil::OptionParser::OPT_STRING, false);
    //env is multi value
    optionParser.addMultiValueOption("", "--env", "env");
    optionParser.addOption("-h", "--help", "help", autil::OptionParser::OPT_HELP, false);
    if (!optionParser.parseArgs(argc, argv)) {
        return false;
    }

    return true;
}

void initAlog(const OptionParser &optionParser) {
    const static string defaultAlogConf = R"(alog.rootLogger=INFO, ha3Appender
alog.max_msg_len=2000000
alog.appender.ha3Appender=FileAppender
alog.appender.ha3Appender.fileName=logs/ha3.log
alog.appender.ha3Appender.layout=PatternLayout
alog.appender.ha3Appender.layout.LogPattern=[%%d] [%%l] [%%t,%%F -- %%f():%%n] [%%m]
alog.appender.ha3Appender.cache_limit=128
alog.appender.ha3Appender.compress=true
alog.appender.ha3Appender.max_file_size=1024
alog.appender.ha3Appender.log_keep_count=100
alog.appender.ha3Appender.async_flush=true
alog.appender.ha3Appender.flush_threshold=1024
alog.appender.ha3Appender.flush_interval=1000)";

    string logFile;
    if (optionParser.getOptionValue("logConf", logFile)) {
        if (fslib::EC_TRUE == fslib::fs::FileSystem::isExist(logFile)) {
            string content;
            if (fslib::EC_OK != fslib::fs::FileSystem::readFile(logFile, content)) {
                fprintf(stderr, "read log config file[%s] failed.", logFile.c_str());
            }
            AUTIL_LOG_CONFIG_FROM_STRING(content.c_str());
        } else {
            fprintf(stderr, "log config file does not exist [%s]\n", logFile.c_str());
        }
        return;
    }
    AUTIL_LOG_CONFIG_FROM_STRING(defaultAlogConf.c_str());
}

void initEnvArgs(const OptionParser &optionParser) {
    vector<string> envArgs;
    if (!optionParser.getOptionValue("env", envArgs)) {
        return;
    }
    for (auto &envArg : envArgs) {
        size_t idx = envArg.find('=');
        if(idx == string::npos || idx == 0) {
            fprintf(stderr, "invalid env: %s\n", envArg.c_str());
            return;
        }
        string key = envArg.substr(0, idx);
        string value = envArg.substr(idx + 1);
        if(!autil::EnvUtil::setEnv(key, value, true)) {
            fprintf(stderr, "export env %s failed\n", envArg.c_str());
            return;
        }
    }
}

int main(int argc, char * argv[])
{
    OptionParser optionParser;
    if (!initOptions(optionParser, argc, argv)) {
        return -1;
    }
    bool daemonTag = false;
    if (optionParser.getOptionValue("daemon", daemonTag) && daemonTag) {
        fprintf(stdout, "begin set self process as daemon.");
        if (daemon(1, 1) == -1) {
            fprintf(stdout, "set self process as daemon failed.");
            return -1;
        }
        fprintf(stdout, "begin set self process as daemon success.");
    }
    initAlog(optionParser);
    initEnvArgs(optionParser);
    isearch::application::Server *server = new isearch::application::Server();
    bool ret = false;
    if (server->init(optionParser)) {
        ret = true;
        server->run();
    }
    delete server;
    AUTIL_LOG_SHUTDOWN();
    return ret ? 0 : -1;
}
