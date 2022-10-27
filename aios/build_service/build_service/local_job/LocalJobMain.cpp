#include "build_service/util/LogSetupGuard.h"
#include "build_service/local_job/LocalJobWorker.h"
#include "build_service/util/FileUtil.h"
#include <fslib/fslib.h>
#include <autil/OptionParser.h>

using namespace std;
using namespace autil;
using namespace build_service::local_job;
using namespace build_service::util;
using build_service::util::LogSetupGuard;

static const string usage = "local_job -s [build|merge|endmerge] -m [mapCount] -r [reduceCount] -p [jobParams] -l logConf";

bool readConfig(const string confPath, string &confContent) {
    fslib::ErrorCode errorCode = fslib::fs::FileSystem::isExist(confPath);

    if (fslib::EC_TRUE == errorCode) {
        if (!FileUtil::readFile(confPath, confContent)) {
            cerr << "Failed to read Logger config file ["
                      << confPath << "], maybe it's a directory"
                      << endl;
            return false;
        }
        return true;
    } else {
        cerr << "config file [" << confPath
             << "] doesn't exist. errorCode: [" << errorCode
             << "], errorMsg: ["
             << fslib::fs::FileSystem::getErrorString(errorCode)
             << "]" << endl;
        return false;
    }
}

int main(int argc, char *argv[]) {
    autil::OptionParser optionParser;
    optionParser.addOption("-s", "--step", "step",
                           OptionParser::OPT_STRING, true);
    optionParser.addOption("-m", "--mapcount", "mapCount",
                           OptionParser::OPT_UINT32, true);
    optionParser.addOption("-r", "--reducecount", "reduceCount",
                           OptionParser::OPT_UINT32, true);
    optionParser.addOption("-p", "--jobparam", "jobParam",
                           OptionParser::OPT_STRING, true);
    optionParser.addOption("-l", "--logConf", "logConf",
                           OptionParser::OPT_STRING, false);

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
    LocalJobWorker worker;
    if (!worker.run(step, mapCount, reduceCount, jobParam)) {
        return -1;
    }
    return 0;
}
