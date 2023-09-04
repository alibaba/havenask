#include <deque>
#include <fstream>
#include <sys/types.h>
#include <dirent.h>

#include "gmock/gmock.h"
#include "alog/Logger.h"
#include "alog/Configurator.h"
#include "autil/Backtrace.h"
#include "autil/EnvUtil.h"
using namespace std;
using namespace ::testing;

int TEST_ARGC;
char** TEST_ARGV;
#define MAX_CWD_LENGTH 10240

const std::string UNITTEST_DEFAULT_LOG_CONF = R"conf(
alog.rootLogger=WARN, unittestAppender
alog.max_msg_len=4096
alog.appender.unittestAppender=ConsoleAppender
alog.appender.unittestAppender.flush=true
alog.appender.unittestAppender.layout=PatternLayout
alog.appender.unittestAppender.layout.LogPattern=[%%d] [%%l] [%%t,%%F -- %%f():%%n] [%%m]
alog.logger.arpc=WARN
)conf";

static void findFile(const string &path, const string &fileName, vector<string> &result) {
    deque<string> queue;
    queue.emplace_back(path);
    while (!queue.empty()) {
        auto path = queue.front();
        queue.pop_front();
        DIR *dir;
        struct dirent *entry;
        if (!(dir = opendir(path.c_str())))
            continue;
        while ((entry = readdir(dir)) != NULL) {
            if (entry->d_type == DT_DIR) {
                if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                    continue;
                queue.emplace_back(path + "/" + entry->d_name);
            } else {
                if (fileName == entry->d_name) {
                    result.emplace_back(path + "/" + entry->d_name);
                }
            }
        }
        closedir(dir);
    }
}


int main(int argc, char **argv, char **envp) {
    TEST_ARGC = argc;
    TEST_ARGV = argv;
    std::string cwd;
    std::string test_binary = argv[0];
    {
        char buf[PATH_MAX];
        if(getcwd(buf, PATH_MAX)==nullptr) {
            return -1;
        }
        cwd = buf;
    }

    std::stringstream ss;
    ss << "cd " << cwd << "; ";
    std::cout << "CWD:" <<std::endl << cwd << std::endl;
    std::cout << "TEST_BINARY:"<<std::endl << test_binary << std::endl;
    std::set<std::string> testVars = {"TEST_BINARY", "TEST_SRCDIR", "TEST_TMPDIR", "GTEST_TMP_DIR", "TEST_WORKSPACE"};
    std::cout << "ENV_VAR_AS_FOLLOW:" << std::endl;
    for (char **env = envp; *env != 0; env++) {
        std::string envStr = *env;
        for (const auto &var : testVars) {
            if (envStr.find(var) == 0) {
                std::cout << envStr << std::endl;
                ss << envStr << ' ';
            }
        }
    }
    std::cout << std::endl << "gdb cmd: " << std::endl;
    ss << " gdb " << test_binary;
    std::cout << ss.str() << std::endl;

    vector<string> alogConfigList;
    findFile(cwd, "test_alog.conf", alogConfigList);
    // Backtrace
    // 开启方式: bazel test //xxxx/xxx:xxx --test_env backtrace=true
    // 开启后case若发生coredump将自动打印堆栈
    // 行号显示可能不是很准确
    auto backtrace = autil::EnvUtil::getEnv("backtrace", false);
    if (backtrace) {
        std::cout << "backtrace on!" << std::endl;
        autil::Backtrace::Register();
    }

    try {
        if (alogConfigList.size() > 0) {
            string confFile = alogConfigList[0];
            ifstream f(confFile.c_str(), ios::in);
            std::cout << "ALOG_CONFIG: " << confFile << std::endl;
            assert(f.is_open());
            alog::Configurator::configureLogger(confFile.c_str());
        } else {
            std::cout << "ALOG_CONFIG: use default config" << std::endl;
            alog::Configurator::configureLoggerFromString(UNITTEST_DEFAULT_LOG_CONF.c_str());
        }
    } catch(std::exception &e) {
        cerr << "WARN! Failed to configure logger!"
             << e.what() << ",use default log conf."
             << endl;
        alog::Configurator::configureRootLogger();
    }

    InitGoogleMock(&argc, argv);
    int ret = RUN_ALL_TESTS();

    alog::Logger::flushAll();
    alog::Logger::shutdown();

    return ret;
}
