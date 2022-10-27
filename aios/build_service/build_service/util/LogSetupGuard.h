#ifndef ISEARCH_BS_LOGSETUPGUARD_H
#define ISEARCH_BS_LOGSETUPGUARD_H

#include <string>

namespace build_service {
namespace util {

class LogSetupGuard {
public:
    static const std::string CONSOLE_LOG_CONF;
    static const std::string FILE_LOG_CONF;
public:
    LogSetupGuard(const std::string &logConf = CONSOLE_LOG_CONF);
    ~LogSetupGuard();
private:
    LogSetupGuard(const LogSetupGuard &);
    LogSetupGuard& operator=(const LogSetupGuard &);
};

}
}

#endif //ISEARCH_BS_LOGSETUPGUARD_H
