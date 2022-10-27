#ifndef ISEARCH_BS_ASYNCSTARTER_H
#define ISEARCH_BS_ASYNCSTARTER_H

#include <string>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <tr1/functional>
#include <autil/Thread.h>

namespace build_service {
namespace workflow {

class AsyncStarter
{
public:
    AsyncStarter();
    ~AsyncStarter();
private:
    AsyncStarter(const AsyncStarter &);
    AsyncStarter& operator=(const AsyncStarter &);
public:
    typedef std::tr1::function<bool()> func_type;
public:
    void setName(const std::string &name) { _name = name; }
    const std::string &getName() { return _name; }

    void asyncStart(const func_type &func);
    void stop();
private:
    void loopStart();
    void getMaxRetryIntervalTime();
private:
    volatile bool _running;
    func_type _func;
    autil::ThreadPtr _startThread;
    int64_t _maxRetryIntervalTime;
    std::string _name;

private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_ASYNCSTARTER_H
