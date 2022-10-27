#include "FakeThread.h"
#include "build_service/test/unittest.h"

namespace autil {

bool gCreateThreadFail = false;

void setCreateThreadFail(bool fail) {
    gCreateThreadFail = fail;
}

ThreadPtr Thread::createThread(const std::tr1::function<void ()>& threadFunction, const std::string &name) {
    if (gCreateThreadFail) {
        return ThreadPtr();
    }
    typedef ThreadPtr (*func_type)(const std::tr1::function<void ()>&, const std::string &);
    func_type func = GET_NEXT_FUNC(func_type);

    return func(threadFunction, name);
}

}
