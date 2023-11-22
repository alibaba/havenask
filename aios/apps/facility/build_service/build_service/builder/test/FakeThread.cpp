#include "build_service/builder/test/FakeThread.h"

#include <functional>
#include <string>

#include "autil/Thread.h"
#include "unittest/unittest.h"

namespace autil {

bool gCreateThreadFail = false;

void setCreateThreadFail(bool fail) { gCreateThreadFail = fail; }

ThreadPtr Thread::createThread(const std::function<void()>& threadFunction, const std::string& name)
{
    if (gCreateThreadFail) {
        return ThreadPtr();
    }
    typedef ThreadPtr (*func_type)(const std::function<void()>&, const std::string&);
    func_type func = GET_NEXT_FUNC(func_type);

    return func(threadFunction, name);
}

} // namespace autil
