#ifndef ISEARCH_BS_CLOSUREGUARD_H
#define ISEARCH_BS_CLOSUREGUARD_H

#include <google/protobuf/stubs/common.h>

namespace build_service {
namespace common {

class ClosureGuard
{
public:
    ClosureGuard(::google::protobuf::Closure* done)
        : _done(done)
    {
        assert(done);
    }
    ~ClosureGuard() {
        _done->Run();
    }
private:
    ClosureGuard(const ClosureGuard &);
    ClosureGuard& operator=(const ClosureGuard &);
private:
    ::google::protobuf::Closure* _done;
};


}
}

#endif //ISEARCH_BS_CLOSUREGUARD_H
