#pragma once
#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/protocol/Transport.pb.h"

namespace swift {
namespace service {

class FakeClosure : public ::google::protobuf::Closure {
public:
    FakeClosure() { _done = false; }
    ~FakeClosure(){};

private:
    FakeClosure(const FakeClosure &);
    FakeClosure &operator=(const FakeClosure &);

public:
    void Run() { _done = true; }
    bool getDoneStatus() { return _done; }

private:
    bool _done;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeClosure);

} // namespace service
} // namespace swift
