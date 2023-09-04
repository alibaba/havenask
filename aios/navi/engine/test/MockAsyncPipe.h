#pragma once

#include "unittest/unittest.h"
#include "navi/engine/AsyncPipe.h"

namespace navi {

class MockAsyncPipe : public navi::AsyncPipe {
public:
    MockAsyncPipe()
        : navi::AsyncPipe(nullptr, AS_REQUIRED) {}
public:
    MOCK_METHOD0(setEof, ErrorCode());
    MOCK_METHOD1(setData, ErrorCode(const DataPtr &));
    MOCK_METHOD2(getData, bool(DataPtr &, bool &));
    MOCK_METHOD0(terminate, void());
};

}
