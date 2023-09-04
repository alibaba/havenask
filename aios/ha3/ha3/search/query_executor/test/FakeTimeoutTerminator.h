#pragma once

#include <memory>
#include <stdint.h>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/TimeoutTerminator.h"

namespace isearch {
namespace search {
class FakeTimeoutTerminator : public common::TimeoutTerminator {
public:
    FakeTimeoutTerminator(int timeOutCount);
    ~FakeTimeoutTerminator();

private:
    FakeTimeoutTerminator(const FakeTimeoutTerminator &);
    FakeTimeoutTerminator &operator=(const FakeTimeoutTerminator &);

protected:
    int64_t getCurrentTime() override;

private:
    int _timeOutCount;
    int _curCount;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FakeTimeoutTerminator> FakeTimeoutTerminatorPtr;

} // namespace search
} // namespace isearch
