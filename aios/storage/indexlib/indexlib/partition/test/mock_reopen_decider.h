#ifndef __INDEXLIB_MOCK_REOPEN_DECIDER_H
#define __INDEXLIB_MOCK_REOPEN_DECIDER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/reopen_decider.h"

namespace indexlib { namespace partition {

class MockReopenDecider : public ReopenDecider
{
public:
    MockReopenDecider(const config::OnlineConfig& onlineConfig) : ReopenDecider(onlineConfig, false) {}

    ~MockReopenDecider() {}

public:
    MOCK_METHOD(ReopenType, GetReopenType, (), (const, override));
    MOCK_METHOD(index_base::Version, GetReopenIncVersion, (), (const, override));
};

DEFINE_SHARED_PTR(MockReopenDecider);
}} // namespace indexlib::partition

#endif //__INDEXLIB_MOCK_REOPEN_DECIDER_H
