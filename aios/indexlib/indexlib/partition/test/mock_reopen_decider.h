#ifndef __INDEXLIB_MOCK_REOPEN_DECIDER_H
#define __INDEXLIB_MOCK_REOPEN_DECIDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/reopen_decider.h"

IE_NAMESPACE_BEGIN(partition);

class MockReopenDecider : public ReopenDecider
{
public:
    MockReopenDecider(const config::OnlineConfig& onlineConfig)
        : ReopenDecider(onlineConfig, false)
    {}

    ~MockReopenDecider() {}

public:
    MOCK_CONST_METHOD0(GetReopenType, ReopenType());
    MOCK_CONST_METHOD0(GetReopenIncVersion, index_base::Version());
};

DEFINE_SHARED_PTR(MockReopenDecider);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MOCK_REOPEN_DECIDER_H
