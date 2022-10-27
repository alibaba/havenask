#pragma once

#include <ha3/search/NormalAggregator.h>
#include "ha3/search/test/AggregateBase.h"

BEGIN_HA3_NAMESPACE(search);
class MatchDocAllocator;
class AggregateFunction;
enum AggMode { AGG_NORMAL, AGG_BATCH };

class NormalAggregatorTest : public testing::TestWithParam< AggMode >,
                             public AggregateBase
{
public:
    NormalAggregatorTest();
    ~NormalAggregatorTest();
public:
    void SetUp() override;
    void TearDown() override;
protected:

    template <typename T, typename T_KEY>
    void checkResult_RT(NormalAggregator<T_KEY>* agg,
                        const std::string &funName, T v1, T v2);
protected:
    AggMode _mode;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);
