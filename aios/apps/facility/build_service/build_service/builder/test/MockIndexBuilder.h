#pragma once

#include "build_service/test/unittest.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/QuotaControl.h"

using testing::ReturnRef;

namespace indexlib { namespace index {

class MockIndexBuilder : public indexlib::partition::IndexBuilder
{
public:
    MockIndexBuilder(const config::IndexPartitionOptions& options, const std::string& rootDir,
                     const config::IndexPartitionSchemaPtr& schema)
        : indexlib::partition::IndexBuilder(rootDir, options, schema,
                                            util::QuotaControlPtr(new indexlib::util::QuotaControl(1024 * 1024 * 1024)),
                                            partition::BuilderBranchHinter::Option::Test())
        , _counterMap(new util::CounterMap())
    {
        ON_CALL(*this, GetCounterMap()).WillByDefault(ReturnRef(_counterMap));
    }
    MockIndexBuilder(const indexlib::partition::IndexPartitionPtr& indexPartition)
        : indexlib::partition::IndexBuilder(indexPartition,
                                            util::QuotaControlPtr(new indexlib::util::QuotaControl(1024 * 1024 * 1024)))
        , _counterMap(new indexlib::util::CounterMap())
    {
        ON_CALL(*this, GetCounterMap()).WillByDefault(ReturnRef(_counterMap));
    }
    ~MockIndexBuilder() {}

public:
    MOCK_METHOD1(Build, bool(const document::DocumentPtr&));
    MOCK_METHOD0(GetCounterMap, const indexlib::util::CounterMapPtr&());

private:
    indexlib::util::CounterMapPtr _counterMap;
};

typedef ::testing::NiceMock<MockIndexBuilder> NiceMockIndexBuilder;
typedef ::testing::StrictMock<MockIndexBuilder> StrictMockIndexBuilder;

DEFINE_SHARED_PTR(MockIndexBuilder);
}} // namespace indexlib::index
