#ifndef __INDEXLIB_MOCK_INDEX_BUILDER_H
#define __INDEXLIB_MOCK_INDEX_BUILDER_H

#include <indexlib/partition/index_builder.h>
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/util/memory_control/quota_control.h>
#include "build_service/test/unittest.h"

using testing::ReturnRef;

IE_NAMESPACE_BEGIN(index);

class MockIndexBuilder : public IE_NAMESPACE(partition)::IndexBuilder {
public:
    MockIndexBuilder(const config::IndexPartitionOptions& options, const std::string& rootDir,
                     const config::IndexPartitionSchemaPtr& schema)
        : IE_NAMESPACE(partition)::IndexBuilder(
            rootDir, options, schema, 
            util::QuotaControlPtr(new IE_NAMESPACE(util)::QuotaControl(1024*1024*1024)))
        , _counterMap(new util::CounterMap())
    {
        ON_CALL(*this, GetCounterMap()).WillByDefault(ReturnRef(_counterMap));
    }
MockIndexBuilder(const IE_NAMESPACE(partition)::IndexPartitionPtr& indexPartition)
        : IE_NAMESPACE(partition)::IndexBuilder(
            indexPartition,
            util::QuotaControlPtr(new IE_NAMESPACE(util)::QuotaControl(1024*1024*1024)))
        , _counterMap(new IE_NAMESPACE(util)::CounterMap())
    {
        ON_CALL(*this, GetCounterMap()).WillByDefault(ReturnRef(_counterMap)); 
    }
    ~MockIndexBuilder() {
    }
public:
    MOCK_METHOD1(Build, bool(const document::DocumentPtr &));
    MOCK_METHOD0(GetCounterMap, const IE_NAMESPACE(util)::CounterMapPtr&());
private:
    IE_NAMESPACE(util)::CounterMapPtr _counterMap;
};

typedef ::testing::NiceMock<MockIndexBuilder> NiceMockIndexBuilder;
typedef ::testing::StrictMock<MockIndexBuilder> StrictMockIndexBuilder;

DEFINE_SHARED_PTR(MockIndexBuilder);
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MOCK_INDEX_BUILDER_H
