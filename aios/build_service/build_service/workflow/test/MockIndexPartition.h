#ifndef __INDEXLIB_MOCK_INDEX_PARTITION_H
#define __INDEXLIB_MOCK_INDEX_PARTITION_H

#include <tr1/memory>
#include <indexlib/partition/index_partition.h>

IE_NAMESPACE_BEGIN(index);

class MockIndexPartition : public IE_NAMESPACE(partition)::IndexPartition
{
public:
    MockIndexPartition() {
        ON_CALL(*this, CheckMemoryStatus()).WillByDefault(
                Invoke(std::tr1::bind(&MockIndexPartition::CheckMemoryStatusForTest, this)));
    }

    MOCK_METHOD4(Open, OpenStatus(const std::string& dir, 
                                  const config::IndexPartitionSchemaPtr& schema, 
                                  const config::IndexPartitionOptions& options,
                                  versionid_t versionId));

    MOCK_METHOD2(ReOpen, OpenStatus(bool forceReopen, versionid_t reopenVersionId));
    MOCK_METHOD0(ReOpenNewSegment, void());
    MOCK_METHOD0(Close, void());
    MOCK_METHOD2(AddVirtualAttributes, void(const config::AttributeConfigVector&,
                                            const config::AttributeConfigVector&));
    MOCK_CONST_METHOD0(GetWriter, IE_NAMESPACE(partition)::PartitionWriterPtr());
    MOCK_CONST_METHOD0(GetReader, IE_NAMESPACE(partition)::IndexPartitionReaderPtr());
    MOCK_CONST_METHOD0(CheckMemoryStatus, MemoryStatus());

private:
    MemoryStatus CheckMemoryStatusForTest() const {
        return MS_OK;
    }
};

typedef ::testing::NiceMock<MockIndexPartition> NiceMockIndexPartition;
typedef ::testing::StrictMock<MockIndexPartition> StrictMockIndexPartition;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MOCK_INDEX_PARTITION_H
