#include "indexlib/framework/mock/MockTablet.h"
#include "indexlib/framework/mock/MockTabletReader.h"
#include "indexlib/framework/mock/MockTabletSchema.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "suez/common/test/TableMetaUtil.h"
#include "suez/sdk/IndexProvider.h"
#include "suez/sdk/SuezTabletPartitionData.h"
#include "suez/service/TableServiceImpl.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using indexlib::config::IndexPartitionOptions;
using indexlib::partition::IndexPartitionPtr;
using indexlib::testlib::IndexlibPartitionCreator;

namespace suez {

class TableServiceDdlTest : public TESTBASE {};

TEST_F(TableServiceDdlTest, getSchemaVersionTest) {
    PartitionId pid1 = TableMetaUtil::makePidFromStr("t1_1");
    pid1.setPartitionIndex(1);

    auto tablet = std::make_shared<indexlibv2::framework::NiceMockTablet>();
    auto mockTablet = tablet.get();
    auto data1 = std::make_shared<SuezTabletPartitionData>(pid1, 1, tablet, false);
    auto singleReader1 = std::make_shared<SingleTableReader>(data1);
    MultiTableReader mutliTableReader1;
    mutliTableReader1.addTableReader(pid1, singleReader1);
    IndexProvider indexProvider;
    indexProvider.multiTableReader = mutliTableReader1;

    TableServiceImpl impl;
    GetSchemaVersionRequest request;
    GetSchemaVersionResponse response;
    impl.getSchemaVersion(nullptr, &request, &response, nullptr);
    ASSERT_TRUE(response.has_errorinfo());
    ASSERT_EQ(TBS_ERROR_SERVICE_NOT_READY, response.errorinfo().errorcode());

    impl.setIndexProvider(std::make_shared<IndexProvider>(indexProvider));
    request.mutable_partids()->Add(0);
    request.set_tablename("t1");

    auto tabletReader = std::make_shared<indexlibv2::framework::MockTabletReader>();
    auto mockTabletReader = tabletReader.get();
    auto schema = std::make_shared<indexlibv2::framework::MockTabletSchema>();
    EXPECT_CALL(*mockTablet, GetTabletReader())
        .WillOnce(Return(nullptr))
        .WillOnce(Return(tabletReader))
        .WillOnce(Return(tabletReader));

    EXPECT_CALL(*mockTabletReader, GetSchema()).WillOnce(Return(nullptr)).WillOnce(Return(schema));

    GetSchemaVersionResponse response2;
    impl.getSchemaVersion(nullptr, &request, &response2, nullptr);
    ASSERT_EQ(TBS_ERROR_OTHERS, response2.errorinfo().errorcode());

    GetSchemaVersionResponse response3;
    impl.getSchemaVersion(nullptr, &request, &response3, nullptr);
    ASSERT_EQ(TBS_ERROR_OTHERS, response3.errorinfo().errorcode());

    GetSchemaVersionResponse response4;
    impl.getSchemaVersion(nullptr, &request, &response4, nullptr);
    ASSERT_EQ(TBS_ERROR_NONE, response4.errorinfo().errorcode()) << "---errror: " << response4.errorinfo().errormsg();
    ASSERT_FALSE(response4.schemaversions().empty());
    ASSERT_EQ(0, response4.schemaversions(0).partid());
    ASSERT_EQ(0, response4.schemaversions(0).currentversionid());
}

} // namespace suez
