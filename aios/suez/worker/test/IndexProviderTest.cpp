#include "suez/sdk/IndexProvider.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <set>
#include <string>

#include "indexlib/framework/Tablet.h"
#include "indexlib/partition/offline_partition.h"
#include "suez/common/InnerDef.h"
#include "suez/common/TableMeta.h"
#include "suez/sdk/TableReader.h"
#include "suez/table/SuezPartition.h"
#include "suez/table/SuezPartitionFactory.h"
#include "suez/table/test/MockSuezPartition.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace indexlib::partition;
using namespace indexlibv2::framework;

namespace suez {

class IndexProviderTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    template <typename PD, typename R, typename T>
    void AddReader(const PartitionId &pid, R &reader, MultiTableReader &multiReader, T *partition);

protected:
    PartitionId _pid[6];
    IndexProvider _provider1;
    IndexProvider _provider2;
    MultiTableReader _multiReader1;
    MultiTableReader _multiReader2;

    TableReader _tableReader1;
    TableReader _tableReader2;
    TabletReader _tabletReader1;
    TabletReader _tabletReader2;
};

template <typename PD, typename R, typename T>
void IndexProviderTest::AddReader(const PartitionId &pid, R &reader, MultiTableReader &multiReader, T *partition) {
    auto data = std::make_shared<PD>(pid, 1, nullptr, true);
    auto singleReader = std::make_shared<SingleTableReader>(data);
    multiReader.addTableReader(pid, singleReader);
    reader[pid].reset(partition);
}

void IndexProviderTest::setUp() {
    for (size_t i = 0; i < 6; ++i) {
        _pid[i].tableId.fullVersion = i;
        _pid[i].tableId.tableName = "table" + to_string(i);
        _pid[i].from = 0;
        _pid[i].to = 65535;
    }
    AddReader<SuezIndexPartitionData>(_pid[0], _tableReader1, _multiReader1, new OfflinePartition());
    AddReader<SuezIndexPartitionData>(_pid[1], _tableReader1, _multiReader1, new OfflinePartition());
    AddReader<SuezIndexPartitionData>(_pid[1], _tableReader2, _multiReader2, new OfflinePartition());
    AddReader<SuezIndexPartitionData>(_pid[2], _tableReader2, _multiReader2, new OfflinePartition());

    TabletResource resource;
    AddReader<SuezIndexPartitionData>(_pid[3], _tabletReader1, _multiReader1, new Tablet(resource));
    AddReader<SuezIndexPartitionData>(_pid[4], _tabletReader1, _multiReader1, new Tablet(resource));
    AddReader<SuezIndexPartitionData>(_pid[4], _tabletReader2, _multiReader2, new Tablet(resource));
    AddReader<SuezIndexPartitionData>(_pid[5], _tabletReader2, _multiReader2, new Tablet(resource));

    _provider1.tableReader = _tableReader1;
    _provider2.tableReader = _tableReader2;
    _provider1.tabletReader = _tabletReader1;
    _provider2.tabletReader = _tabletReader2;
    _provider1.multiTableReader = _multiReader1;
    _provider2.multiTableReader = _multiReader2;
}

void IndexProviderTest::tearDown() {}

TEST_F(IndexProviderTest, testMergeProvider) {
    _provider1.allTableReady = false;
    _provider2.allTableReady = true;
    ASSERT_TRUE(_provider1.mergeProvider(_provider2));
    ASSERT_EQ(3, _provider1.tableReader.size());
    ASSERT_EQ(2, _provider2.tableReader.size());
    ASSERT_TRUE(_multiReader1.getTableReader(_pid[1]) != _multiReader2.getTableReader(_pid[1]));
    ASSERT_EQ(_provider1.tableReader[_pid[0]], _tableReader1[_pid[0]]);
    ASSERT_EQ(_provider1.tableReader[_pid[1]], _tableReader2[_pid[1]]);
    ASSERT_EQ(_provider1.tableReader[_pid[2]], _tableReader2[_pid[2]]);
    ASSERT_EQ(_provider1.tabletReader[_pid[3]], _tabletReader1[_pid[3]]);
    ASSERT_EQ(_provider1.tabletReader[_pid[4]], _tabletReader2[_pid[4]]);
    ASSERT_EQ(_provider1.tabletReader[_pid[5]], _tabletReader2[_pid[5]]);
    ASSERT_EQ(_provider1.multiTableReader.getTableReader(_pid[0]), _multiReader1.getTableReader(_pid[0]));
    ASSERT_EQ(_provider1.multiTableReader.getTableReader(_pid[1]), _multiReader2.getTableReader(_pid[1]));
    ASSERT_EQ(_provider1.multiTableReader.getTableReader(_pid[2]), _multiReader2.getTableReader(_pid[2]));
    ASSERT_EQ(_provider1.multiTableReader.getTableReader(_pid[3]), _multiReader1.getTableReader(_pid[3]));
    ASSERT_EQ(_provider1.multiTableReader.getTableReader(_pid[4]), _multiReader2.getTableReader(_pid[4]));
    ASSERT_EQ(_provider1.multiTableReader.getTableReader(_pid[5]), _multiReader2.getTableReader(_pid[5]));
    ASSERT_TRUE(_provider1.allTableReady);
}

TEST_F(IndexProviderTest, testGenErasePartitionIdSet) {
    set<PartitionId> pids = _provider1.genErasePartitionIdSet(_provider2);
    ASSERT_EQ(2, pids.size());
    ASSERT_TRUE(pids.find(_pid[0]) != pids.end());
    ASSERT_TRUE(pids.find(_pid[3]) != pids.end());
}

TEST_F(IndexProviderTest, testErase) {
    _provider1.erase(_pid[0]);
    _multiReader1.erase(_pid[0]); // make sure erase all ref to pid1's single table reader
    ASSERT_EQ(1, _provider1.tableReader.size());
    ASSERT_EQ(_provider1.tableReader[_pid[1]], _tableReader1[_pid[1]]);
    ASSERT_EQ(_provider1.multiTableReader.getTableReader(_pid[1]), _multiReader1.getTableReader(_pid[1]));
    _provider1.erase(_pid[1]);
    ASSERT_EQ(0, _provider1.tableReader.size());
    ASSERT_EQ(2, _provider1.multiTableReader._tableReaders.size());

    _provider1.erase(_pid[3]);
    _multiReader1.erase(_pid[3]); // make sure erase all ref to pid1's single table reader
    ASSERT_EQ(1, _provider1.tabletReader.size());
    ASSERT_EQ(_provider1.tabletReader[_pid[4]], _tabletReader1[_pid[4]]);
    ASSERT_EQ(_provider1.multiTableReader.getTableReader(_pid[4]), _multiReader1.getTableReader(_pid[4]));
    _provider1.erase(_pid[4]);
    ASSERT_EQ(0, _provider1.tabletReader.size());
    ASSERT_EQ(0, _provider1.multiTableReader._tableReaders.size());
}

} // namespace suez
