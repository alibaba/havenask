#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/data/ColumnData.h>
#include <ha3/sql/ops/test/OpTestBase.h>

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class ColumnDataTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
public:
    template <typename T>
    ColumnData<T>* createColumnData(string name, vector<T> data) {
        size_t rowCount = data.size();
        _docs = getMatchDocs(_allocator, rowCount);
        IF_FAILURE_RET_NULL(extendMatchDocAllocator<T>(_allocator, _docs, name, data));
        return new ColumnData<T>(_allocator->findReference<T>(name), &_docs);
    }
private:
    MatchDocAllocatorPtr _allocator;
    vector<matchdoc::MatchDoc> _docs;
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, ColumnDataTest);

void ColumnDataTest::setUp() {
}

void ColumnDataTest::tearDown() {
    _docs.clear();
    _allocator.reset();
}

TEST_F(ColumnDataTest, testSimple) {
    auto columnData = createColumnData<uint32_t>("field1", {1, 2, 3, 4});
    ASSERT_EQ(4 , columnData->getRowCount());
    ASSERT_EQ(1 , columnData->get(0));
    ASSERT_EQ(2 , columnData->get(1));
    ASSERT_EQ(3 , columnData->get(2));
    ASSERT_EQ(4 , columnData->get(3));
    DELETE_AND_SET_NULL(columnData);
}

TEST_F(ColumnDataTest, testSetGet) {
    auto columnData = createColumnData<uint32_t>("field1", {1, 2, 3, 4});
    ASSERT_EQ(4 , columnData->getRowCount());
    columnData->set(0, 100);
    ASSERT_EQ(2 , columnData->get(1));
    ASSERT_EQ(100 , columnData->get(0));
    ASSERT_EQ(4 , columnData->getRowCount());
    DELETE_AND_SET_NULL(columnData);
}

END_HA3_NAMESPACE(sql);
