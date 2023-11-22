#include "table/ColumnData.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace matchdoc;

namespace table {

class ColumnDataTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

public:
    template <typename T>
    std::unique_ptr<ColumnData<T>> createColumnData(const string &name, const vector<T> &data) {
        _docs = _matchDocUtil.createMatchDocs(_allocator, data.size());
        _matchDocUtil.extendMatchDocAllocator<T>(_allocator, _docs, name, data);
        auto colData = ColumnData<T>::fromReference(_allocator->findReference<T>(name));
        colData->attachRows(&_docs);
        return colData;
    }

private:
    MatchDocUtil _matchDocUtil;
    MatchDocAllocatorPtr _allocator;
    vector<matchdoc::MatchDoc> _docs;
};

void ColumnDataTest::setUp() {}

void ColumnDataTest::tearDown() {
    _docs.clear();
    _allocator.reset();
}

TEST_F(ColumnDataTest, testSimple) {
    auto columnData = createColumnData<uint32_t>("field1", {1, 2, 3, 4});
    ASSERT_EQ(4, columnData->getRowCount());
    ASSERT_EQ(1, columnData->get(0));
    ASSERT_EQ(2, columnData->get(1));
    ASSERT_EQ(3, columnData->get(2));
    ASSERT_EQ(4, columnData->get(3));
}

TEST_F(ColumnDataTest, testSetGet) {
    auto columnData = createColumnData<uint32_t>("field1", {1, 2, 3, 4});
    ASSERT_EQ(4, columnData->getRowCount());
    columnData->set(0, 100);
    ASSERT_EQ(2, columnData->get(1));
    ASSERT_EQ(100, columnData->get(0));
    ASSERT_EQ(4, columnData->getRowCount());
}

} // namespace table
