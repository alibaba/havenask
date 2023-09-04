#include "sql/ops/scan/DocIdsScanIterator.h"

#include <cstddef>
#include <memory>
#include <vector>

#include "autil/result/Result.h"
#include "ha3/common/DocIdsQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/Term.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "navi/util/NaviTestPool.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace isearch::common;

namespace sql {

class DocIdsScanIteratorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
};

void DocIdsScanIteratorTest::setUp() {}

void DocIdsScanIteratorTest::tearDown() {}

TEST_F(DocIdsScanIteratorTest, testSimple) {
    autil::mem_pool::PoolAsan pool;
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    std::vector<docid_t> docIds = {10, 5, 8};
    QueryPtr query(new DocIdsQuery(docIds));
    DocIdsScanIterator iter(allocator, nullptr, query);
    std::vector<matchdoc::MatchDoc> docs;
    ASSERT_TRUE(iter.batchSeek(0, docs).unwrap());
    ASSERT_EQ(3, docs.size());
    for (size_t i = 0; i < docs.size(); ++i) {
        ASSERT_EQ(docIds[i], docs[i].getDocId());
    }
    ASSERT_EQ(3, iter.getTotalScanCount());
}

} // namespace sql
