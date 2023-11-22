#include "sql/ops/util/KernelUtil.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "navi/resource/MemoryPoolR.h"
#include "sql/data/TableData.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

namespace navi {
class Data;
} // namespace navi

using namespace std;
using namespace matchdoc;
using namespace table;

namespace sql {

class KernelUtilTest : public TESTBASE {
public:
    KernelUtilTest();
    ~KernelUtilTest();

public:
    void setUp() override {}
    void tearDown() override {}
};

KernelUtilTest::KernelUtilTest() {}

KernelUtilTest::~KernelUtilTest() {}

TEST_F(KernelUtilTest, testStripName) {
    map<string, vector<string>> value = {{"$name", {"$a", "$b"}}};
    KernelUtil::stripName(value);
    const auto &it = value.find("name");
    ASSERT_TRUE(it != value.end());
    ASSERT_EQ(2, it->second.size());
    ASSERT_EQ("a", it->second[0]);
    ASSERT_EQ("b", it->second[1]);
}

TEST_F(KernelUtilTest, testGetTable) {
    auto poolPtr = std::make_shared<autil::mem_pool::Pool>();
    table::MatchDocUtil matchDocUtil(poolPtr);
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> docs = matchDocUtil.createMatchDocs(allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, docs, "id", {1, 2}));
    table::TablePtr table = table::Table::fromMatchDocs(docs, allocator);
    auto data = std::dynamic_pointer_cast<navi::Data>(TableDataPtr(new TableData(table)));
    auto tableGet = KernelUtil::getTable(data, nullptr);
    ASSERT_EQ(table, tableGet);
}

TEST_F(KernelUtilTest, testGetTableCopy) {
    auto poolPtr = std::make_shared<autil::mem_pool::Pool>();
    table::MatchDocUtil matchDocUtil(poolPtr);
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> docs = matchDocUtil.createMatchDocs(allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, docs, "id", {1, 2}));
    table::TablePtr table = table::Table::fromMatchDocs(docs, allocator);
    auto data = std::dynamic_pointer_cast<navi::Data>(TableDataPtr(new TableData(table)));
    navi::MemoryPoolR memPoolR;
    auto graphMemPoolR = std::make_shared<navi::GraphMemoryPoolR>();
    graphMemPoolR->_memoryPoolR = &memPoolR;
    auto tableGet = KernelUtil::getTable(data, graphMemPoolR.get(), true);
    ASSERT_NE(table, tableGet);
    ASSERT_EQ(TableUtil::toString(tableGet), TableUtil::toString(table));
}

TEST_F(KernelUtilTest, testStripKernelNamePrefix) {
    {
        std::string kernelName = "sql.ScanKernel";
        KernelUtil::stripKernelNamePrefix(kernelName);
        ASSERT_EQ("ScanKernel", kernelName);
    }
    {
        std::string kernelName = "ss.ScanKernel";
        KernelUtil::stripKernelNamePrefix(kernelName);
        ASSERT_EQ("ss.ScanKernel", kernelName);
    }
}

} // namespace sql
