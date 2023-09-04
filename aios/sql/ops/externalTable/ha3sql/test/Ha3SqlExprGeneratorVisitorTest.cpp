#include "sql/ops/externalTable/ha3sql/Ha3SqlExprGeneratorVisitor.h"

#include <iosfwd>
#include <string>

#include "navi/common.h"
#include "unittest/unittest.h"

using namespace std;
using namespace navi;
using namespace testing;

namespace sql {

class Ha3SqlExprGeneratorVisitorTest : public TESTBASE {
public:
    Ha3SqlExprGeneratorVisitorTest() {}
    ~Ha3SqlExprGeneratorVisitorTest() {}
};

TEST_F(Ha3SqlExprGeneratorVisitorTest, testWrapBacktick) {
    std::string name = "`sell``er_id`";
    Ha3SqlExprGeneratorVisitor::wrapBacktick(name);
    ASSERT_EQ("```sell````er_id```", name);
}

TEST_F(Ha3SqlExprGeneratorVisitorTest, testGetItemFullName) {
    ASSERT_EQ("```tags```['hos''t']",
              Ha3SqlExprGeneratorVisitor::getItemFullName("$`tags`", "hos't"));
}

} // namespace sql
