#include "suez/deploy/DeployFiles.h"

#include "unittest/unittest.h"

using namespace std;

namespace suez {

class DeployFilesTest : public TESTBASE {};

TEST_F(DeployFilesTest, testEqualAndHash) {
    auto base = DeployFiles{"path1", "path2", {"a"}};
    auto test1 = DeployFiles{"path1", "path2", {"a"}};
    auto test2 = DeployFiles{"path1", "path2", {}};
    auto test3 = DeployFiles{"path1", "", {"a"}};
    auto test4 = DeployFiles{"", "path2", {"a"}};
    EXPECT_EQ(base, test1);
    EXPECT_NE(base, test2);
    EXPECT_NE(base, test3);
    EXPECT_NE(base, test4);

    auto h = std::hash<DeployFiles>();
    EXPECT_EQ(h(base), h(test1));
    EXPECT_NE(h(base), h(test2));
    EXPECT_NE(h(base), h(test3));
    EXPECT_NE(h(base), h(test4));
}

} // namespace suez
