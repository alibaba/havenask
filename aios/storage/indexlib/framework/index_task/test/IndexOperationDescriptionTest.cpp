#include "indexlib/framework/index_task/IndexOperationDescription.h"

#include <string>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class IndexOperationDescriptionTest : public TESTBASE
{
public:
    IndexOperationDescriptionTest() {}
    ~IndexOperationDescriptionTest() {}

    void setUp() override {}
    void tearDown() override {}
};

TEST_F(IndexOperationDescriptionTest, testSimple)
{
    IndexOperationDescription desc(/*id*/ 1, /*type*/ "test");
    desc.AddParameter("a", "1");
    desc.AddDepend(0);
    std::string value;
    ASSERT_TRUE(desc.GetParameter("a", value));
    ASSERT_EQ("1", value);
    ASSERT_TRUE(desc.IsDependOn(0));
    std::string content = autil::legacy::ToJsonString(desc);

    IndexOperationDescription desc1;
    autil::legacy::FromJsonString(desc1, content);
    ASSERT_EQ(desc, desc1);
}

} // namespace indexlibv2::framework
