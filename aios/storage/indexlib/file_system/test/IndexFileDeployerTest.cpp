#include "indexlib/file_system/IndexFileDeployer.h"

#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class IndexFileDeployerTest : public INDEXLIB_TESTBASE
{
public:
    IndexFileDeployerTest();
    ~IndexFileDeployerTest();

    DECLARE_CLASS_NAME(IndexFileDeployerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestExtractFenceName();

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, IndexFileDeployerTest);

INDEXLIB_UNIT_TEST_CASE(IndexFileDeployerTest, TestExtractFenceName);

//////////////////////////////////////////////////////////////////////

IndexFileDeployerTest::IndexFileDeployerTest() {}

IndexFileDeployerTest::~IndexFileDeployerTest() {}

void IndexFileDeployerTest::CaseSetUp() {}

void IndexFileDeployerTest::CaseTearDown() {}

void IndexFileDeployerTest::TestExtractFenceName() {}

}} // namespace indexlib::file_system
