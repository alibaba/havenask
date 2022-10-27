#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/turing/qrs/QrsServiceSnapshot.h>
#include <ha3/common/VersionCalculator.h>

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(common);

class QrsServiceSnapshotTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, QrsServiceSnapshotTest);

void QrsServiceSnapshotTest::setUp() {
}

void QrsServiceSnapshotTest::tearDown() {
}

TEST_F(QrsServiceSnapshotTest, testCalcVersion) {
    QrsServiceSnapshot snapshot(false);
    auto biz = snapshot.doCreateBiz("bizName");
    snapshot._workerParam.workerConfigVersion = "workerConfigVersion";
    biz->_bizInfo._versionConfig._dataVersion = "dataVersion";
    biz->_bizMeta.setRemoteConfigPath("remote path");
    EXPECT_EQ(VersionCalculator::calcVersion("workerConfigVersion",
                                             "dataVersion", "remote path", ""),
              snapshot.calcVersion(biz.get()));
}

TEST_F(QrsServiceSnapshotTest, testGetFirstQrsBiz) {
    {
        QrsServiceSnapshot snapshot(false);
        QrsBizPtr qrsBiz = snapshot.getFirstQrsBiz();
        ASSERT_TRUE(qrsBiz == nullptr);
    }
    {
        QrsServiceSnapshot snapshot(false);
        auto bizA = new QrsBiz();
        bizA->_bizName = "a";
        snapshot._bizMap["a"].reset(bizA);
        QrsBizPtr qrsBiz = snapshot.getFirstQrsBiz();
        ASSERT_TRUE(qrsBiz != nullptr);
        ASSERT_EQ("a", qrsBiz->getBizName());
    }
    {
        QrsServiceSnapshot snapshot(false);
        auto bizA = new QrsSqlBiz();
        bizA->_bizName = "a";
        snapshot._bizMap["a"].reset(bizA);
        auto bizB = new QrsBiz();
        bizB->_bizName = "b";
        snapshot._bizMap["b"].reset(bizB);
        QrsBizPtr qrsBiz = snapshot.getFirstQrsBiz();
        ASSERT_TRUE(qrsBiz != nullptr);
        ASSERT_EQ("b", qrsBiz->getBizName());
    }

}

TEST_F(QrsServiceSnapshotTest, testGetFirstQrsSqlBiz) {
    {
        QrsServiceSnapshot snapshot(false);
        QrsSqlBizPtr qrsSqlBiz = snapshot.getFirstQrsSqlBiz();
        ASSERT_TRUE(qrsSqlBiz == nullptr);
    }
    {
        QrsServiceSnapshot snapshot(false);
        auto bizA = new QrsSqlBiz();
        bizA->_bizName = "a";
        snapshot._bizMap["a"].reset(bizA);
        QrsSqlBizPtr qrsBiz = snapshot.getFirstQrsSqlBiz();
        ASSERT_TRUE(qrsBiz != nullptr);
        ASSERT_EQ("a", qrsBiz->getBizName());
    }
    {
        QrsServiceSnapshot snapshot(false);
        auto bizA = new QrsBiz();
        bizA->_bizName = "a";
        snapshot._bizMap["a"].reset(bizA);
        auto bizB = new QrsSqlBiz();
        bizB->_bizName = "b";
        snapshot._bizMap["b"].reset(bizB);
        QrsSqlBizPtr qrsBiz = snapshot.getFirstQrsSqlBiz();
        ASSERT_TRUE(qrsBiz != nullptr);
        ASSERT_EQ("b", qrsBiz->getBizName());
    }
}

END_HA3_NAMESPACE(turing);
