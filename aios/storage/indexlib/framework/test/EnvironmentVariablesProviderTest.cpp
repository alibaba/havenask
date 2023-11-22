#include "indexlib/framework/EnvironmentVariablesProvider.h"

#include <autil/EnvUtil.h>
#include <autil/StringUtil.h>
#include <autil/Thread.h>

#include "unittest/unittest.h"

namespace indexlib::framework::test {

class EnvironmentVariablesProviderTest : public TESTBASE
{
public:
    EnvironmentVariablesProviderTest() = default;
    ~EnvironmentVariablesProviderTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void EnvironmentVariablesProviderTest::setUp() {}

void EnvironmentVariablesProviderTest::tearDown() {}

TEST_F(EnvironmentVariablesProviderTest, testUsage)
{
    {
        autil::EnvGuard _v1("key1", "1000");
        autil::EnvGuard _v2("key2", "2000");
        auto envProvider = std::make_unique<indexlibv2::framework::EnvironmentVariablesProvider>();
        envProvider->Init({{"key1", "1001"}, {"key3", "3001"}});

        EXPECT_EQ(std::string("1000"), envProvider->Get("key1", "some-value"));
        EXPECT_EQ(std::string("2000"), envProvider->Get("key2", "some-value"));
        EXPECT_EQ(std::string("3001"), envProvider->Get("key3", "some-value"));
        EXPECT_EQ(std::string("value4"), envProvider->Get("key4", "value4"));
    }
}

TEST_F(EnvironmentVariablesProviderTest, testMultiThread)
{
    {
        autil::EnvGuard _v1("key1", "1000");
        autil::EnvGuard _v2("key2", "2000");
        autil::EnvGuard _v3("key3", "3000");
        auto envProvider = std::make_unique<indexlibv2::framework::EnvironmentVariablesProvider>();
        envProvider->Init({{"key1", "1001"}, {"key3", "3001"}, {"key4", "4001"}});

        int globalCount = 5000;
        autil::ThreadPtr t1 = autil::Thread::createThread(
            [globalCount]() {
                for (int i = 0; i < globalCount; i++) {
                    autil::EnvUtil::setEnv("my_" + autil::StringUtil::toString(i), autil::StringUtil::toString(i));
                }
            },
            "set");
        autil::ThreadPtr t2 = autil::Thread::createThread(
            [globalCount, &envProvider]() {
                for (int i = 0; i < globalCount; i++) {
                    envProvider->Get("my_" + autil::StringUtil::toString(i), autil::StringUtil::toString(i));
                    EXPECT_EQ(std::string("1000"), envProvider->Get("key1", "some-value"));
                    EXPECT_EQ(std::string("2000"), envProvider->Get("key2", "some-value"));
                    EXPECT_EQ(std::string("3000"), envProvider->Get("key3", "some-value"));
                    EXPECT_EQ(std::string("4001"), envProvider->Get("key4", "some-value"));
                }
            },
            "get");
        t1->join();
        for (int i = 0; i < globalCount; i++) {
            ASSERT_EQ(autil::StringUtil::toString(i),
                      envProvider->Get("my_" + autil::StringUtil::toString(i), "some-value"));
        }
    }
}

} // namespace indexlib::framework::test
