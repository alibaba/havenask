#include "indexlib/base/NoExceptionWrapper.h"

#include "autil/Log.h"
#include "unittest/unittest.h"

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.base, NoExceptionWrapperTest);

namespace indexlibv2 {

class NoExceptionWrapperTest : public TESTBASE
{
};

std::unique_ptr<int> newInt(int x) { return std::make_unique<int>(x); }

std::unique_ptr<int> newIntIOError(int x)
{
    INDEXLIB_FATAL_ERROR(FileIO, "fileio error");
    return std::make_unique<int>(x);
}

void foo(int x, int& y) { y = x; }
void fooIOError() { INDEXLIB_FATAL_ERROR(FileIO, "fileio error"); }

TEST_F(NoExceptionWrapperTest, testNormal)
{
    int x = 9;
    auto [st, ret] = NoExceptionWrapper::Invoke(newInt, x);
    EXPECT_TRUE(st.IsOK());
    EXPECT_EQ(9, *ret);

    int y = 0;
    auto st2 = NoExceptionWrapper::Invoke(foo, x, y);
    EXPECT_TRUE(st2.IsOK());
    EXPECT_EQ(9, y);
}

TEST_F(NoExceptionWrapperTest, testException)
{
    int x = 9;
    auto [st, ret] = NoExceptionWrapper::Invoke(newIntIOError, x);
    EXPECT_TRUE(st.IsIOError());
    EXPECT_FALSE(st.ToString().empty());
    EXPECT_FALSE(ret);

    auto st2 = NoExceptionWrapper::Invoke(fooIOError);
    EXPECT_TRUE(st2.IsIOError());
    EXPECT_FALSE(st2.ToString().empty());
}

class TestBase
{
public:
    virtual std::unique_ptr<int> newInt(int x, int& y) { return std::make_unique<int>(-1); }
    virtual std::unique_ptr<int> newIntIOError(int x) { return std::make_unique<int>(-1); }
    virtual void foo(int x, int& y) {}
    virtual void fooIoError() {}
    virtual ~TestBase() {}
};

class TestDerived : public TestBase
{
public:
    std::unique_ptr<int> newInt(int x, int& y) override
    {
        y = x;
        return std::make_unique<int>(x);
    }
    std::unique_ptr<int> newIntIOError(int x) override
    {
        INDEXLIB_FATAL_ERROR(FileIO, "fileio error");
        return std::make_unique<int>(x);
    }
    void foo(int x, int& y) override { y = x; }
    void fooIoError() override { INDEXLIB_FATAL_ERROR(FileIO, "fileio error"); }
};

TEST_F(NoExceptionWrapperTest, testNormalMethod)
{
    TestBase* a = new TestDerived();
    std::unique_ptr<TestBase> guard(a);
    int x = 9;
    int y = 0;
    auto [st, ret] = NoExceptionWrapper::Invoke(&TestBase::newInt, a, x, y);
    EXPECT_TRUE(st.IsOK());
    EXPECT_EQ(9, *ret);
    EXPECT_EQ(9, y);

    int x2 = 100;
    int y2 = -1;
    auto st2 = NoExceptionWrapper::Invoke(&TestBase::foo, a, x2, y2);
    EXPECT_TRUE(st2.IsOK());
    EXPECT_EQ(100, y2);
}

TEST_F(NoExceptionWrapperTest, testExceptionMethod)
{
    TestBase* a = new TestDerived();
    std::unique_ptr<TestBase> guard(a);
    auto [st, ret] = NoExceptionWrapper::Invoke(&TestBase::newIntIOError, a, 9);
    EXPECT_TRUE(st.IsIOError());
    EXPECT_FALSE(st.ToString().empty());
    EXPECT_FALSE(ret);

    auto st2 = NoExceptionWrapper::Invoke(&TestBase::fooIoError, a);
    EXPECT_TRUE(st2.IsIOError());
    EXPECT_FALSE(st2.ToString().empty());
}

} // namespace indexlibv2
