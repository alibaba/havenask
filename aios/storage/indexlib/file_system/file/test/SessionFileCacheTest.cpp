#include "indexlib/file_system/file/SessionFileCache.h"

#include "gtest/gtest.h"
#include <iosfwd>
#include <pthread.h>
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class SessionFileCacheTest : public INDEXLIB_TESTBASE
{
public:
    SessionFileCacheTest();
    ~SessionFileCacheTest();

    DECLARE_CLASS_NAME(SessionFileCacheTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, SessionFileCacheTest);

INDEXLIB_UNIT_TEST_CASE(SessionFileCacheTest, TestSimpleProcess);

//////////////////////////////////////////////////////////////////////

class FileCacheGuard
{
public:
    FileCacheGuard(SessionFileCache* owner, const std::string& filePath, ssize_t fileLength = -1) noexcept(false)
        : _owner(owner)
    {
        // assert filePath is not a logical path (in bazel ut physical path may start with '.')
        assert(!filePath.empty());
        assert(filePath.find("://") != std::string::npos || filePath[0] == '.' || filePath[0] == '/');
        if (_owner) {
            _filePath = filePath;
            _object = _owner->Get(filePath, fileLength).GetOrThrow(filePath);
        }
    }

    virtual ~FileCacheGuard()
    {
        if (_object != NULL) {
            _owner->Put(_object, _filePath);
        }
    }

public:
    fslib::fs::File* Get() const { return _object; }
    fslib::fs::File* Steal()
    {
        fslib::fs::File* obj = _object;
        _object = nullptr;
        return obj;
    }

private:
    SessionFileCache* _owner;
    fslib::fs::File* _object;
    std::string _filePath;
};

SessionFileCacheTest::SessionFileCacheTest() {}

SessionFileCacheTest::~SessionFileCacheTest() {}

void SessionFileCacheTest::CaseSetUp() {}

void SessionFileCacheTest::CaseTearDown() {}

void SessionFileCacheTest::TestSimpleProcess()
{
    string rawFilePath = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "test");
    ASSERT_EQ(FSEC_OK, FslibWrapper::Store(rawFilePath, "").Code());
    string filePath = "LOCAL://" + rawFilePath;

    SessionFileCache cache;
    {
        FileCacheGuard guard(&cache, filePath, -1);
        ASSERT_EQ(0, cache.GetSessionFileCount(pthread_self(), filePath));
    }
    ASSERT_EQ(1, cache.GetSessionFileCount(pthread_self(), filePath));
    {
        FileCacheGuard guard(&cache, filePath, -1);
        ASSERT_EQ(0, cache.GetSessionFileCount(pthread_self(), filePath));
        FileCacheGuard guard1(&cache, filePath, -1);
        ASSERT_EQ(0, cache.GetSessionFileCount(pthread_self(), filePath));
    }
    ASSERT_EQ(2, cache.GetSessionFileCount(pthread_self(), filePath));
    {
        FileCacheGuard guard(&cache, filePath, -1);
        ASSERT_EQ(1, cache.GetSessionFileCount(pthread_self(), filePath));
        FileCacheGuard guard1(&cache, filePath, -1);
        ASSERT_EQ(0, cache.GetSessionFileCount(pthread_self(), filePath));
    }
}
}} // namespace indexlib::file_system
