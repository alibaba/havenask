#ifndef __SUEZ_UNITTEST_H
#define __SUEZ_UNITTEST_H

#include <dlfcn.h>
#include <execinfo.h>
#include <string>
#include <typeinfo>
#include <unistd.h>
#define GTEST_USE_OWN_TR1_TUPLE 0
#include "gmock/gmock.h" // IWYU pragma: export
#include "gtest/gtest.h" // IWYU pragma: export
#include <gmock/gmock-actions.h> // IWYU pragma: export
#include <gmock/gmock-spec-builders.h> // IWYU pragma: export
#include <gtest/gtest-message.h> // IWYU pragma: export
#include <gtest/gtest-test-part.h> // IWYU pragma: export
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/mman.h>

#define private public
#define protected public

#if defined(__clang__)
#define DISABLE_NON_CLANG(casename) casename
#else
#define DISABLE_NON_CLANG(casename) DISABLED_##casename
#endif

using testing::_;
using testing::DoAll;
using testing::Invoke;
using testing::Return;
using testing::SetArgReferee;
using testing::Throw;

namespace testing {
static const std::string SUEZ_BAZEL_PATH = "/external/suez";
static const std::string TF_SEARCH_BAZEL_PATH = "/external/tf_search";
static const std::string EXPRESSION_BAZEL_PATH = "/external/expression";
} // namespace testing

class TESTBASE_BASE
{
public:
    TESTBASE_BASE()
    {
        const char *tmp_path = getenv("TEST_TMPDIR");
        if (tmp_path == NULL) {
            _testTempRoot = "./";
        } else {
            _testTempRoot = std::string(tmp_path);
        }
        std::string currentPath;

        const char *src_path = getenv("TEST_SRCDIR");
        const char *workspace = getenv("TEST_WORKSPACE");

        if (tmp_path && workspace) {
            size_t pos = std::string(tmp_path).find(workspace);
            _workspacePath = std::string(tmp_path).substr(0, pos + strlen(workspace));
        }

        if (src_path && workspace) {
            currentPath = std::string(src_path) + "/" + std::string(workspace) + "/";
        } else {
            char cwdPath[PATH_MAX];
            auto unused = getcwd(cwdPath, PATH_MAX);
            (void)unused;
            currentPath = cwdPath;
            currentPath += "/";
        }

        const char *test_binary = getenv("TEST_BINARY");
        if (test_binary) {
            std::string test_binary_str = std::string(test_binary);
            assert(*test_binary_str.rbegin() != '/');
            size_t filePos = test_binary_str.rfind('/') + 1;
            _privateTestDataPath = currentPath + test_binary_str.substr(0, filePos) + "testdata/";
            _privateTestDataPath2 = currentPath + test_binary_str.substr(0, filePos) + "test/testdata/";
            if (!test_binary_str.empty() && test_binary_str[0] == '/') {
                if (_testTempRoot == "./") {
                    _testTempRoot = test_binary_str.substr(0, filePos);
                }
                _privateTestDataPath = test_binary_str.substr(0, filePos) + "testdata/";
                _privateTestDataPath2 = test_binary_str.substr(0, filePos) + "test/testdata/";
            }
            static const std::string AIOS_PREFIX = "aios/";
            static const std::string HA3_PLUGINS_PREFIX = "ha3_plugins/";
            static const std::string UNIFIED_PLUGINS_PREFIX = "pluginplatform/";
            // 迁移目录后的项目
            static const std::set<std::string> PROJECTS = {
                "swift/", "fslib/", "kiwi/", "build_service/",
                "build_service_tasks/", "basic_bs_plugins/",
                "sql_plugins/", "bs_plugins/", "analyzer_plugins/",
                "plugin_framework/", "ha3_plugins/", "hobbit_framework_ha3/",
                "config_fetcher/", "algo_models/", "flib_for_magician/",
                "global_feature_provider/", "magician_common/", "magician_ha_plugin/",
                "formula/", "magician_feature_lib/", "freeschema/", "cava_feature_lib/",
                "magician_analyzer/", "vulcan/"};
            size_t pos = 0;
            bool found = false;
            for (const std::string &project : PROJECTS) {
                pos = test_binary_str.find(project);
                if (std::string::npos != pos) {
                    found = true;
                    _testDataPath = currentPath + test_binary_str.substr(0, pos+project.size());
                }
            }
            if (!found) {
                if (test_binary_str.find(AIOS_PREFIX) == 0) {
                    pos = test_binary_str.find('/', AIOS_PREFIX.size());
                } else if (test_binary_str.find(HA3_PLUGINS_PREFIX) == 0) {
                    pos = test_binary_str.find('/', HA3_PLUGINS_PREFIX.size());
                } else if (test_binary_str.find(UNIFIED_PLUGINS_PREFIX) == 0) {
                    pos = test_binary_str.find('/', UNIFIED_PLUGINS_PREFIX.size());
                } else {
                    pos = test_binary_str.find('/');
                }
                _testDataPath = currentPath + test_binary_str.substr(0, pos) + "/";
            }
            if (!test_binary_str.empty() && test_binary_str[0] == '/') {
                _testDataPath = currentPath;
            }
        } else {
            _privateTestDataPath = currentPath + "testdata/"; // MAY not correct
            _privateTestDataPath2 = currentPath + "test/testdata/"; // MAY not correct
            _testDataPath = currentPath;
        }
        if (_testTempRoot == "./") {
            char buf[PATH_MAX];
            if(getcwd(buf, PATH_MAX) != nullptr) {
                _testTempRoot = buf;
            }
        }
        std::cerr << "_workspacePath: " << (_workspacePath) << std::endl;
        std::cerr << "TEST_TMPDIR: " << (tmp_path ? tmp_path : "") << std::endl;
        std::cerr << "_testTempRoot: " << _testTempRoot << std::endl;
        std::cerr << "TEST_SRCDIR: " << (src_path ? src_path : "") << std::endl;
        std::cerr << "TEST_WORKSPACE: " << (workspace ? workspace : "") << std::endl;
        std::cerr << "TEST_BINARY: " << (test_binary ? test_binary : "") << std::endl;
        std::cerr << "_testDataPath: " << _testDataPath << std::endl;
        std::cerr << "_privateTestDataPath: " << _privateTestDataPath << std::endl;
        std::cerr << "_privateTestDataPath2: " << _privateTestDataPath2 << std::endl;
    }
    virtual ~TESTBASE_BASE() {}

public:
    void SetUp()
    {
        setenv("IS_TEST_MODE", "true", 1);
        testDataPathSetup();
        // ::chdir(_testDataPath.c_str());
        setUp();
    }

    void TearDown()
    {
        tearDown();
        // ::chdir(_testRootPath.c_str());
        testDataPathTearDown();
        unsetenv("IS_TEST_MODE");
    }

public:
    virtual void setUp() {}
    virtual void tearDown() {}

    std::string GET_CLASS_NAME() const
    {
        const ::testing::TestInfo *const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        return std::string(test_info->test_case_name()) + "_" + test_info->name();
    }

    const std::string &GET_WORKSPACE_PATH() const { return _workspacePath; }
    const std::string &GET_TEMPLATE_DATA_PATH() const { return _testClassTempPath; }
    const std::string &GET_TEMP_DATA_PATH() const { return GET_TEMPLATE_DATA_PATH(); }
    const std::string &TEST_ROOT_PATH() const { return _testDataPath; }
    std::string GET_BUILD_LIB_PATH() const { return _testDataPath + std::string("testdata/plugins"); }
    std::string GET_TEST_DATA_PATH() const { return _testDataPath + std::string("testdata/"); }
    std::string GET_PRIVATE_TEST_DATA_PATH() const
    {
        // MUST put testdata/ into the directory of test's BUILD file is in.
        return _privateTestDataPath;
    }
    std::string GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() const
    {
        // MUST put test/testdata/ into the directory of test's BUILD file is in.
        return _privateTestDataPath2;
    }

protected:
    void replaceSlashWithUnderscore(std::string &s)
    {
        for (auto pos = s.find("/"); pos != std::string::npos; pos = s.find("/")) {
            s.replace(pos, 1, "_");
        }
    }

    virtual void testDataPathSetup()
    {
        std::string runtimeTestData = _testTempRoot + "/runtime_testdata/";
        if (0 != mkdir(runtimeTestData.c_str(), 0755) && EEXIST != errno) {
            ASSERT_FALSE(true) << "mkdir failed, " << runtimeTestData << " err " << strerror(errno);
        }
        std::string folderName = GET_CLASS_NAME();
        replaceSlashWithUnderscore(folderName);
        _testClassTempPath = runtimeTestData + folderName + "/";
        deleteExistDataPath();
        if (0 != mkdir(_testClassTempPath.c_str(), 0755) && EEXIST != errno) {
            ASSERT_FALSE(true) << "mkdir failed, " << _testClassTempPath << " err " << strerror(errno);
        }
    }

    virtual void testDataPathTearDown() { deleteExistDataPath(); }

    virtual void deleteExistDataPath()
    {
        if (!_testClassTempPath.empty() && _testClassTempPath.rfind(_testTempRoot) != std::string::npos &&
            access(_testClassTempPath.c_str(), F_OK) == 0) {
            remove(_testClassTempPath);
        }
    }

protected:
    bool isFile(const std::string &path)
    {
        struct stat buf;
        if (stat(path.c_str(), &buf) < 0) {
            return false;
        }
        if (S_ISREG(buf.st_mode)) {
            return true;
        }
        return false;
    }
    bool isDir(const std::string &path)
    {
        struct stat buf;
        if (stat(path.c_str(), &buf) < 0) {
            return false;
        }
        if (S_ISDIR(buf.st_mode)) {
            return true;
        }
        return false;
    }

    bool remove(const std::string &path)
    {
        if (isFile(path)) {
            return ::remove(path.c_str()) == 0;
        }
        if (isDir(path)) {
            DIR *pDir;
            struct dirent *ent;
            std::string childName;
            pDir = opendir(path.c_str());
            if (pDir == NULL) {
                return errno == ENOENT;
            }
            while ((ent = readdir(pDir)) != NULL) {
                childName = path + '/' + ent->d_name;
                if (ent->d_type & DT_DIR) {
                    if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) {
                        continue;
                    }
                    if (!remove(childName)) {
                        closedir(pDir);
                        return false;
                    }
                } else if (::remove(childName.c_str()) != 0) {
                    return false;
                }
            }

            closedir(pDir);
            if (::remove(path.c_str()) != 0) {
                if (errno != ENOENT) {
                    return false;
                }
            }
            return true;
        }

        if (::remove(path.c_str()) != 0) {
            return false;
        }
        return true;
    }

protected:
    std::string _workspacePath;
    std::string _testDataPath;
    std::string _testTempRoot;
    std::string _testClassTempPath;
    std::string _privateTestDataPath;
    std::string _privateTestDataPath2;
};

class TESTBASE : public virtual TESTBASE_BASE, public virtual ::testing::Test
{
public:
    void SetUp() override { TESTBASE_BASE::SetUp(); }
    void TearDown() override { TESTBASE_BASE::TearDown(); }
};

#if __cplusplus >= 201103L
#define ASSERT_CAST_AND_RETURN(expectType, pointer)                                                                    \
    ({                                                                                                                 \
        auto p = pointer;                                                                                              \
        ASSERT_TRUE(p) << "pointer is NULL";                                                                           \
        expectType *castResult__ = dynamic_cast<expectType *>(p);                                                      \
        ASSERT_TRUE(castResult__) << "Expect: " #expectType << " Actual: " << typeid(*p).name();                       \
        castResult__;                                                                                                  \
    })
#else
#define ASSERT_CAST_AND_RETURN(expectType, pointer)                                                                    \
    ({                                                                                                                 \
        __typeof__(pointer) p = pointer;                                                                               \
        ASSERT_TRUE(p) << "pointer is NULL";                                                                           \
        expectType *castResult__ = dynamic_cast<expectType *>(p);                                                      \
        ASSERT_TRUE(castResult__) << "Expect: " #expectType << " Actual: " << typeid(*p).name();                       \
        castResult__;                                                                                                  \
    })
#endif

// @note:
// This is a link seam helper. It finds the next function with same symbol and
// return. With this helper, you can fake the non-virtual function call easily
// by replace it at runtime.
//
// @param:
// you need to define type *func_type* before call this
//
// for static function, so as the signature:
// ex. typedef ThreadPtr (*func_type)(const std::function<void ()>& threadFunction);
//
// for class function, lift *this* to parameter list:
// ex. typedef void (*func_type)(ClassType*,  const std::function<void ()>& );
//
// @restriction:
// 1. you must use it at the function that you fake, since we get backtrace_symbol at depth == 1
// 2. it does not help with *inline* function.. you know
//
// Two way to use it with fake sources:
// 1. compile the fake sources file and generate the shared library by linking to the original library.
// 2. compile the fake sources file, add it to LD_PRELOAD at runtime.
//
// See FakeThread for example.

// the stack backtrace is like:
// /home/lk/test/libtesta.so(_ZN4Test1tESsR2CC+0x1c9) [0x7f7ba79ad1df]

#define GET_NEXT_FUNC(func_type)                                        \
    ({                                                                  \
        const size_t max_depth = 3;                                     \
        void *stack_addrs[max_depth];                                   \
                                                                        \
        size_t stack_depth = backtrace(stack_addrs, max_depth);         \
        assert(stack_depth <= 3);                                       \
        (void)stack_depth;                                              \
        char **stack_strings = backtrace_symbols(stack_addrs, max_depth); \
        std::string stackStr;                                           \
        for (size_t i = 0; i < stack_depth; ++i) {                      \
            std::string str = stack_strings[i];                         \
            if ((str.find("libasan.so") == std::string::npos) &&        \
                (str.find("backtrace+") == std::string::npos)) {        \
                stackStr = str;                                         \
                break;                                                  \
            }                                                           \
        }                                                               \
        size_t start = stackStr.find('(');                              \
        assert(start != std::string::npos);                             \
        stackStr = stackStr.substr(start + 1);                          \
        size_t end = stackStr.find('+');                                \
        assert(end != std::string::npos);                               \
        std::string funcSym = stackStr.substr(0, end);                  \
        free(stack_strings);                                            \
        func_type func = reinterpret_cast<func_type>(dlsym(RTLD_NEXT, funcSym.c_str())); \
        assert(func);                                                   \
        func;                                                           \
    })

#endif //__SUEZ_UNITTEST_H
