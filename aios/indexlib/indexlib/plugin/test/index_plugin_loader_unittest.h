#ifndef __INDEXLIB_INDEXPLUGINLOADERTEST_H
#define __INDEXLIB_INDEXPLUGINLOADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/plugin/index_plugin_loader.h"

IE_NAMESPACE_BEGIN(plugin);

class IndexPluginLoaderTest : public INDEXLIB_TESTBASE
{
public:
    IndexPluginLoaderTest();
    ~IndexPluginLoaderTest();

    DECLARE_CLASS_NAME(IndexPluginLoaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    // void TestLoadOfflineModules();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexPluginLoaderTest, TestSimpleProcess);
// INDEXLIB_UNIT_TEST_CASE(IndexPluginLoaderTest, TestLoadOfflineModules);

IE_NAMESPACE_END(plugin);

#endif //__INDEXLIB_INDEXPLUGINLOADERTEST_H
