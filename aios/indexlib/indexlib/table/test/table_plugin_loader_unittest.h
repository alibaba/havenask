#ifndef __INDEXLIB_TABLEPLUGINLOADERTEST_H
#define __INDEXLIB_TABLEPLUGINLOADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/table/table_plugin_loader.h"

IE_NAMESPACE_BEGIN(table);

class TablePluginLoaderTest : public INDEXLIB_TESTBASE
{
public:
    TablePluginLoaderTest();
    ~TablePluginLoaderTest();

    DECLARE_CLASS_NAME(TablePluginLoaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestLoadException();
    void TestLoadNormal();

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TablePluginLoaderTest, TestLoadException);
INDEXLIB_UNIT_TEST_CASE(TablePluginLoaderTest, TestLoadNormal);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLEPLUGINLOADERTEST_H
