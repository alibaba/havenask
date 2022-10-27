#ifndef __INDEXLIB_INDEXLIBEXCEPTIONPERFTEST_H
#define __INDEXLIB_INDEXLIBEXCEPTIONPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/config/test/truncate_config_maker.h"

IE_NAMESPACE_BEGIN(index);

class IndexlibExceptionPerfTest : public INDEXLIB_TESTBASE
{
public:
    IndexlibExceptionPerfTest();
    ~IndexlibExceptionPerfTest();

    DECLARE_CLASS_NAME(IndexlibExceptionPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCaseForCreateBucketMapsExceptionAndRecover();
    void TestCaseForIOExceptionShutDown();
    void TestCaseForKKVIOExceptionShutDown();
    void TestCaseForDocReclaimException();
    void TestIndexBuilderInitFail();
    void TestSeekWithIOException();
private:
    void InnerTestBuild(size_t normalIOCount, bool asyncDump,
                        bool asyncIO, bool asyncMerge);
    void BuildTruncateIndex(
            const config::IndexPartitionSchemaPtr& schema, 
            const std::string& rawDocStr, bool needMerge);
    void InnerTestTruncateIOException(
            const std::string& path, uint32_t errorType, bool asyncIO = false);
    void ResetIndexPartitionSchema(const config::TruncateParams& param);
    void InnerTestForDocReclaimException(size_t normalIOCount);
    void InnerTestCaseForKKVIOExceptionShutDown(size_t normalIOCount);
private:
    enum class IOExceptionType : int{
        NO_EXCEPTION = 0,
        LOOKUP = 1,
        SEEK = 2
    };
private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    util::QuotaControlPtr mQuotaControl;
    std::string mFullDocs;
    std::string mIncDocs;
    std::string mRootDir;
    std::string mRawDocStr;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexlibExceptionPerfTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(IndexlibExceptionPerfTest, TestCaseForCreateBucketMapsExceptionAndRecover);
INDEXLIB_UNIT_TEST_CASE(IndexlibExceptionPerfTest, TestCaseForIOExceptionShutDown);
INDEXLIB_UNIT_TEST_CASE(IndexlibExceptionPerfTest, TestCaseForKKVIOExceptionShutDown);
INDEXLIB_UNIT_TEST_CASE(IndexlibExceptionPerfTest, TestCaseForDocReclaimException);
INDEXLIB_UNIT_TEST_CASE(IndexlibExceptionPerfTest, TestIndexBuilderInitFail);
INDEXLIB_UNIT_TEST_CASE(IndexlibExceptionPerfTest, TestSeekWithIOException);
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEXLIBEXCEPTIONPERFTEST_H
