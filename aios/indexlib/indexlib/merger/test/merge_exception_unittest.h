#ifndef __INDEXLIB_MERGEEXCEPTIONUNITTESTTEST_H
#define __INDEXLIB_MERGEEXCEPTIONUNITTESTTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(merger, IndexPartitionMerger);
DECLARE_REFERENCE_CLASS(merger, MergeMeta);

IE_NAMESPACE_BEGIN(merger);

class MergeExceptionUnittestTest : public INDEXLIB_TESTBASE
{
public:
    MergeExceptionUnittestTest();
    ~MergeExceptionUnittestTest();

    DECLARE_CLASS_NAME(MergeExceptionUnittestTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNormalTable();
    void TestNormalTableWithTruncate();

private:
    template <class MergerCreator>
    void InnerTestMerge(MergerCreator mergerCreator,
                        config::IndexPartitionSchemaPtr schema,
                        const config::IndexPartitionOptions& options,
                        uint32_t exceptionStep = 37);
    void PrepareBaseline(IndexPartitionMergerPtr merger);
    void ResetFsAfterException();
    MergeMetaPtr LoadMergeMeta();
    void StoreMergeMeta(MergeMetaPtr mergeMeta);
    inline std::string ToStr(size_t i) { return autil::StringUtil::toString(i); }

private:
    void PrepareNormalIndex(
        const std::string& path, config::IndexPartitionSchemaPtr& schema,
        config::IndexPartitionOptions& options,
        index_base::ParallelBuildInfo& parallelBuildInfo);
    void CheckNormalIndex(
        const std::string& path, config::IndexPartitionSchemaPtr schema,
        const config::IndexPartitionOptions& options);

    //////////
    void PrepareNormalWithTruncateIndex(
        const std::string& path, config::IndexPartitionSchemaPtr& schema,
        config::IndexPartitionOptions& options);
    void CheckNormalWithTruncateIndex(
        const std::string& path, config::IndexPartitionSchemaPtr schema,
        const config::IndexPartitionOptions& options);

private:
    std::string mRootPath;
    std::string mBuildPath;
    std::string mMergePath;
    std::string mBaselineMergePath;  // you can find baseline files in this dir for debug
    std::string mExceptionTimepointMergePath;  // you can find copy of mergePath at the timepoint of [end of exception-happend merge step] for debug
    std::string mMergeMetaPath;
    uint32_t mLogLevel;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeExceptionUnittestTest, TestNormalTable);
INDEXLIB_UNIT_TEST_CASE(MergeExceptionUnittestTest, TestNormalTableWithTruncate);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGEEXCEPTIONUNITTESTTEST_H
