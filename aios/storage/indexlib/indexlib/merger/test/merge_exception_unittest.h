#ifndef __INDEXLIB_MERGEEXCEPTIONUNITTESTTEST_H
#define __INDEXLIB_MERGEEXCEPTIONUNITTESTTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(merger, IndexPartitionMerger);
DECLARE_REFERENCE_CLASS(merger, MergeMeta);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace merger {

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
    void TestKVTable();

private:
    template <class MergerCreator>
    void InnerTestMerge(MergerCreator mergerCreator, config::IndexPartitionSchemaPtr schema,
                        const config::IndexPartitionOptions& options, uint32_t exceptionStep);
    template <class MergerCreator>
    void PrepareBaseline(MergerCreator mergerCreator, const config::IndexPartitionOptions& options,
                         bool enablePackageFile);
    void ResetFsAfterException();
    MergeMetaPtr LoadMergeMeta();
    void StoreMergeMeta(MergeMetaPtr mergeMeta);
    inline std::string ToStr(size_t i) { return autil::StringUtil::toString(i); }

private:
    void PrepareNormalIndex(const std::string& path, config::IndexPartitionSchemaPtr& schema,
                            config::IndexPartitionOptions& options, index_base::ParallelBuildInfo& parallelBuildInfo);
    void CheckNormalIndex(const std::string& path, config::IndexPartitionSchemaPtr schema,
                          const config::IndexPartitionOptions& options);

    //////////
    void PrepareNormalWithTruncateIndex(const std::string& path, config::IndexPartitionSchemaPtr& schema,
                                        config::IndexPartitionOptions& options);
    void CheckNormalWithTruncateIndex(const std::string& path, config::IndexPartitionSchemaPtr schema,
                                      const config::IndexPartitionOptions& options);

    /////////
    void PrepareKVIndex(const std::string& path, config::IndexPartitionSchemaPtr& schema,
                        config::IndexPartitionOptions& options);
    void CheckKVIndex(const std::string& path, config::IndexPartitionSchemaPtr schema,
                      const config::IndexPartitionOptions& options);

private:
    std::string mRootPath;
    std::string mBuildPath;
    std::string mMergePath;
    file_system::DirectoryPtr mMergeDir;
    std::string mBaselineMergePath;           // you can find baseline files in this dir for debug
    std::string mExceptionTimepointMergePath; // you can find copy of mergePath at the timepoint of [end of
                                              // exception-happend merge step] for debug
    std::string mMergeMetaPath;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeExceptionUnittestTest, TestNormalTable);
INDEXLIB_UNIT_TEST_CASE(MergeExceptionUnittestTest, TestKVTable);
INDEXLIB_UNIT_TEST_CASE(MergeExceptionUnittestTest, TestNormalTableWithTruncate);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGEEXCEPTIONUNITTESTTEST_H
