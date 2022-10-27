#ifndef __INDEXLIB_SUBDOCTEST_H
#define __INDEXLIB_SUBDOCTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/test/partition_state_machine.h"

IE_NAMESPACE_BEGIN(partition);

class SubDocTest : public INDEXLIB_TESTBASE_WITH_PARAM<std::tuple<bool, bool> >
{
public:
    SubDocTest();
    ~SubDocTest();

    DECLARE_CLASS_NAME(SubDocTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestAddMainDocWithSubDoc();
    void TestAddMainDocOnly();
    void TestAddThreeDocWithOneEmptySubDoc();
    void TestAddDocWithoutPk();
    void TestAddDupSubDocInFull();
    void TestAddDupSubDocInInc();
    void TestAddDupSubDocInRt();
    void TestAddDupSubDocInOneMainDocInRt();
    void TestAddDocInRtOnly();
    void TestAddDupMainDoc();
    void TestAddDupMainDocWithDiffSubDoc();
    void TestAddDocWithoutAttributeSchema();
    void TestBuildingSegmentDedup();
    void TestBuiltSegmentsDedup();

    void TestRewriteToUpdateDocWithMainFieldModified();
    void TestRewriteToUpdateDocWithSubFieldModified();
    void TestRewriteToUpdateDocWithOnlySubFieldModified();
    void TestRewriteToUpdateDocForExceptionSubModifiedField();

    void TestUpdateMainDocOnly();
    void TestUpdateSubDocOnly();
    void TestUpdateMainDocWithSubDoc();
    void TestUpdateMainDocWithSubDocInOneSegment();
    void TestUpdateSubPkNotInSameDoc();

    void TestDeleteMainDocOnly();
    void TestDeleteMainDocInOneFullSegment();
    void TestDeleteMainDocInOneRtSegment();
    void TestDeleteSubDocOnly();
    void TestDeleteSubPkNotInSameDoc();

    void TestObsoleteRtDoc();

    void TestNeedDumpByMaxDocCount();
    void TestNeedDumpByMemorUse();

    void TestReOpenWithAdd();
    void TestReOpenWithUpdate();
    void TestReOpenWithDeleteSub();
    void TestForceReOpenWithAdd();
    void TestIncCoverPartialRtWithDupSubDoc();
    void TestIncCoverEntireRt();
    void TestIncCoverEntireRtWhenIncDiffRt();
    void TestIncCoverPartialRt();

    void TestMerge();
    void TestMergeForJoinDocIdWithEmptySubDocAndDupDoc();
    void TestMergeWithEmptySubSegment();
    void TestMergeWithEmptyMainAndSubSegment();

    void TestGetDocIdAfterAddDocument();
    void TestGetDocIdAfterUpdateDocument();
    void TestGetDocIdAfterRemoveDocument();

    void TestGetDocIdAfterRemoveSubDocument();
    void TestGetDocIdAfterOnlyUpdateSubDocument();

    void TestOptimizedReopenForIncNewerThanRt();
    void TestOptimizedReopenForAllModifiedOnDiskDocUnchanged();
    void TestOptimizedReopenForAllModifiedOnDiskDocChanged();
    void TestOptimizedReopenForAllModifiedRtDocBeenCovered();
    void TestOptimizedReopenForAllModifiedRtDocNotBeenCovered();

    void TestBuildAndMergeWithPackageFile();
    void TestBuildEmptySegmentInSubSegment();

    void TestAccessCounters();

    void TestAddMainSubDocWithMultiInMemSegments();
    void TestMergeWithPackageFile();

private:
    static bool CheckDocCount(const test::PartitionStateMachine& psm,
                              size_t mainDocCount, size_t subDocCount);
    void TestGetDocIdWithCommand(std::string command);

    void DoTestOptimizedReopen(const std::string& fullDocSeq,
                               const std::string& rtDocSeq,
                               const std::string& incDocSeq,
                               bool isIncMerge);

    void InnerTestBuildAndMergeWithPackageFile(bool optimizeMerge);

    std::string GetDoc(const std::vector<std::string>& docs,
                       const std::string& docSeq);

    void CheckPackageFile(const std::string& segmentDirPath);
    bool CheckAccessCounter(const std::string& name, int expect);

    void DoTestMergeForJoinDocIdWithEmptySubDocAndDupDoc(bool multiTargetSegment);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    index_base::PartitionMetaPtr mPartMeta;
    config::IndexPartitionOptions mOptions;
    test::PartitionStateMachine mPsm;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(SubDocTestMode, SubDocTest, 
                                     testing::Values(
                                             std::tuple<bool, bool>(true, true),
                                             std::tuple<bool, bool>(false, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddMainDocWithSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddMainDocOnly);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddThreeDocWithOneEmptySubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddDocWithoutPk);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddDupSubDocInFull);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddDupSubDocInInc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddDupSubDocInRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddDupSubDocInOneMainDocInRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddDocInRtOnly);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddDupMainDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddDupMainDocWithDiffSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddDocWithoutAttributeSchema);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestBuildingSegmentDedup);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestBuiltSegmentsDedup);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestRewriteToUpdateDocWithMainFieldModified);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestRewriteToUpdateDocWithSubFieldModified);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestRewriteToUpdateDocWithOnlySubFieldModified);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestRewriteToUpdateDocForExceptionSubModifiedField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestUpdateMainDocOnly);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestUpdateSubDocOnly);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestUpdateMainDocWithSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestUpdateMainDocWithSubDocInOneSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestUpdateSubPkNotInSameDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestDeleteMainDocOnly);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestDeleteMainDocInOneFullSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestDeleteMainDocInOneRtSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestDeleteSubDocOnly);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestDeleteSubPkNotInSameDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestObsoleteRtDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestNeedDumpByMaxDocCount);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestNeedDumpByMemorUse);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestReOpenWithAdd);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestReOpenWithUpdate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestReOpenWithDeleteSub);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestForceReOpenWithAdd);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestIncCoverPartialRtWithDupSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestIncCoverEntireRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestIncCoverEntireRtWhenIncDiffRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestIncCoverPartialRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestMergeForJoinDocIdWithEmptySubDocAndDupDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestMergeWithEmptySubSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestMergeWithEmptyMainAndSubSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestGetDocIdAfterUpdateDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestGetDocIdAfterOnlyUpdateSubDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestOptimizedReopenForIncNewerThanRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestOptimizedReopenForAllModifiedOnDiskDocUnchanged);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestOptimizedReopenForAllModifiedOnDiskDocChanged);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestOptimizedReopenForAllModifiedRtDocBeenCovered);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestOptimizedReopenForAllModifiedRtDocNotBeenCovered);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestBuildAndMergeWithPackageFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestBuildEmptySegmentInSubSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAccessCounters);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTestMode, TestAddMainSubDocWithMultiInMemSegments);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
        PackageTestMode, SubDocTest, 
        testing::Values(std::tuple<bool, bool>(false, true), // no tag, checkpoint = 0
                        std::tuple<bool, bool>(true, false), // has tag, checkpoint = 600
                        std::tuple<bool, bool>(true, true)));// has tag, checkpoint = 0
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackageTestMode, TestMergeWithPackageFile);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SUBDOCTEST_H
