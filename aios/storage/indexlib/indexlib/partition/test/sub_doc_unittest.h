#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class SubDocTest : public INDEXLIB_TESTBASE_WITH_PARAM<std::tuple<bool, bool, int>>
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
    static bool CheckDocCount(const test::PartitionStateMachine& psm, size_t mainDocCount, size_t subDocCount);
    void TestGetDocIdWithCommand(std::string command);

    void DoTestOptimizedReopen(const std::string& fullDocSeq, const std::string& rtDocSeq, const std::string& incDocSeq,
                               bool isIncMerge);

    void InnerTestBuildAndMergeWithPackageFile(bool optimizeMerge);

    std::string GetDoc(const std::vector<std::string>& docs, const std::string& docSeq);

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

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddMainDocWithSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddMainDocOnly);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddThreeDocWithOneEmptySubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddDocWithoutPk);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddDupSubDocInFull);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddDupSubDocInInc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddDupSubDocInRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddDupSubDocInOneMainDocInRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddDocInRtOnly);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddDupMainDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddDupMainDocWithDiffSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddDocWithoutAttributeSchema);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestBuildingSegmentDedup);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestBuiltSegmentsDedup);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestRewriteToUpdateDocWithMainFieldModified);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestRewriteToUpdateDocWithSubFieldModified);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestRewriteToUpdateDocWithOnlySubFieldModified);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestRewriteToUpdateDocForExceptionSubModifiedField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestUpdateMainDocOnly);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestUpdateSubDocOnly);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestUpdateMainDocWithSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestUpdateMainDocWithSubDocInOneSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestUpdateSubPkNotInSameDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestDeleteMainDocOnly);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestDeleteMainDocInOneFullSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestDeleteMainDocInOneRtSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestDeleteSubDocOnly);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestDeleteSubPkNotInSameDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestObsoleteRtDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestNeedDumpByMaxDocCount);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestNeedDumpByMemorUse);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestReOpenWithAdd);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestReOpenWithUpdate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestReOpenWithDeleteSub);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestForceReOpenWithAdd);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestIncCoverPartialRtWithDupSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestIncCoverEntireRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestIncCoverEntireRtWhenIncDiffRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestIncCoverPartialRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestMergeForJoinDocIdWithEmptySubDocAndDupDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestMergeWithEmptySubSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestMergeWithEmptyMainAndSubSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestGetDocIdAfterUpdateDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestGetDocIdAfterOnlyUpdateSubDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestOptimizedReopenForIncNewerThanRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestOptimizedReopenForAllModifiedOnDiskDocUnchanged);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestOptimizedReopenForAllModifiedOnDiskDocChanged);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestOptimizedReopenForAllModifiedRtDocBeenCovered);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestOptimizedReopenForAllModifiedRtDocNotBeenCovered);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestBuildAndMergeWithPackageFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestBuildEmptySegmentInSubSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAccessCounters);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SubDocTest, TestAddMainSubDocWithMultiInMemSegments);

INSTANTIATE_TEST_CASE_P(SubDocTestModeAndBuildMode, SubDocTest,
                        testing::Values(std::tuple<bool, bool, int>(true, true, 0),
                                        std::tuple<bool, bool, int>(false, false, 0),
                                        std::tuple<bool, bool, int>(false, true, 0),
                                        std::tuple<bool, bool, int>(true, false, 0)));
}} // namespace indexlib::partition
