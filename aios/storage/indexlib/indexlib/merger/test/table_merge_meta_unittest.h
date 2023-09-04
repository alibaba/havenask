#ifndef __INDEXLIB_TABLEMERGEMETATEST_H
#define __INDEXLIB_TABLEMERGEMETATEST_H

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_meta.h"
#include "indexlib/merger/table_merge_meta.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class TableMergeMetaTest : public INDEXLIB_TESTBASE
{
public:
    TableMergeMetaTest();
    ~TableMergeMetaTest();

    DECLARE_CLASS_NAME(TableMergeMetaTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestLoadWithoutPlanResource();

private:
    void CompareMergeMeta(const MergeMetaPtr& expectMeta, const MergeMetaPtr& actualMeta, bool loadResource);

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::merger

#endif //__INDEXLIB_TABLEMERGEMETATEST_H
