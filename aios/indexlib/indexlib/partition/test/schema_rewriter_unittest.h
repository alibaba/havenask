#ifndef __INDEXLIB_SCHEMAREWRITERTEST_H
#define __INDEXLIB_SCHEMAREWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/schema_rewriter.h"

IE_NAMESPACE_BEGIN(partition);

class SchemaRewriterTest : public INDEXLIB_TESTBASE
{
public:
    SchemaRewriterTest();
    ~SchemaRewriterTest();

    DECLARE_CLASS_NAME(SchemaRewriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestForRewriteForPrimaryKey();

private:
    void CheckPrimaryKeySpeedUp(
            bool speedUp, const config::IndexPartitionSchemaPtr& schema);
    void CheckDefragSlicePercent(float defragPercent,
                                 const config::IndexPartitionSchemaPtr& schema);


private:
    config::IndexPartitionSchemaPtr mSchema;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestForRewriteForPrimaryKey);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SCHEMAREWRITERTEST_H
