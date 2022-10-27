#ifndef __INDEXLIB_PACKINDEXMERGEMETATEST_H
#define __INDEXLIB_PACKINDEXMERGEMETATEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/test/index_merge_meta_unittest.h"

IE_NAMESPACE_BEGIN(merger);

class PackIndexMergeMetaTest : public IndexMergeMetaTest
{
public:
    PackIndexMergeMetaTest();
    ~PackIndexMergeMetaTest();

    DECLARE_CLASS_NAME(PackIndexMergeMetaTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_PACKINDEXMERGEMETATEST_H
