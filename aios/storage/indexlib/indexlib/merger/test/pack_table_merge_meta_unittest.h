#ifndef __INDEXLIB_PACKTABLEMERGEMETATEST_H
#define __INDEXLIB_PACKTABLEMERGEMETATEST_H

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_meta.h"
#include "indexlib/merger/table_merge_meta.h"
#include "indexlib/merger/test/table_merge_meta_unittest.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class PackTableMergeMetaTest : public TableMergeMetaTest
{
public:
    PackTableMergeMetaTest();
    ~PackTableMergeMetaTest();

    DECLARE_CLASS_NAME(PackTableMergeMetaTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::merger

#endif //__INDEXLIB_PACKTABLEMERGEMETATEST_H
