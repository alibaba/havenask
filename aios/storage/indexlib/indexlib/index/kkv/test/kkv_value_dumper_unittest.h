#ifndef __INDEXLIB_KKVVALUEDUMPERTEST_H
#define __INDEXLIB_KKVVALUEDUMPERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_value_dumper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(index, MockChunkStrategy);

namespace indexlib { namespace index {

enum ItemType {
    IT_DEL_PKEY,
    IT_DEL_SKEY,
    IT_ADD,
};

class KKVValueDumperTest : public INDEXLIB_TESTBASE
{
public:
    KKVValueDumperTest();
    ~KKVValueDumperTest();

    DECLARE_CLASS_NAME(KKVValueDumperTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void AddItem(KKVValueDumper<uint64_t, 4>& dumper, MockChunkStrategy* mockStrategy, uint64_t pkey, uint64_t skey,
                 uint32_t ts, ItemType itemType, const std::string& value, bool isLastNode, bool needFlush,
                 regionid_t regionId);

    void CheckIterator(const DumpableSKeyNodeIteratorPtr& iter, const std::string& str);

    void CheckData(const std::string& str);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KKVValueDumperTest, TestSimpleProcess);
}} // namespace indexlib::index

#endif //__INDEXLIB_KKVVALUEDUMPERTEST_H
