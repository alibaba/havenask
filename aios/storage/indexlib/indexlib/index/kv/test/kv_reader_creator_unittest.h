#ifndef __INDEXLIB_KVREADERIMPLFACTORYTEST_H
#define __INDEXLIB_KVREADERIMPLFACTORYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/index/kv/kv_factory.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class KVReaderCreatorTest : public INDEXLIB_TESTBASE
{
public:
    KVReaderCreatorTest();
    ~KVReaderCreatorTest();

    DECLARE_CLASS_NAME(KVReaderCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestGetKVTypeId();
    void TestCreate();
    void TestCompactHashKeyInKVTypeId();

private:
    void InnerTestGetKVTypeId(const std::string& field, bool enableTTL, bool isVarLen, FieldType valueType);

    void InnerTestCompactHashKeyInKVTypeId(const std::string& field, bool enableCompactKey, bool expectCompactKey);

    config::KVIndexConfigPtr CreateKVConfig(const std::string& field, bool enableTTL = false,
                                            bool enableCompactKey = false);

    template <typename T>
    void InnerTestCreate(const std::string& field, bool enableTTL);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KVReaderCreatorTest, TestGetKVTypeId);
INDEXLIB_UNIT_TEST_CASE(KVReaderCreatorTest, TestCreate);
INDEXLIB_UNIT_TEST_CASE(KVReaderCreatorTest, TestCompactHashKeyInKVTypeId);
}} // namespace indexlib::index

#endif //__INDEXLIB_KVREADERIMPLFACTORYTEST_H
