#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/index/kv/hash_table_fix_writer.h"
#include "indexlib/index/kv/hash_table_var_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class CreateKVWriterTest : public INDEXLIB_TESTBASE
{
public:
    CreateKVWriterTest();
    ~CreateKVWriterTest();

    DECLARE_CLASS_NAME(CreateKVWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreate();
    void TestOccupancy();

private:
    template <typename T, bool compactKey, bool shortOffset = false>
    void CheckCreate(const std::string& configStr, bool enableCompactKey);

    template <typename Traits>
    void InnerCheckHashWriter(const KVWriterPtr& kvWriter, bool isVarLen);

    template <typename T, typename Writer>
    void CheckCreateWithCuckoo(const std::string& configStr);

    config::KVIndexConfigPtr CreateKVConfig(const std::string& field);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CreateKVWriterTest, TestCreate);
INDEXLIB_UNIT_TEST_CASE(CreateKVWriterTest, TestOccupancy);
}} // namespace indexlib::index
