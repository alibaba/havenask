#ifndef __INDEXLIB_HASHPRIMARYKEYPERFTEST_H
#define __INDEXLIB_HASHPRIMARYKEYPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/primarykey/hash_primary_key_formatter.h"
#include "indexlib/test/multi_thread_test_base.h"

IE_NAMESPACE_BEGIN(index);

class HashPrimaryKeyPerfTest : public test::MultiThreadTestBase
{
private:
    typedef HashPrimaryKeyFormatter<uint64_t> Formatter;
    typedef std::tr1::shared_ptr<Formatter> FormatterPtr;
public:
    HashPrimaryKeyPerfTest();
    ~HashPrimaryKeyPerfTest();

    DECLARE_CLASS_NAME(HashPrimaryKeyPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();

    void DoWrite() override;
    void DoRead(int* status) override;
private:
    void CreateHashTable();
private:
    std::vector<char> mData;
    Formatter* mFormatter;
    std::vector<FormatterPtr> mFormatters;


private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyPerfTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_HASHPRIMARYKEYPERFTEST_H
