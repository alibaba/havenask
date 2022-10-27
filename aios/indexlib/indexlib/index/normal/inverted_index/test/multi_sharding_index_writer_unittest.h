#ifndef __INDEXLIB_MULTISHARDINGINDEXWRITERTEST_H
#define __INDEXLIB_MULTISHARDINGINDEXWRITERTEST_H

#include <autil/mem_pool/SimpleAllocator.h>
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_writer.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

IE_NAMESPACE_BEGIN(index);

class MultiShardingIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    MultiShardingIndexWriterTest();
    ~MultiShardingIndexWriterTest();

    DECLARE_CLASS_NAME(MultiShardingIndexWriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void PrepareIndex(
            const config::IndexSchemaPtr& indexSchema, 
            const std::string& indexName,
            const std::vector<document::NormalDocumentPtr>& docs, 
            const file_system::DirectoryPtr& indexDirectory);

    void CheckIndex(
            const config::IndexSchemaPtr& indexSchema, 
            const std::string& indexName,
            const std::vector<document::NormalDocumentPtr>& docs, 
            const file_system::DirectoryPtr& indexDirectory);

    void CheckToken(const std::vector<DictionaryReaderPtr>& dictReaders,
                    dictkey_t tokenKey);
            
private:
    util::SimplePool mSimplePool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MultiShardingIndexWriterTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTISHARDINGINDEXWRITERTEST_H
