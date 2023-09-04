#ifndef __INDEXLIB_COLLECT_INFO_PROCESSOR_TEST_H
#define __INDEXLIB_COLLECT_INFO_PROCESSOR_TEST_H

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/collect_info_comparator.h"
#include "indexlib/index/kkv/collect_info_processor.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class CollectInfoProcessorTest : public INDEXLIB_TESTBASE
{
public:
    CollectInfoProcessorTest();
    ~CollectInfoProcessorTest();

    DECLARE_CLASS_NAME(CollectInfoProcessorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNoSortNorTruncate();
    void TestSortByKey();
    void TestSortByValue();
    void TestSortByTs();
    void TestMultiLayerResortBySkey();
    void TestMultiLayerResortBySortValue();

private:
    typedef SKeyCollectInfo CollectInfo;
    typedef std::vector<CollectInfo*> CollectInfos;
    typedef SKeyCollectInfoPool CollectInfoPool;

    // skey,isDel,ts,value;......
    void MakeCollectInfos(const std::string& infoStr, CollectInfos& infos);

    void CheckCollectInfos(const std::string& infoStr, const CollectInfos& infos);

private:
    autil::mem_pool::Pool mPool;
    config::IndexPartitionSchemaPtr mSchema;
    config::KKVIndexConfigPtr mKKVIndexConfig;
    CollectInfoPool mCollectInfoPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CollectInfoProcessorTest, TestNoSortNorTruncate);
INDEXLIB_UNIT_TEST_CASE(CollectInfoProcessorTest, TestSortByKey);
INDEXLIB_UNIT_TEST_CASE(CollectInfoProcessorTest, TestSortByValue);
INDEXLIB_UNIT_TEST_CASE(CollectInfoProcessorTest, TestSortByTs);
INDEXLIB_UNIT_TEST_CASE(CollectInfoProcessorTest, TestMultiLayerResortBySkey);
INDEXLIB_UNIT_TEST_CASE(CollectInfoProcessorTest, TestMultiLayerResortBySortValue);
}} // namespace indexlib::index

#endif //__INDEXLIB_COLLECT_INFO_PROCESSOR_TEST_H
