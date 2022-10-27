#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/dictionary_config.h"
#include "indexlib/config/adaptive_dictionary_config.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger_creator.h"
#include "indexlib/index/normal/adaptive_bitmap/df_adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/adaptive_bitmap/percent_adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/adaptive_bitmap/index_size_adaptive_bitmap_trigger.h"
#include <autil/HashAlgorithm.h>

#define TOP_TERM "a"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);

class PostingMergerMock : public PostingMerger
{
public:
    PostingMergerMock(df_t docFreq, uint32_t postingLength)
        : mPostingLength(postingLength), mDocFreq(docFreq)
    {}
    ~PostingMergerMock() {}
public:
    virtual void Merge(const SegmentTermInfos& segTermInfos, 
                       const ReclaimMapPtr& reclaimMap) {}

    virtual void SortByWeightMerge(const SegmentTermInfos& segTermInfos, 
                                   const ReclaimMapPtr& reclaimMap) {}
    virtual void Dump(dictkey_t key, IndexOutputSegmentResources& indexOutputSegmentResources) {}

    virtual uint64_t GetDumpLength() const { return 0; }
    virtual ttf_t GetTotalTF()  { return 0; }
    virtual df_t GetDocFreq()  { return mDocFreq; }
    virtual uint64_t GetPostingLength() const
    { return mPostingLength; }
    virtual bool IsCompressedPostingHeader() const { return true; }
    virtual PostingIteratorPtr CreatePostingIterator()
        {
            return PostingIteratorPtr();
        } 
private:
    uint32_t mPostingLength;
    df_t mDocFreq;
};

class ReclaimMapMock : public ReclaimMap
{
public:
    ReclaimMapMock(uint32_t docCount) : mDocCount(docCount) {}
    ~ReclaimMapMock() {}
public:
    uint32_t GetNewDocCount() const
    {
        return mDocCount;
    }
private:
    uint32_t mDocCount;
};

class AdaptiveBitmapTriggerCreatorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(AdaptiveBitmapTriggerCreatorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForCreate()
    {
        ReclaimMapPtr ptr(new ReclaimMapMock(10));
        AdaptiveBitmapTriggerCreator creator(ptr);
        IndexConfigPtr indexConfig(
                new SingleFieldIndexConfig("phrase", it_text));
        {
            AdaptiveBitmapTriggerPtr trigger =  creator.Create(indexConfig);
            INDEXLIB_TEST_TRUE(NULL == trigger);
        }

        {
            AdaptiveDictionaryConfigPtr adaptiveDict = CreateAdaptiveDictConfig("dict", "DOC_FREQUENCY", 20);
            DictionaryConfigPtr dict = CreateDictConfig(true);
            indexConfig->SetDictConfig(dict);
            AdaptiveBitmapTriggerPtr trigger =  creator.Create(indexConfig);
            INDEXLIB_TEST_TRUE(NULL == trigger);
        }

        {
            AdaptiveDictionaryConfigPtr adaptiveDict = CreateAdaptiveDictConfig("dict", "DOC_FREQUENCY", 20);
            DictionaryConfigPtr dict = CreateDictConfig(false);
            indexConfig->SetDictConfig(dict);
            indexConfig->SetAdaptiveDictConfig(adaptiveDict);
            AdaptiveBitmapTriggerPtr trigger =  creator.Create(indexConfig);
            DfAdaptiveBitmapTrigger* dfTrigger =
                dynamic_cast<DfAdaptiveBitmapTrigger*>(trigger.get());
            INDEXLIB_TEST_TRUE(NULL != dfTrigger);
        }

        {
            AdaptiveDictionaryConfigPtr adaptiveDict = CreateAdaptiveDictConfig("dict", "PERCENT", 20);
            DictionaryConfigPtr dict = CreateDictConfig(false);
            indexConfig->SetDictConfig(dict);
            indexConfig->SetAdaptiveDictConfig(adaptiveDict);
            AdaptiveBitmapTriggerPtr trigger =  creator.Create(indexConfig);
            PercentAdaptiveBitmapTrigger* percentTrigger =
                dynamic_cast<PercentAdaptiveBitmapTrigger*>(trigger.get());
            INDEXLIB_TEST_TRUE(NULL != percentTrigger);
        }

        {
            AdaptiveDictionaryConfigPtr adaptiveDict = CreateAdaptiveDictConfig("dict", "INDEX_SIZE", 20);
            DictionaryConfigPtr dict = CreateDictConfig(false);
            indexConfig->SetDictConfig(dict);
            indexConfig->SetAdaptiveDictConfig(adaptiveDict);
            AdaptiveBitmapTriggerPtr trigger =  creator.Create(indexConfig);
            IndexSizeAdaptiveBitmapTrigger* indexSizeTrigger =
                dynamic_cast<IndexSizeAdaptiveBitmapTrigger*>(trigger.get());
            INDEXLIB_TEST_TRUE(NULL !=indexSizeTrigger);
        }
    }

    void TestCaseForIndexSizeAdaptiveBitmapTrigger()
    {
        ReclaimMapPtr ptr(new ReclaimMapMock(80));
        AdaptiveBitmapTriggerCreator creator(ptr);
        IndexConfigPtr indexConfig(
                new SingleFieldIndexConfig("phrase", it_text));
        AdaptiveDictionaryConfigPtr adaptiveDict = CreateAdaptiveDictConfig("dict", "INDEX_SIZE", 20);
        DictionaryConfigPtr dict = CreateDictConfig(false);
        indexConfig->SetDictConfig(dict);
        indexConfig->SetAdaptiveDictConfig(adaptiveDict);
        dictkey_t key = autil::HashAlgorithm::hashString64(TOP_TERM);
        AdaptiveBitmapTriggerPtr trigger =  creator.Create(indexConfig);
        {
            //bitmap size bigger
            PostingMergerPtr ptr(new PostingMergerMock(80, 5));
            bool result = trigger->MatchAdaptiveBitmap(ptr);
            INDEXLIB_TEST_TRUE(!result);
        }

        {
            //bitmap size smaller
            PostingMergerPtr ptr(new PostingMergerMock(80, 50));
            bool result = trigger->MatchAdaptiveBitmap(ptr);
            INDEXLIB_TEST_TRUE(result);

            // key is in dict config
            result = trigger->NeedGenerateAdaptiveBitmap(key, ptr);
            INDEXLIB_TEST_TRUE(!result);
        }
    }

    void TestCaseForPercentAdaptiveBitmapTrigger()
    {
        ReclaimMapPtr ptr(new ReclaimMapMock(80));
        AdaptiveBitmapTriggerCreator creator(ptr);
        IndexConfigPtr indexConfig(
                new SingleFieldIndexConfig("phrase", it_text));
        AdaptiveDictionaryConfigPtr adaptiveDict = CreateAdaptiveDictConfig("dict", "PERCENT", 20);
        DictionaryConfigPtr dict = CreateDictConfig(false);
        indexConfig->SetDictConfig(dict);
        indexConfig->SetAdaptiveDictConfig(adaptiveDict);
        dictkey_t key = autil::HashAlgorithm::hashString64(TOP_TERM);
        AdaptiveBitmapTriggerPtr trigger =  creator.Create(indexConfig);
        {
            PostingMergerPtr ptr(new PostingMergerMock(15, 5));
            bool result = trigger->MatchAdaptiveBitmap(ptr);
            INDEXLIB_TEST_TRUE(!result);
        }

        {
            PostingMergerPtr ptr(new PostingMergerMock(17, 50));
            bool result = trigger->MatchAdaptiveBitmap(ptr);
            INDEXLIB_TEST_TRUE(result);

            // key is in dict config
            result = trigger->NeedGenerateAdaptiveBitmap(key, ptr);
            INDEXLIB_TEST_TRUE(!result);
        }
    }

    void TestCaseForDfAdaptiveBitmapTrigger()
    {
        ReclaimMapPtr ptr(new ReclaimMapMock(80));
        AdaptiveBitmapTriggerCreator creator(ptr);
        IndexConfigPtr indexConfig(
                new SingleFieldIndexConfig("phrase", it_text));
        AdaptiveDictionaryConfigPtr adaptiveDict = CreateAdaptiveDictConfig("dict", "DOC_FREQUENCY", 20);
        DictionaryConfigPtr dict = CreateDictConfig(false);
        indexConfig->SetDictConfig(dict);
        indexConfig->SetAdaptiveDictConfig(adaptiveDict);
        dictkey_t key = autil::HashAlgorithm::hashString64(TOP_TERM);
        AdaptiveBitmapTriggerPtr trigger =  creator.Create(indexConfig);
        {
            PostingMergerPtr ptr(new PostingMergerMock(15, 5));
            bool result = trigger->MatchAdaptiveBitmap(ptr);
            INDEXLIB_TEST_TRUE(!result);
        }

        {
            PostingMergerPtr ptr(new PostingMergerMock(21, 50));
            bool result = trigger->MatchAdaptiveBitmap(ptr);
            INDEXLIB_TEST_TRUE(result);

            // key is in dict config
            result = trigger->NeedGenerateAdaptiveBitmap(key, ptr);
            INDEXLIB_TEST_TRUE(!result);
        }
    }

private:    
    DictionaryConfigPtr CreateDictConfig(bool isAdaptiveDict)
    {
        DictionaryConfigPtr ptr(new DictionaryConfig("dicName", TOP_TERM));
        return ptr;
    }

    AdaptiveDictionaryConfigPtr CreateAdaptiveDictConfig(
            string ruleName,
            string type,
            int32_t threshold)
    {
        AdaptiveDictionaryConfigPtr adaptiveDict(
                new AdaptiveDictionaryConfig(ruleName, type, threshold));
        return adaptiveDict;
    }
    
public:
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, AdaptiveBitmapTriggerCreatorTest);

INDEXLIB_UNIT_TEST_CASE(AdaptiveBitmapTriggerCreatorTest, TestCaseForCreate);
INDEXLIB_UNIT_TEST_CASE(AdaptiveBitmapTriggerCreatorTest, TestCaseForIndexSizeAdaptiveBitmapTrigger);
INDEXLIB_UNIT_TEST_CASE(AdaptiveBitmapTriggerCreatorTest, TestCaseForPercentAdaptiveBitmapTrigger);
INDEXLIB_UNIT_TEST_CASE(AdaptiveBitmapTriggerCreatorTest, TestCaseForDfAdaptiveBitmapTrigger);

IE_NAMESPACE_END(index);
