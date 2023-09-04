#include "autil/HashAlgorithm.h"
#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger_creator.h"
#include "indexlib/index/normal/adaptive_bitmap/df_adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/adaptive_bitmap/index_size_adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/adaptive_bitmap/percent_adaptive_bitmap_trigger.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/test/unittest.h"

#define TOP_TERM "a"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index { namespace legacy {

class PostingMergerMock : public PostingMerger
{
public:
    PostingMergerMock(df_t docFreq, uint32_t postingLength) : mPostingLength(postingLength), mDocFreq(docFreq) {}
    ~PostingMergerMock() {}

public:
    virtual void Merge(const SegmentTermInfos& segTermInfos, const ReclaimMapPtr& reclaimMap) {}

    virtual void SortByWeightMerge(const SegmentTermInfos& segTermInfos, const ReclaimMapPtr& reclaimMap) {}
    virtual void Dump(const index::DictKeyInfo& key, IndexOutputSegmentResources& indexOutputSegmentResources) {}

    virtual uint64_t GetDumpLength() const { return 0; }
    virtual ttf_t GetTotalTF() { return 0; }
    virtual df_t GetDocFreq() { return mDocFreq; }
    virtual uint64_t GetPostingLength() const { return mPostingLength; }
    virtual bool IsCompressedPostingHeader() const { return true; }
    virtual std::shared_ptr<PostingIterator> CreatePostingIterator() { return std::shared_ptr<PostingIterator>(); }

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
    uint32_t GetNewDocCount() const { return mDocCount; }

private:
    uint32_t mDocCount;
};

class AdaptiveBitmapTriggerCreatorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(AdaptiveBitmapTriggerCreatorTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForCreate()
    {
        ReclaimMapPtr ptr(new ReclaimMapMock(10));
        AdaptiveBitmapTriggerCreator creator(ptr);
        SingleFieldIndexConfigPtr indexConfig(new SingleFieldIndexConfig("phrase", it_text));
        FieldConfigPtr fieldConfig(new FieldConfig("f1", ft_text, false));
        indexConfig->SetFieldConfig(fieldConfig);
        {
            AdaptiveBitmapTriggerPtr trigger = creator.Create(indexConfig);
            INDEXLIB_TEST_TRUE(NULL == trigger);
        }

        {
            std::shared_ptr<AdaptiveDictionaryConfig> adaptiveDict =
                CreateAdaptiveDictConfig("dict", "DOC_FREQUENCY", 20);
            std::shared_ptr<DictionaryConfig> dict = CreateDictConfig(true);
            indexConfig->SetDictConfig(dict);
            AdaptiveBitmapTriggerPtr trigger = creator.Create(indexConfig);
            INDEXLIB_TEST_TRUE(NULL == trigger);
        }

        {
            std::shared_ptr<AdaptiveDictionaryConfig> adaptiveDict =
                CreateAdaptiveDictConfig("dict", "DOC_FREQUENCY", 20);
            std::shared_ptr<DictionaryConfig> dict = CreateDictConfig(false);
            indexConfig->SetDictConfig(dict);
            indexConfig->SetAdaptiveDictConfig(adaptiveDict);
            AdaptiveBitmapTriggerPtr trigger = creator.Create(indexConfig);
            DfAdaptiveBitmapTrigger* dfTrigger = dynamic_cast<DfAdaptiveBitmapTrigger*>(trigger.get());
            INDEXLIB_TEST_TRUE(NULL != dfTrigger);
        }

        {
            std::shared_ptr<AdaptiveDictionaryConfig> adaptiveDict = CreateAdaptiveDictConfig("dict", "PERCENT", 20);
            std::shared_ptr<DictionaryConfig> dict = CreateDictConfig(false);
            indexConfig->SetDictConfig(dict);
            indexConfig->SetAdaptiveDictConfig(adaptiveDict);
            AdaptiveBitmapTriggerPtr trigger = creator.Create(indexConfig);
            PercentAdaptiveBitmapTrigger* percentTrigger = dynamic_cast<PercentAdaptiveBitmapTrigger*>(trigger.get());
            INDEXLIB_TEST_TRUE(NULL != percentTrigger);
        }

        {
            std::shared_ptr<AdaptiveDictionaryConfig> adaptiveDict = CreateAdaptiveDictConfig("dict", "INDEX_SIZE", 20);
            std::shared_ptr<DictionaryConfig> dict = CreateDictConfig(false);
            indexConfig->SetDictConfig(dict);
            indexConfig->SetAdaptiveDictConfig(adaptiveDict);
            AdaptiveBitmapTriggerPtr trigger = creator.Create(indexConfig);
            IndexSizeAdaptiveBitmapTrigger* indexSizeTrigger =
                dynamic_cast<IndexSizeAdaptiveBitmapTrigger*>(trigger.get());
            INDEXLIB_TEST_TRUE(NULL != indexSizeTrigger);
        }
    }

    void TestCaseForLayerHash()
    {
        ReclaimMapPtr ptr(new ReclaimMapMock(80));
        AdaptiveBitmapTriggerCreator creator(ptr);
        SingleFieldIndexConfigPtr indexConfig(new SingleFieldIndexConfig("phrase", it_text));
        std::shared_ptr<AdaptiveDictionaryConfig> adaptiveDict = CreateAdaptiveDictConfig("dict", "INDEX_SIZE", 20);
        std::shared_ptr<DictionaryConfig> dict(new DictionaryConfig("dicName", "a#b"));
        FieldConfigPtr fieldConfig(new FieldConfig("phrase", ft_text, false));
        fieldConfig->SetUserDefinedParam(util::ConvertToJsonMap({{"dict_hash_func", "LayerHash"}}));
        indexConfig->SetFieldConfig(fieldConfig);
        indexConfig->SetDictConfig(dict);
        indexConfig->SetAdaptiveDictConfig(adaptiveDict);

        util::LayerTextHasher layerHash({});
        dictkey_t key;
        ASSERT_TRUE(layerHash.GetHashKey("a#b", key));
        AdaptiveBitmapTriggerPtr trigger = creator.Create(indexConfig);
        {
            // bitmap size bigger
            PostingMergerPtr ptr(new PostingMergerMock(80, 5));
            bool result = trigger->MatchAdaptiveBitmap(ptr);
            INDEXLIB_TEST_TRUE(!result);
        }

        {
            // bitmap size smaller
            PostingMergerPtr ptr(new PostingMergerMock(80, 50));
            bool result = trigger->MatchAdaptiveBitmap(ptr);
            INDEXLIB_TEST_TRUE(result);

            // key is in dict config
            result = trigger->NeedGenerateAdaptiveBitmap(index::DictKeyInfo(key), ptr);
            INDEXLIB_TEST_TRUE(!result);
        }
    }

    void TestCaseForIndexSizeAdaptiveBitmapTrigger()
    {
        ReclaimMapPtr ptr(new ReclaimMapMock(80));
        AdaptiveBitmapTriggerCreator creator(ptr);
        SingleFieldIndexConfigPtr indexConfig(new SingleFieldIndexConfig("phrase", it_text));
        std::shared_ptr<AdaptiveDictionaryConfig> adaptiveDict = CreateAdaptiveDictConfig("dict", "INDEX_SIZE", 20);
        std::shared_ptr<DictionaryConfig> dict = CreateDictConfig(false);
        indexConfig->SetFieldConfig(FieldConfigPtr(new FieldConfig("phrase", ft_text, false)));
        indexConfig->SetDictConfig(dict);
        indexConfig->SetAdaptiveDictConfig(adaptiveDict);
        dictkey_t key = autil::HashAlgorithm::hashString64(TOP_TERM);
        AdaptiveBitmapTriggerPtr trigger = creator.Create(indexConfig);
        {
            // bitmap size bigger
            PostingMergerPtr ptr(new PostingMergerMock(80, 5));
            bool result = trigger->MatchAdaptiveBitmap(ptr);
            INDEXLIB_TEST_TRUE(!result);
        }

        {
            // bitmap size smaller
            PostingMergerPtr ptr(new PostingMergerMock(80, 50));
            bool result = trigger->MatchAdaptiveBitmap(ptr);
            INDEXLIB_TEST_TRUE(result);

            // key is in dict config
            result = trigger->NeedGenerateAdaptiveBitmap(index::DictKeyInfo(key), ptr);
            INDEXLIB_TEST_TRUE(!result);
        }
    }

    void TestCaseForPercentAdaptiveBitmapTrigger()
    {
        ReclaimMapPtr ptr(new ReclaimMapMock(80));
        AdaptiveBitmapTriggerCreator creator(ptr);
        SingleFieldIndexConfigPtr indexConfig(new SingleFieldIndexConfig("phrase", it_text));
        std::shared_ptr<AdaptiveDictionaryConfig> adaptiveDict = CreateAdaptiveDictConfig("dict", "PERCENT", 20);
        std::shared_ptr<DictionaryConfig> dict = CreateDictConfig(false);
        indexConfig->SetFieldConfig(FieldConfigPtr(new FieldConfig("f1", ft_text, false)));
        indexConfig->SetDictConfig(dict);
        indexConfig->SetAdaptiveDictConfig(adaptiveDict);
        dictkey_t key = autil::HashAlgorithm::hashString64(TOP_TERM);
        AdaptiveBitmapTriggerPtr trigger = creator.Create(indexConfig);
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
            result = trigger->NeedGenerateAdaptiveBitmap(index::DictKeyInfo(key), ptr);
            INDEXLIB_TEST_TRUE(!result);
        }
    }

    void TestCaseForDfAdaptiveBitmapTrigger()
    {
        ReclaimMapPtr ptr(new ReclaimMapMock(80));
        AdaptiveBitmapTriggerCreator creator(ptr);
        SingleFieldIndexConfigPtr indexConfig(new SingleFieldIndexConfig("phrase", it_text));
        std::shared_ptr<AdaptiveDictionaryConfig> adaptiveDict = CreateAdaptiveDictConfig("dict", "DOC_FREQUENCY", 20);
        std::shared_ptr<DictionaryConfig> dict = CreateDictConfig(false);
        indexConfig->SetFieldConfig(FieldConfigPtr(new FieldConfig("f1", ft_text, false)));
        indexConfig->SetDictConfig(dict);
        indexConfig->SetAdaptiveDictConfig(adaptiveDict);
        dictkey_t key = autil::HashAlgorithm::hashString64(TOP_TERM);
        AdaptiveBitmapTriggerPtr trigger = creator.Create(indexConfig);
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
            result = trigger->NeedGenerateAdaptiveBitmap(index::DictKeyInfo(key), ptr);
            INDEXLIB_TEST_TRUE(!result);
        }
    }

private:
    std::shared_ptr<DictionaryConfig> CreateDictConfig(bool isAdaptiveDict)
    {
        std::shared_ptr<DictionaryConfig> ptr(new DictionaryConfig("dicName", TOP_TERM));
        return ptr;
    }

    std::shared_ptr<AdaptiveDictionaryConfig> CreateAdaptiveDictConfig(string ruleName, string type, int32_t threshold)
    {
        std::shared_ptr<AdaptiveDictionaryConfig> adaptiveDict(new AdaptiveDictionaryConfig(ruleName, type, threshold));
        return adaptiveDict;
    }

public:
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, AdaptiveBitmapTriggerCreatorTest);

INDEXLIB_UNIT_TEST_CASE(AdaptiveBitmapTriggerCreatorTest, TestCaseForCreate);
INDEXLIB_UNIT_TEST_CASE(AdaptiveBitmapTriggerCreatorTest, TestCaseForLayerHash);
INDEXLIB_UNIT_TEST_CASE(AdaptiveBitmapTriggerCreatorTest, TestCaseForIndexSizeAdaptiveBitmapTrigger);
INDEXLIB_UNIT_TEST_CASE(AdaptiveBitmapTriggerCreatorTest, TestCaseForPercentAdaptiveBitmapTrigger);
INDEXLIB_UNIT_TEST_CASE(AdaptiveBitmapTriggerCreatorTest, TestCaseForDfAdaptiveBitmapTrigger);
}}} // namespace indexlib::index::legacy
