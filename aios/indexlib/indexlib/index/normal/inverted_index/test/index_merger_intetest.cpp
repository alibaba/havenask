#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_dumper_impl.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_dumper.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/index/normal/inverted_index/test/index_iterator_mock.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(merger);

IE_NAMESPACE_BEGIN(index);

class IndexMergerInteTest : public INDEXLIB_TESTBASE
{
public:
    typedef set<dictkey_t> KeySet;

public:
    IndexMergerInteTest()
    {
    }

    ~IndexMergerInteTest()
    {
    }

    DECLARE_CLASS_NAME(IndexMergerInteTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    string PrepareEmptySegmentDoc(int32_t firstSegmentDocIdx)
    {
        stringstream ss;
        ss << "delete" << firstSegmentDocIdx;
        return ss.str();
    }

    string PrepareSegmentDoc(segmentid_t segId, size_t termCount, KeySet& keySet)
    {
        stringstream ss;
        for (uint32_t i = 0; i < termCount; i++)
        {
            dictkey_t key = i * (segId + 1);
            ss << key;
            if (i < termCount - 1)
            {
                ss << ",";
            }
            keySet.insert(key);
        }
        return ss.str();
    }

    void InnerTestMerge(const string& segDocCounts, const string& mergePlan)
    {
        TearDown();
        SetUp();
        vector<int32_t> docCounts;
        StringUtil::fromString(segDocCounts, docCounts, ";");
        stringstream allDocs;
        allDocs << "1,2,3,4,5,6,7,8,9,10#";
        int32_t deleteDoc = 10;
        KeySet keys;
        vector<size_t> mergeSegments;
        StringUtil::fromString(mergePlan, mergeSegments, ",");
        for (size_t i = 0; i < docCounts.size(); i++)
        {
            if (docCounts[i] == 0)
            {
                ASSERT_TRUE(deleteDoc > 1);
                allDocs << PrepareEmptySegmentDoc(deleteDoc--);
            }
            else
            {
                if (find(mergeSegments.begin(), mergeSegments.end(), i + 1) != mergeSegments.end())
                {
                    allDocs << PrepareSegmentDoc(i + 1, docCounts[i], keys);
                }
                else
                {
                    KeySet tmpKeys;
                    allDocs << PrepareSegmentDoc(i + 1, docCounts[i], tmpKeys);
                }
            }
            if (i < docCounts.size() - 1)
            {
                allDocs << "#";
            }
        }
        stringstream content;
        auto iter = keys.begin();
        for(; iter != keys.end(); iter++)
        {
            content << *iter << ";";
        }
        DictionaryConfigPtr dict(new DictionaryConfig("tf", content.str()));

        SingleFieldPartitionDataProvider provider;
        provider.Init(GET_TEST_DATA_PATH(), "int64", SFP_INDEX);
        auto schema = provider.GetSchema();
        schema->AddDictionaryConfig("tf", content.str());
        auto indexConfig = provider.GetIndexConfig();
        indexConfig->SetDictConfig(dict);
        indexConfig->SetHighFreqencyTermPostingType(hp_both);
        ASSERT_TRUE(provider.Build(allDocs.str(), SFP_OFFLINE));
        provider.Merge(mergePlan);
        stringstream mergedSegmentPath;
        mergedSegmentPath << GET_TEST_DATA_PATH()
                         << "/segment_" << docCounts.size() + 1 << "_level_0";
        DirectoryPtr mergedSegmentDir = DirectoryCreator::Create(mergedSegmentPath.str());
        mergedSegmentDir->MountPackageFile(PACKAGE_FILE_PREFIX);
        DirectoryPtr dictDir = mergedSegmentDir->GetDirectory(
            "/index/" + provider.GetIndexConfig()->GetIndexName() + "/", true);
        CheckMergeResult(indexConfig, dictDir, keys, false);
        CheckMergeResult(indexConfig, dictDir, keys, true);
    }
    
    void TestForMergeAll()
    {
        //begin from segment 1
        string segmentInfoStr = "20;3000;0;0;3000";
        string mergePlanStr = "1,2,3,5";
        InnerTestMerge(segmentInfoStr, mergePlanStr);
    }

    void TestForMergeWithEmptySegment()
    {
        string segmentInfoStr = "20;3000;0;0;3000;0";
        string mergePlanStr = "1;2;3;5;6";
        InnerTestMerge(segmentInfoStr, mergePlanStr);
    }

    void TestForMergeSomeSegments()
    {
        string segmentInfoStr = "20;3000;0;0;3000";
        string mergePlanStr = "1;5";
        InnerTestMerge(segmentInfoStr, mergePlanStr);
    }


private:
    void CheckMergeResult(const config::IndexConfigPtr& indexConfig,
                          const DirectoryPtr& dictDirectory, const KeySet& keyAnswers,
                          bool isBitmap = false)
    {
        string fileName;
        if (isBitmap)
        {
            fileName = BITMAP_DICTIONARY_FILE_NAME;
        }
        else
        {
            fileName = DICTIONARY_FILE_NAME;
        }

        DictionaryReaderPtr dictReader;
        if (isBitmap)
        {
            dictReader.reset(new TieredDictionaryReader);
        }
        else
        {
            dictReader.reset(DictionaryCreator::CreateReader(indexConfig));
        }
        dictReader->Open(dictDirectory, fileName);
        
        DictionaryIteratorPtr keyIter = dictReader->CreateIterator();
        dictkey_t key;
        dictvalue_t value;
        KeySet::const_iterator answerIter = keyAnswers.begin();
        
        while (keyIter->HasNext())
        {
            keyIter->Next(key, value);
            INDEXLIB_TEST_EQUAL(*answerIter, key);
            answerIter++;
        }
        INDEXLIB_TEST_TRUE(answerIter == keyAnswers.end());
    }

private:
    string mIndexName;
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(IndexMergerInteTest, TestForMergeAll);
INDEXLIB_UNIT_TEST_CASE(IndexMergerInteTest, TestForMergeSomeSegments);
INDEXLIB_UNIT_TEST_CASE(IndexMergerInteTest, TestForMergeWithEmptySegment);

IE_NAMESPACE_END(index);
