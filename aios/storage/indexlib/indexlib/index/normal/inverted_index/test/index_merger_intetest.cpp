#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/inverted_index/OnDiskIndexIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingDumper.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/index/inverted_index/test/IndexIteratorMock.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::test;

using namespace indexlib::index_base;
using namespace indexlib::merger;

namespace indexlib { namespace index {

class IndexMergerInteTest : public INDEXLIB_TESTBASE
{
public:
    typedef set<dictkey_t> KeySet;

public:
    IndexMergerInteTest() {}

    ~IndexMergerInteTest() {}

    DECLARE_CLASS_NAME(IndexMergerInteTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    string PrepareEmptySegmentDoc(int32_t firstSegmentDocIdx)
    {
        stringstream ss;
        ss << "delete" << firstSegmentDocIdx;
        return ss.str();
    }

    string PrepareSegmentDoc(segmentid_t segId, size_t termCount, KeySet& keySet)
    {
        stringstream ss;
        for (uint32_t i = 0; i < termCount; i++) {
            dictkey_t key = i * (segId + 1);
            ss << key;
            if (i < termCount - 1) {
                ss << ",";
            }
            keySet.insert(key);
        }
        return ss.str();
    }

    void InnerTestMerge(const string& segDocCounts, const string& mergePlan, bool enableNull)
    {
        tearDown();
        setUp();
        vector<int32_t> docCounts;
        StringUtil::fromString(segDocCounts, docCounts, ";");
        stringstream allDocs;
        allDocs << "1,2,3,4,5,6,7,8,9,10#";
        int32_t deleteDoc = 10;
        KeySet keys;
        vector<size_t> mergeSegments;
        StringUtil::fromString(mergePlan, mergeSegments, ",");
        for (size_t i = 0; i < docCounts.size(); i++) {
            if (docCounts[i] == 0) {
                ASSERT_TRUE(deleteDoc > 1);
                allDocs << PrepareEmptySegmentDoc(deleteDoc--);
            } else {
                if (find(mergeSegments.begin(), mergeSegments.end(), i + 1) != mergeSegments.end()) {
                    if (enableNull && i == docCounts.size() - 1) {
                        allDocs << "__NULL__";
                    } else {
                        allDocs << PrepareSegmentDoc(i + 1, docCounts[i], keys);
                    }
                } else {
                    KeySet tmpKeys;
                    if (enableNull && i == docCounts.size() - 1) {
                        allDocs << "__NULL__";
                    } else {
                        allDocs << PrepareSegmentDoc(i + 1, docCounts[i], tmpKeys);
                    }
                }
            }
            if (i < docCounts.size() - 1) {
                allDocs << "#";
            }
        }
        stringstream content;
        content << "__NULL__;";
        auto iter = keys.begin();
        for (; iter != keys.end(); iter++) {
            content << *iter << ";";
        }
        std::shared_ptr<DictionaryConfig> dict(new DictionaryConfig("tf", content.str()));

        SingleFieldPartitionDataProvider provider;
        provider.Init(GET_TEMP_DATA_PATH(), "int64", SFP_INDEX, enableNull);
        auto schema = provider.GetSchema();
        schema->AddDictionaryConfig("tf", content.str());
        auto indexConfig = provider.GetIndexConfig();
        indexConfig->SetDictConfig(dict);
        indexConfig->SetHighFreqencyTermPostingType(hp_both);
        ASSERT_TRUE(provider.Build(allDocs.str(), SFP_OFFLINE));
        provider.Merge(mergePlan);
        GET_FILE_SYSTEM()->TEST_MountLastVersion();
        DirectoryPtr mergedSegmentDir = GET_PARTITION_DIRECTORY()->GetDirectory(
            "segment_" + StringUtil::toString(docCounts.size() + 1) + "_level_0", true);

        DirectoryPtr dictDir =
            mergedSegmentDir->GetDirectory("index/" + provider.GetIndexConfig()->GetIndexName() + "/", true);
        CheckMergeResult(indexConfig, dictDir, keys, false, enableNull);
        CheckMergeResult(indexConfig, dictDir, keys, true, enableNull);
    }

    void TestForMergeAll()
    {
        // begin from segment 1
        string segmentInfoStr = "20;3000;0;0;3000";
        string mergePlanStr = "1,2,3,5";
        InnerTestMerge(segmentInfoStr, mergePlanStr, false);
        InnerTestMerge(segmentInfoStr, mergePlanStr, true);
    }

    void TestForMergeWithEmptySegment()
    {
        string segmentInfoStr = "20;3000;0;0;3000;0";
        string mergePlanStr = "1;2;3;5;6";
        InnerTestMerge(segmentInfoStr, mergePlanStr, false);
        InnerTestMerge(segmentInfoStr, mergePlanStr, true);
    }

    void TestForMergeSomeSegments()
    {
        string segmentInfoStr = "20;3000;0;0;3000";
        string mergePlanStr = "1;5";
        InnerTestMerge(segmentInfoStr, mergePlanStr, false);
        InnerTestMerge(segmentInfoStr, mergePlanStr, true);
    }

private:
    void CheckMergeResult(const config::IndexConfigPtr& indexConfig, const DirectoryPtr& dictDirectory,
                          const KeySet& keyAnswers, bool isBitmap, bool enableNull)
    {
        string fileName;
        if (isBitmap) {
            fileName = BITMAP_DICTIONARY_FILE_NAME;
        } else {
            fileName = DICTIONARY_FILE_NAME;
        }

        std::shared_ptr<DictionaryReader> dictReader;
        if (isBitmap) {
            dictReader.reset(new TieredDictionaryReader);
        } else {
            dictReader.reset(DictionaryCreator::CreateReader(indexConfig));
        }
        ASSERT_TRUE(dictReader->Open(dictDirectory, fileName, false).IsOK());

        std::shared_ptr<DictionaryIterator> keyIter = dictReader->CreateIterator();
        index::DictKeyInfo key;
        dictvalue_t value;
        KeySet::const_iterator answerIter = keyAnswers.begin();

        while (keyIter->HasNext()) {
            keyIter->Next(key, value);
            if (answerIter != keyAnswers.end()) {
                INDEXLIB_TEST_EQUAL(*answerIter, key.GetKey());
                ASSERT_FALSE(key.IsNull());
                answerIter++;
            } else {
                ASSERT_TRUE(key.IsNull());
            }
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
}} // namespace indexlib::index
