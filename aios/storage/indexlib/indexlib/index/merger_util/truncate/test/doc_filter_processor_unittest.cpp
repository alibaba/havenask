#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/doc_filter_processor.h"
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/index/merger_util/truncate/test/fake_truncate_attribute_reader.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;

namespace indexlib::index::legacy {

class DocFilterProcessorTest : public INDEXLIB_TESTBASE
{
    typedef TruncateMetaReader::DictType DictType;

public:
    DECLARE_CLASS_NAME(DocFilterProcessorTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    template <typename T>
    void InnerTestForIsFiltered(FieldType fieldType, int64_t minValue, int64_t maxValue, int64_t value, bool expect)
    {
        config::DiversityConstrain constrain;
        constrain.SetFilterMinValue(minValue);
        constrain.SetFilterMaxValue(maxValue);

        std::stringstream ss;
        std::string attrValue;
        ss << value;
        ss >> attrValue;
        FakeTruncateAttributeReader<T>* pAttrReader = new FakeTruncateAttributeReader<T>();
        pAttrReader->SetAttrValue(attrValue);
        TruncateAttributeReaderPtr attrReader(pAttrReader);
        DocFilterProcessorTyped<T> processor(attrReader, constrain);

        INDEXLIB_TEST_TRUE(expect == processor.IsFiltered((docid_t)0));
    }

    void TestCaseForIsFiltered()
    {
        InnerTestForIsFiltered<int8_t>(ft_int8, 0, 100, 0, false);
        InnerTestForIsFiltered<int16_t>(ft_int16, 0, 100, 100, false);
        InnerTestForIsFiltered<int32_t>(ft_int32, -100, 100, -111, true);
        InnerTestForIsFiltered<int64_t>(ft_int64, -100, 100, 111, true);
    }

    // keyStr: key1;key2;key3;...
    // rangeStr: start,end;start,end;...
    // toCheckKeyStr: key1;key2;key3;...
    // keyExistStr: true;false;true;...
    // toCheckValueStr: 1,2,3;1,2,3;...
    // valueFilteredStr: true,false,false;...
    template <typename T>
    void InnerTestForFilteredByTruncateMeta(FieldType fieldType, const string& keyStr, const string& rangeStr,
                                            const string& toCheckKeyStr, const string& keyExistStr,
                                            const string& toCheckValueStr, const string& nullValueStr,
                                            const string& valueFilteredStr, bool testByThreshold = false)
    {
        DictType dict;
        CreateDict(dict, keyStr, rangeStr);

        TruncateMetaReaderPtr metaReader(new TruncateMetaReader(true));
        metaReader->SetDict(dict);

        // DocInfoAllocator allocator;
        // ReferenceTyped<T>* refer = allocator.DeclareReferenceTyped<T>("test", fieldType);
        // DocInfo *docinfo = allocator.Allocate();

        // config::DiversityConstrain constrain;
        // FilterProcessorTyped<T> processor(allocator.GetReference("test"), constrain);

        FakeTruncateAttributeReader<T>* pAttrReader = new FakeTruncateAttributeReader<T>();
        TruncateAttributeReaderPtr attrReader(pAttrReader);
        config::DiversityConstrain constrain;
        DocFilterProcessorTyped<T> processor(attrReader, constrain);
        if (!testByThreshold) {
            processor.SetTruncateMetaReader(metaReader);
        }

        vector<index::DictKeyInfo> toCheckKeyVec;
        vector<string> keyExistVec;
        vector<vector<T>> toCheckValueVec;
        vector<vector<bool>> nullVecs;
        vector<vector<string>> valueFilteredVec;

        vector<string> toCheckKeyStrs;
        StringUtil::fromString(toCheckKeyStr, toCheckKeyStrs, ";");
        for (auto& keyStr : toCheckKeyStrs) {
            index::DictKeyInfo tmp;
            tmp.FromString(keyStr);
            toCheckKeyVec.push_back(tmp);
        }

        StringUtil::fromString(keyExistStr, keyExistVec, ";");
        StringUtil::fromString(toCheckValueStr, toCheckValueVec, ",", ";");
        StringUtil::fromString(valueFilteredStr, valueFilteredVec, ",", ";");
        StringUtil::fromString(nullValueStr, nullVecs, ",", ";");

        assert(toCheckKeyVec.size() == keyExistVec.size() && keyExistVec.size() == toCheckValueVec.size() &&
               toCheckValueVec.size() && valueFilteredVec.size());

        for (size_t i = 0; i < toCheckKeyVec.size(); i++) {
            bool ret = false;
            if (testByThreshold) {
                auto iter = dict.find(toCheckKeyVec[i]);
                if (iter != dict.end()) {
                    processor.BeginFilter(/*minValue*/ iter->second.first, /*maxValue*/ iter->second.second);
                    ret = true;
                }
            } else {
                ret = processor.BeginFilter(toCheckKeyVec[i], std::shared_ptr<PostingIterator>());
            }
            if (keyExistVec[i] == "false") {
                INDEXLIB_TEST_TRUE(!ret);
                continue;
            }
            INDEXLIB_TEST_TRUE(ret);
            // const vector<T> &values = toCheckValueVec[i];
            pAttrReader->clear();
            pAttrReader->SetAttrValue(toCheckValueVec[i], nullVecs[i]);
            const vector<string>& filteredVec = valueFilteredVec[i];
            for (size_t j = 0; j < toCheckValueVec[i].size(); j++) {
                // refer->Set(values[j], docinfo);
                // bool isFiltered = processor.IsFiltered(docinfo);
                bool isFiltered = processor.IsFiltered(j);
                if (filteredVec[j] == "true") {
                    INDEXLIB_TEST_TRUE(isFiltered);
                } else {
                    INDEXLIB_TEST_TRUE(!isFiltered);
                }
            }
        }

        // allocator.Deallocate(docinfo);
    }

    void TestCaseForFilteredByTruncateMeta()
    {
        string keyStr = "1;2;3";
        string rangeStr = "0,100;1,101;2,102";
        string toCheckKeyStr = "2;4;3";
        string keyExistStr = "true;false;true";
        string toCheckValueStr = "0,9,101,102;1,2;0,1,10,99,102";
        string nullValueStr = "false,false,false,true;false,false;false,false,false,true,false";
        string valueFilteredStr = "true,false,false,true;true,true;true,true,false,true,false";

        InnerTestForFilteredByTruncateMeta<int8_t>(ft_int8, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                   toCheckValueStr, nullValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<uint8_t>(ft_uint8, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                    toCheckValueStr, nullValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<int16_t>(ft_int16, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                    toCheckValueStr, nullValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<uint16_t>(ft_uint16, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                     toCheckValueStr, nullValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<int32_t>(ft_int32, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                    toCheckValueStr, nullValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<uint32_t>(ft_uint32, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                     toCheckValueStr, nullValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<int64_t>(ft_int64, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                    toCheckValueStr, nullValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<uint64_t>(ft_uint64, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                     toCheckValueStr, nullValueStr, valueFilteredStr);
    }

    void TestCaseForFilterByMetaThreshold()
    {
        string keyStr = "1;2;3";
        string rangeStr = "0,100;1,101;2,102";
        string toCheckKeyStr = "2;4;3";
        string keyExistStr = "true;false;true";
        string toCheckValueStr = "0,9,101,102;1,2;0,1,10,99,102";
        string nullValueStr = "false,false,false,true;false,false;false,false,false,true,false";
        string valueFilteredStr = "true,false,false,true;true,true;true,true,false,true,false";

        InnerTestForFilteredByTruncateMeta<int8_t>(ft_int8, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                   toCheckValueStr, nullValueStr, valueFilteredStr, true);
        InnerTestForFilteredByTruncateMeta<uint8_t>(ft_uint8, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                    toCheckValueStr, nullValueStr, valueFilteredStr, true);
        InnerTestForFilteredByTruncateMeta<int16_t>(ft_int16, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                    toCheckValueStr, nullValueStr, valueFilteredStr, true);
        InnerTestForFilteredByTruncateMeta<uint16_t>(ft_uint16, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                     toCheckValueStr, nullValueStr, valueFilteredStr, true);
        InnerTestForFilteredByTruncateMeta<int32_t>(ft_int32, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                    toCheckValueStr, nullValueStr, valueFilteredStr, true);
        InnerTestForFilteredByTruncateMeta<uint32_t>(ft_uint32, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                     toCheckValueStr, nullValueStr, valueFilteredStr, true);
        InnerTestForFilteredByTruncateMeta<int64_t>(ft_int64, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                    toCheckValueStr, nullValueStr, valueFilteredStr, true);
        InnerTestForFilteredByTruncateMeta<uint64_t>(ft_uint64, keyStr, rangeStr, toCheckKeyStr, keyExistStr,
                                                     toCheckValueStr, nullValueStr, valueFilteredStr, true);
    }

    // key str: key1;key2;key3;...
    // range str: start,end;start,end;...
    void CreateDict(DictType& dict, const string& keyStr, const string& rangeStr)
    {
        vector<index::DictKeyInfo> keyVec;
        vector<string> keyStrVec;
        StringUtil::fromString(keyStr, keyStrVec, ";");
        for (auto& keyStr : keyStrVec) {
            index::DictKeyInfo tmp;
            tmp.FromString(keyStr);
            keyVec.push_back(tmp);
        }

        vector<vector<int64_t>> rangeVec;
        StringUtil::fromString(rangeStr, rangeVec, ",", ";");
        assert(keyVec.size() == rangeVec.size());
        for (size_t i = 0; i < keyVec.size(); i++) {
            assert(rangeVec[i].size() == 2);
            dict[keyVec[i]] = make_pair(rangeVec[i][0], rangeVec[i][1]);
        }
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, DocFilterProcessorTest);

INDEXLIB_UNIT_TEST_CASE(DocFilterProcessorTest, TestCaseForIsFiltered);
INDEXLIB_UNIT_TEST_CASE(DocFilterProcessorTest, TestCaseForFilteredByTruncateMeta);
INDEXLIB_UNIT_TEST_CASE(DocFilterProcessorTest, TestCaseForFilterByMetaThreshold);
} // namespace indexlib::index::legacy
