#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_filter_processor.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/index/normal/inverted_index/truncate/test/fake_truncate_attribute_reader.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(index);

class DocFilterProcessorTest : public INDEXLIB_TESTBASE
{
    typedef TruncateMetaReader::DictType DictType;
public:
    DECLARE_CLASS_NAME(DocFilterProcessorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    template<typename T>
    void InnerTestForIsFiltered(FieldType fieldType,
                                int64_t minValue,
                                int64_t maxValue,
                                int64_t value,
                                bool expect)
    {
        config::DiversityConstrain constrain;
        constrain.SetFilterMinValue(minValue);
        constrain.SetFilterMaxValue(maxValue);

        std::stringstream ss;
        std::string attrValue;
        ss<<value;
        ss>>attrValue;
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
    void InnerTestForFilteredByTruncateMeta(FieldType fieldType,
            const string &keyStr, const string &rangeStr,
            const string &toCheckKeyStr, const string &keyExistStr, 
            const string &toCheckValueStr, const string &valueFilteredStr)
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
        processor.SetTruncateMetaReader(metaReader);

        vector<dictkey_t> toCheckKeyVec;
        vector<string> keyExistVec;
        vector<vector<T> > toCheckValueVec;
        vector<vector<string> > valueFilteredVec;

        StringUtil::fromString(toCheckKeyStr, toCheckKeyVec, ";");
        StringUtil::fromString(keyExistStr, keyExistVec, ";");
        StringUtil::fromString(toCheckValueStr, toCheckValueVec, ",", ";");
        StringUtil::fromString(valueFilteredStr, valueFilteredVec, ",", ";");

        assert(toCheckKeyVec.size() == keyExistVec.size() && 
               keyExistVec.size() == toCheckValueVec.size() && 
               toCheckValueVec.size() && 
               valueFilteredVec.size());

        for (size_t i = 0; i < toCheckKeyVec.size(); i++)
        {
            bool ret = processor.BeginFilter(toCheckKeyVec[i], PostingIteratorPtr());
            if (keyExistVec[i] == "false")
            {
                INDEXLIB_TEST_TRUE(!ret);
                continue;
            }
            INDEXLIB_TEST_TRUE(ret);
            // const vector<T> &values = toCheckValueVec[i];
            pAttrReader->clear();
            pAttrReader->SetAttrValue(toCheckValueVec[i]);
            const vector<string> &filteredVec = valueFilteredVec[i];
            for (size_t j = 0; j < toCheckValueVec[i].size(); j++)
            {
                // refer->Set(values[j], docinfo);
                // bool isFiltered = processor.IsFiltered(docinfo);
                bool isFiltered = processor.IsFiltered(j);
                if (filteredVec[j] == "true")
                {
                    INDEXLIB_TEST_TRUE(isFiltered);
                }
                else
                {
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
        string toCheckValueStr = "0,9,101;1,2;0,1,10,102";
        string valueFilteredStr = 
            "true,false,false;true,true;true,true,false,false";

        InnerTestForFilteredByTruncateMeta<int8_t>(ft_int8, 
                keyStr, rangeStr, toCheckKeyStr, keyExistStr, 
                toCheckValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<uint8_t>(ft_uint8, 
                keyStr, rangeStr, toCheckKeyStr, keyExistStr, 
                toCheckValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<int16_t>(ft_int16, 
                keyStr, rangeStr, toCheckKeyStr, keyExistStr, 
                toCheckValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<uint16_t>(ft_uint16, 
                keyStr, rangeStr, toCheckKeyStr, keyExistStr, 
                toCheckValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<int32_t>(ft_int32, 
                keyStr, rangeStr, toCheckKeyStr, keyExistStr, 
                toCheckValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<uint32_t>(ft_uint32, 
                keyStr, rangeStr, toCheckKeyStr, keyExistStr, 
                toCheckValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<int64_t>(ft_int64, 
                keyStr, rangeStr, toCheckKeyStr, keyExistStr, 
                toCheckValueStr, valueFilteredStr);
        InnerTestForFilteredByTruncateMeta<uint64_t>(ft_uint64, 
                keyStr, rangeStr, toCheckKeyStr, keyExistStr, 
                toCheckValueStr, valueFilteredStr);
    }

    // key str: key1;key2;key3;...
    // range str: start,end;start,end;...
    void CreateDict(DictType &dict, const string &keyStr, 
                    const string &rangeStr)
    {
        vector<dictkey_t> keyVec;
        vector<vector<int64_t> > rangeVec;

        StringUtil::fromString(keyStr, keyVec, ";");
        StringUtil::fromString(rangeStr, rangeVec, ",", ";");

        assert(keyVec.size() == rangeVec.size());
        for (size_t i = 0; i < keyVec.size(); i++)
        {
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

IE_NAMESPACE_END(index);
