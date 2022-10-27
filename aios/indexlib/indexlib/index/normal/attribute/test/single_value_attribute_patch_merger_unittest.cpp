#include <autil/StringUtil.h>
#include <fslib/fslib.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_merger.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_BEGIN(index);

class SingleValueAttributePatchMergerTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH() + "/";
        srand(8888);


        config::FieldConfigPtr fieldConfig(new config::FieldConfig("field", ft_uint32, false));
        mAttrConfig.reset(new config::AttributeConfig);
        mAttrConfig->Init(fieldConfig);
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForMergeOnePatch()
    {
        InnerTestMerge(1);
    }

    void TestCaseForMergeMultiPatches()
    {
        InnerTestMerge(5);
    }

private:

    void InnerTestMerge(uint32_t patchCount)
    {
        vector<pair<string, segmentid_t> > patchFiles;
        map<docid_t, uint32_t> answer;
        CreatePatches(patchCount, patchFiles, answer);

        SingleValueAttributePatchMerger<uint32_t> merger(mAttrConfig);
        string destFile = mDir + "dest_data";
        merger.Merge(patchFiles, destFile);

        vector<pair<string, segmentid_t> >patchFileVect;
        patchFileVect.push_back(make_pair(destFile, patchCount));
        SingleValueAttributeSegmentPatchIterator<uint32_t> it(mAttrConfig);
        it.Init(patchFileVect);

        Check(answer, it);
    }

    void CreatePatches(uint32_t patchCount, 
                       vector<pair<string, segmentid_t> >& patchFiles, 
                       map<docid_t, uint32_t>& answer)
    {
        for (uint32_t i = 0; i < patchCount; ++i)
        {
            string patchFile;
            CreateOnePatch(i, patchFile, answer);
            patchFiles.push_back(make_pair(patchFile, i));
        }
    }

    void CreateOnePatch(uint32_t patchId, string& patchFile, 
                       map<docid_t, uint32_t>& answer)
    {
        map<docid_t, uint32_t> patchData;
        for (uint32_t i = 0; i < MAX_DOC_COUNT; ++i)
        {
            docid_t docId = rand() % MAX_DOC_COUNT;
            uint32_t value = rand() % 10000;
            patchData[docId] = value;
            answer[docId] = value;
        }

        patchFile = mDir + StringUtil::toString<uint32_t>(patchId);
        unique_ptr<File> file(FileSystem::openFile(patchFile.c_str(), WRITE));

        map<docid_t, uint32_t>::const_iterator it = patchData.begin();
        for ( ; it != patchData.end(); ++it)
        {
            file->write((void*)(&(it->first)), sizeof(docid_t));
            file->write((void*)(&(it->second)), sizeof(uint32_t));
        }
        file->close();
    }

    void Check(map<docid_t, uint32_t>& answer,
               SingleValueAttributeSegmentPatchIterator<uint32_t>& it)
    {
         docid_t docId;
         uint32_t value;

         map<docid_t, uint32_t>::const_iterator mapIt = answer.begin();
         for (; mapIt != answer.end(); ++mapIt)
         {
             INDEXLIB_TEST_TRUE(it.Next(docId, value));
             INDEXLIB_TEST_EQUAL(mapIt->first, docId);
             INDEXLIB_TEST_EQUAL(mapIt->second, value);
        }
    }

private:
    static const uint32_t MAX_DOC_COUNT = 100;
    string mDir;
    config::AttributeConfigPtr mAttrConfig;
};

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributePatchMergerTest, TestCaseForMergeOnePatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributePatchMergerTest, TestCaseForMergeMultiPatches);

IE_NAMESPACE_END(index);
