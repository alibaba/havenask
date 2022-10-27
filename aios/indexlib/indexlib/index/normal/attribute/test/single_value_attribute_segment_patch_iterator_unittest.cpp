#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_patch_iterator.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);

class SingleValueAttributeSegmentPatchIteratorTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH() + "/";
        srand(8888);
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForNext()
    {
        map<docid_t, uint32_t> expectedPatchData;
        uint32_t patchNum = 3;
        vector<pair<string, segmentid_t> >patchFileVect;
        CreatePatchData(patchNum, expectedPatchData, patchFileVect);

        FieldConfigPtr fieldConfig(new FieldConfig("abc", ft_int32, false));
        AttributeConfigPtr attrConfig(new AttributeConfig);
        attrConfig->Init(fieldConfig);
        SingleValueAttributeSegmentPatchIterator<uint32_t> it(attrConfig);
        it.Init(patchFileVect);
        CheckIterator(it, expectedPatchData);
     }

 private:
     void CreatePatchData(uint32_t patchNum, 
                          map<docid_t, uint32_t>& expectedPatchData, 
                          vector<pair<string, segmentid_t> >& patchFileVect)
     {
         IndexTestUtil::ResetDir(mDir);
         for (uint32_t i = 0; i < patchNum; ++i)
         {
             string patchFileName;
             CreateOnePatchData(i, expectedPatchData, patchFileName);
             patchFileVect.push_back(make_pair(patchFileName, i));
         }
     }

     void CreateOnePatchData(uint32_t patchId, 
                             map<docid_t, uint32_t>& expectedPatchData,
                             string& patchFileName)
     {
         map<docid_t, uint32_t> patchData;        
         for (uint32_t i = 0; i < MAX_DOC_COUNT; ++i)
         {
             docid_t docId = rand() % MAX_DOC_COUNT;
             uint32_t value = rand() % 10000;
             patchData[docId] = value;
             expectedPatchData[docId] = value;
         }

         patchFileName = mDir + StringUtil::toString<uint32_t>(patchId);
         unique_ptr<File> file(FileSystem::openFile(patchFileName.c_str(), WRITE));         

         map<docid_t, uint32_t>::const_iterator it = patchData.begin();
         for ( ; it != patchData.end(); ++it)
         {
             file->write((void*)(&(it->first)), sizeof(docid_t));
             file->write((void*)(&(it->second)), sizeof(uint32_t));
         }
         file->close();
     }

    void CheckIterator(SingleValueAttributeSegmentPatchIterator<uint32_t>& it,
                       map<docid_t, uint32_t>& expectedPatchData)
     {
         docid_t docId;
         uint32_t value;

         map<docid_t, uint32_t>::const_iterator mapIt = expectedPatchData.begin();
         for (; mapIt != expectedPatchData.end(); ++mapIt)
         {
             INDEXLIB_TEST_TRUE(it.Next(docId, value));
             INDEXLIB_TEST_EQUAL(mapIt->first, docId);
             INDEXLIB_TEST_EQUAL(mapIt->second, value);
        }
    }

private:
    static const uint32_t MAX_DOC_COUNT = 100;
    string mDir;
};

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentPatchIteratorTest, TestCaseForNext);

IE_NAMESPACE_END(index);
