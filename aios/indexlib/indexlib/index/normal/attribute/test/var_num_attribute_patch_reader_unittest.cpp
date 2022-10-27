#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/util/path_util.h"
#include "indexlib/config/attribute_config.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);

class VarNumAttributePatchReaderTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
        srand(8888);
    }

    void CaseTearDown() override
    {
    }

    void TestCaseSimpleForNextUint32()
    {
        map<docid_t, vector<uint32_t> > expectedPatchData;
        vector<uint32_t>& valueVector = expectedPatchData[0];
        valueVector.push_back(1000);
        valueVector.push_back(2000);
        string patchFilePath; 
        CreatePatchFile<uint32_t>(0, expectedPatchData, patchFilePath);

        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_uint32, true));
        mAttrConfig.reset(new AttributeConfig);
        mAttrConfig->Init(fieldConfig);

        VarNumAttributePatchReader<uint32_t> reader(mAttrConfig);
        DirectoryPtr patchDirectory;
        string patchFileName;
        GetPatchFileInfos(patchFilePath,
                          patchDirectory, patchFileName);
        reader.AddPatchFile(patchDirectory, patchFileName, 0);
        CheckReader<uint32_t>(reader, expectedPatchData);
    }
    
    void TestCaseSimpleForNextUint64()
    {
        map<docid_t, vector<uint64_t> > expectedPatchData;
        vector<uint64_t>& valueVector = expectedPatchData[0];
        valueVector.push_back(567878);
        valueVector.push_back(3435345);
        string patchFilePath; 
        CreatePatchFile<uint64_t>(0, expectedPatchData, patchFilePath);

        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_uint64, true));
        mAttrConfig.reset(new AttributeConfig);
        mAttrConfig->Init(fieldConfig);

        VarNumAttributePatchReader<uint64_t> reader(mAttrConfig);

        DirectoryPtr patchDirectory;
        string patchFileName;
        GetPatchFileInfos(patchFilePath,
                          patchDirectory, patchFileName);
        reader.AddPatchFile(patchDirectory, patchFileName, 0);

        CheckReader<uint64_t>(reader, expectedPatchData);
    }
    void TestCaseSimpleForNextFloat()
    {
        map<docid_t, vector<float> > expectedPatchData;
        vector<float>& valueVector = expectedPatchData[0];
        valueVector.push_back(10.01);
        valueVector.push_back(20.1);
        string patchFilePath; 
        CreatePatchFile<float>(0, expectedPatchData, patchFilePath);

        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_float, true));
        mAttrConfig.reset(new AttributeConfig);
        mAttrConfig->Init(fieldConfig);

        VarNumAttributePatchReader<float> reader(mAttrConfig);

        DirectoryPtr patchDirectory;
        string patchFileName;
        GetPatchFileInfos(patchFilePath,
                          patchDirectory, patchFileName);
        reader.AddPatchFile(patchDirectory, patchFileName, 0);

        CheckReader<float>(reader, expectedPatchData);
    }

    void TestCaseRandomForNextUint32()
    {
        map<docid_t, vector<uint32_t> > expectedPatchData;
        uint32_t patchFileNum = 2;
        vector<pair<string, segmentid_t> > patchFileVect;
        CreatePatchRandomData<uint32_t>(patchFileNum, expectedPatchData, patchFileVect);

        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_uint32, true));
        mAttrConfig.reset(new AttributeConfig);
        mAttrConfig->Init(fieldConfig);

        VarNumAttributePatchReader<uint32_t> reader(mAttrConfig);
        for (vector<pair<string, segmentid_t> >::iterator iter = patchFileVect.begin();
             iter != patchFileVect.end(); ++iter)
        {
            DirectoryPtr patchDirectory;
            string patchFileName;
            GetPatchFileInfos((*iter).first,
                    patchDirectory, patchFileName);
            reader.AddPatchFile(patchDirectory, patchFileName, (*iter).second);
        }
        CheckReader<uint32_t>(reader, expectedPatchData);
    }
    
    void TestCaseRandomForNextUint64()
    {
        map<docid_t, vector<uint64_t> > expectedPatchData;
        uint32_t patchFileNum = 2;
        vector<pair<string, segmentid_t> > patchFileVect;
        CreatePatchRandomData<uint64_t>(patchFileNum, expectedPatchData, patchFileVect);

        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_uint64, true));
        mAttrConfig.reset(new AttributeConfig);
        mAttrConfig->Init(fieldConfig);

        VarNumAttributePatchReader<uint64_t> reader(mAttrConfig);
        for (vector<pair<string, segmentid_t> >::iterator iter = patchFileVect.begin();
             iter != patchFileVect.end(); ++iter)
        {
            DirectoryPtr patchDirectory;
            string patchFileName;
            GetPatchFileInfos((*iter).first,
                    patchDirectory, patchFileName);
            reader.AddPatchFile(patchDirectory, patchFileName, (*iter).second);
        }
        CheckReader<uint64_t>(reader, expectedPatchData);
    }

    void TestCaseForSkipReadUint16()
    {
        map<docid_t, vector<uint16_t> > expectedPatchData;
        vector<uint16_t>& valueVector0 = expectedPatchData[0];
        valueVector0.push_back(1000);
        valueVector0.push_back(2000);
        vector<uint16_t>& valueVector2 = expectedPatchData[2];
        valueVector2.push_back(11);
        string patchFilePath; 
        CreatePatchFile<uint16_t>(0, expectedPatchData, patchFilePath);

        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_uint16, true));
        mAttrConfig.reset(new AttributeConfig);
        mAttrConfig->Init(fieldConfig);
        
        VarNumAttributePatchReader<uint16_t> reader(mAttrConfig);
        DirectoryPtr patchDirectory;
        string patchFileName;
        GetPatchFileInfos(patchFilePath,
                          patchDirectory, patchFileName);
        reader.AddPatchFile(patchDirectory, patchFileName, 0);

        size_t buffLen = sizeof(uint32_t) * 4 + sizeof(uint16_t);
        uint8_t buff[buffLen];
        const size_t buffSize = sizeof(uint8_t) * buffLen;
        EXPECT_EQ((size_t)0, reader.Seek(1, (uint8_t*)buff, buffSize));
        EXPECT_EQ((size_t)3, reader.Seek(2, (uint8_t*)buff, buffSize));
        
        size_t encodeCountLen = 0;
        uint32_t recordNum = VarNumAttributeFormatter::DecodeCount(
                (const char*)buff, encodeCountLen);
        uint16_t *value = (uint16_t *)(buff + encodeCountLen);
        EXPECT_EQ(valueVector2.size(), (size_t)recordNum);
        EXPECT_EQ((uint16_t)11, value[0]);
    }

private:
    
    template<typename T>
    void CreatePatchFile(uint32_t patchId,
                         map<docid_t, vector<T> >& expectedPatchData,
                         string& patchFilePath)
    {
        patchFilePath = mRootDir + StringUtil::toString<T>(patchId);
        unique_ptr<File> file(FileSystem::openFile(patchFilePath.c_str(), WRITE));         

        uint32_t patchCount = expectedPatchData.size();
        uint32_t maxPatchLen = 0;
        typename map<docid_t, vector<T> >::const_iterator it;
        for (it = expectedPatchData.begin(); it != expectedPatchData.end(); ++it)
        {
            file->write((void*)(&(it->first)), sizeof(docid_t));
            char countBuffer[4];
            size_t encodeLen = VarNumAttributeFormatter::EncodeCount(
                    it->second.size(), countBuffer, 4);
            file->write(countBuffer, encodeLen);

            const vector<T>& valueVector = it->second;
            for (size_t i = 0; i < valueVector.size(); ++i)
            {
                file->write((void*)(&(valueVector[i])), sizeof(T));
            }
            uint32_t patchLen = VarNumAttributeFormatter::GetEncodedCountLength(
                    it->second.size()) + it->second.size() * sizeof(T);
            maxPatchLen = std::max(maxPatchLen, patchLen);
        }

        file->write(&patchCount, sizeof(uint32_t));
        file->write(&maxPatchLen, sizeof(uint32_t));
        file->close();
    }


    template<typename T>
    void CreatePatchRandomData(uint32_t patchFileNum, 
                               map<docid_t, vector<T> >& expectedPatchData, 
                               vector<pair<string, segmentid_t> >& patchFileVect)
    {
        IndexTestUtil::ResetDir(mRootDir);
        for (uint32_t i = 0; i < patchFileNum; ++i)
        {
            string patchFilePath;
            CreateOnePatchRandomData<T>(i, expectedPatchData, patchFilePath);
            patchFileVect.push_back(make_pair(patchFilePath, i));
        }
    }
    template<typename T>
    void CreateOnePatchRandomData(uint32_t patchId, 
                                  map<docid_t, vector<T> >& expectedPatchData,
                                  string& patchFilePath)
    {
        map<docid_t, vector<T> > patchData;        
        for (uint32_t i = 0; i < MAX_DOC_COUNT; ++i)
        {
            docid_t docId = rand() % MAX_DOC_COUNT;
            uint32_t valueCount =rand() % MAX_VALUE_COUNT;
            vector<T> valueVector(valueCount);
            for (uint32_t j = 0; j < valueCount; ++j) 
            {
                T value = rand() % 10000;
                valueVector.push_back(value);
            }
            patchData[docId] = valueVector;
            expectedPatchData[docId] = valueVector;
        }
        CreatePatchFile<T>(patchId, expectedPatchData, patchFilePath);
    }

    template<typename T>
    void CheckReader(VarNumAttributePatchReader<T>& reader,
                     map<docid_t, vector<T> >& expectedPatchData)
    {
        docid_t docId;
        size_t buffLen = sizeof(uint32_t) + sizeof(T) * 65535;
        uint8_t *buff = new uint8_t[buffLen];

        typename map<docid_t, vector<T> >::const_iterator mapIt = expectedPatchData.begin();
        for (; mapIt != expectedPatchData.end(); ++mapIt)
        {
            size_t dataLen = reader.Next(docId, buff, buffLen);

            size_t encodeCountLen = 0;
            uint32_t recordNum = VarNumAttributeFormatter::DecodeCount(
                    (const char*)buff, encodeCountLen);
            EXPECT_EQ(recordNum, (dataLen - encodeCountLen) / sizeof(T));
            EXPECT_EQ(mapIt->first, docId);
            EXPECT_EQ((uint16_t)mapIt->second.size(), recordNum);
            T *value = (T *)(buff + encodeCountLen);
            for (size_t i = 0 ; i < recordNum; i++)
            {
                EXPECT_EQ((mapIt->second)[i], ((T*)value)[i]);
            }
        }
        delete[] buff;
    }

private:
    void GetPatchFileInfos(const string& patchFilePath,
                           DirectoryPtr& directory, string& fileName)
    {
        string path = PathUtil::GetParentDirPath(patchFilePath);
        directory = DirectoryCreator::Get(GET_FILE_SYSTEM(), path, true);
        fileName = PathUtil::GetFileName(patchFilePath);
    }

private:
    AttributeConfigPtr mAttrConfig;
    
private:
    static const uint32_t MAX_DOC_COUNT = 10;
    static const uint32_t MAX_VALUE_COUNT = 1000;
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchReaderTest, TestCaseSimpleForNextUint32);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchReaderTest, TestCaseSimpleForNextUint64);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchReaderTest, TestCaseSimpleForNextFloat);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchReaderTest, TestCaseRandomForNextUint32);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchReaderTest, TestCaseRandomForNextUint64);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchReaderTest, TestCaseForSkipReadUint16);
     
IE_NAMESPACE_END(index);
