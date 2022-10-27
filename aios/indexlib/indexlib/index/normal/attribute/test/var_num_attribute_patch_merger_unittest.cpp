#include <autil/StringUtil.h>
#include <fslib/fslib.h>
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_merger.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/fp16_encoder.h"
#include "indexlib/util/block_fp_encoder.h"
#include "indexlib/util/float_int8_encoder.h"


using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);

class VarNumAttributePatchMergerTest : public INDEXLIB_TESTBASE
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


public:
    void TestCaseForMergeOnePatch()
    {
        InnerTestMerge(1);
    }

    void TestCaseForMergeMultiPatches()
    {
        InnerTestMerge(5);
    }

    void TestCaseForMergeFixedLength()
    {
        InnerTestMergeForFixLength(3, "");
        InnerTestMergeForFixLength(5, "fp16");
        InnerTestMergeForFixLength(2, "block_fp");
        InnerTestMergeForFixLength(4, "int8#127");
    }
    
private:

    void InnerTestMerge(uint32_t patchCount)
    {
        config::FieldConfigPtr fieldConfig(new config::FieldConfig("field", ft_uint32, true));
        config::AttributeConfigPtr attrConfig(new config::AttributeConfig);
        attrConfig->Init(fieldConfig);

        vector<pair<string, segmentid_t> > patchFiles;
        map<docid_t, vector<uint32_t> > expectedAnswer;
        CreatePatches(attrConfig, patchCount, patchFiles, expectedAnswer);
        
        VarNumAttributePatchMerger<uint32_t> merger(attrConfig);
        string destFile = mRootDir + "dest_data";
        merger.Merge(patchFiles, destFile);

        vector<pair<string, segmentid_t> >patchFileVect;
        patchFileVect.push_back(make_pair(destFile, patchCount));
        
        VarNumAttributePatchReader<uint32_t> reader(attrConfig);
        for (vector<pair<string, segmentid_t> >::iterator iter = patchFileVect.begin(); 
             iter != patchFileVect.end(); ++iter)
        {
            string path = PathUtil::GetParentDirPath(iter->first);
            DirectoryPtr directory = DirectoryCreator::Get(GET_FILE_SYSTEM(), path, true);
            string fileName = PathUtil::GetFileName(iter->first);
            reader.AddPatchFile(directory, fileName, iter->second);
        }
        Check(attrConfig, expectedAnswer, reader);
    }

    void InnerTestMergeForFixLength(uint32_t patchCount, const string& compressTypeStr)
    {
        TearDown();
        SetUp();
        
        config::AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig(
                ft_float, true, "attr_name", 0, compressTypeStr, 4);

        vector<pair<string, segmentid_t> > patchFiles;
        map<docid_t, vector<float> > expectedAnswer;
        CreatePatches(attrConfig, patchCount, patchFiles, expectedAnswer);
        
        VarNumAttributePatchMerger<float> merger(attrConfig);
        string destFile = mRootDir + "dest_data";
        merger.Merge(patchFiles, destFile);

        vector<pair<string, segmentid_t> >patchFileVect;
        patchFileVect.push_back(make_pair(destFile, patchCount));
        
        VarNumAttributePatchReader<float> reader(attrConfig);
        for (vector<pair<string, segmentid_t> >::iterator iter = patchFileVect.begin(); 
             iter != patchFileVect.end(); ++iter)
        {
            string path = PathUtil::GetParentDirPath(iter->first);
            DirectoryPtr directory = DirectoryCreator::Get(GET_FILE_SYSTEM(), path, true);
            string fileName = PathUtil::GetFileName(iter->first);
            reader.AddPatchFile(directory, fileName, iter->second);
        }
        Check(attrConfig, expectedAnswer, reader);
    }

    template <typename T>
    void CreatePatches(const config::AttributeConfigPtr& attrConfig,
                       uint32_t patchCount, 
                       vector<pair<string, segmentid_t> >& patchFiles, 
                       map<docid_t, vector<T> >& expectedAnswer)
    {
        for (uint32_t i = 0; i < patchCount; ++i)
        {
            string patchFile;
            CreateOnePatch(attrConfig, i, patchFile, expectedAnswer);
            patchFiles.push_back(make_pair(patchFile, i));
        }
    }

    template <typename T>
    void CreateOnePatch(const config::AttributeConfigPtr& attrConfig,
                        uint32_t patchId, string& patchFile, 
                        map<docid_t, vector<T> >& expectedAnswer)
    {
        for (uint32_t i = 0; i < MAX_DOC_COUNT; ++i)
        {
            docid_t docId = rand() % MAX_DOC_COUNT;
            uint16_t valueCount = attrConfig->IsLengthFixed() ?
                                  attrConfig->GetFieldConfig()->GetFixedMultiValueCount() :
                                  rand() % MAX_VALUE_COUNT;
            expectedAnswer[docId].clear();
            for (uint16_t j = 0; j < valueCount; ++j)
            {
                T value = T(rand() % 128);
                expectedAnswer[docId].push_back(value);
            }
        }

        patchFile = mRootDir + StringUtil::toString<uint32_t>(patchId);
        unique_ptr<File> file(FileSystem::openFile(patchFile.c_str(), WRITE));

        uint32_t patchCount = expectedAnswer.size();
        uint32_t maxPatchLen = 0;
        typename map<docid_t, vector<T> >::const_iterator it = expectedAnswer.begin();
        for ( ; it != expectedAnswer.end(); ++it)
        {
            uint16_t recordNum = it->second.size();
            file->write((void*)(&(it->first)), sizeof(docid_t));

            if (attrConfig->IsLengthFixed())
            {
                CompressTypeOption compressType = attrConfig->GetFieldConfig()->GetCompressType();
                int32_t encodeLen = 0;
                char buffer[recordNum * sizeof(T)];
                if (compressType.HasFp16EncodeCompress())
                {
                    encodeLen = Fp16Encoder::Encode((const float*)it->second.data(),
                            recordNum, buffer, recordNum * sizeof(float));
                }
                else if (compressType.HasBlockFpEncodeCompress())
                {
                    encodeLen = BlockFpEncoder::Encode((const float*)it->second.data(),
                            recordNum, buffer, recordNum * sizeof(float));
                }
                else if (compressType.HasInt8EncodeCompress())
                {
                    encodeLen = FloatInt8Encoder::Encode(compressType.GetInt8AbsMax(),
                            (const float*)it->second.data(), recordNum, buffer,
                            recordNum * sizeof(float));
                }
                else
                {
                    encodeLen = sizeof(T) * recordNum;
                    memcpy(buffer, it->second.data(), encodeLen);
                }
                file->write((void*)buffer, encodeLen);
                maxPatchLen = std::max((uint32_t)encodeLen, maxPatchLen);
            }
            else
            {
                char countBuffer[4];
                size_t encodeLen = VarNumAttributeFormatter::EncodeCount(
                        recordNum, countBuffer, 4);
                file->write(countBuffer, encodeLen);
                file->write((void*)it->second.data(), sizeof(T) * recordNum);

                maxPatchLen = std::max(maxPatchLen,
                        (uint32_t)(encodeLen + sizeof(T) * recordNum));
            }
        }

        file->write(&patchCount, sizeof(uint32_t));
        file->write(&maxPatchLen, sizeof(uint32_t));
        file->close();
    }

    template <typename T>
    void Check(const AttributeConfigPtr& attrConfig,
               map<docid_t, vector<T> >& expectedAnswer,
               VarNumAttributePatchReader<T>& reader)
    {
         size_t buffLen = sizeof(uint32_t) + sizeof(T) * VAR_NUM_ATTRIBUTE_MAX_COUNT;
         uint8_t buff[buffLen];
         typename map<docid_t, vector<T> >::const_iterator mapIt = expectedAnswer.begin();
         for (; mapIt != expectedAnswer.end(); ++mapIt)
         {
             size_t dataLen = reader.Seek(mapIt->first, buff, buffLen);
             ASSERT_TRUE(dataLen > 0) << "docId: " << mapIt->first;

             if (attrConfig->IsLengthFixed())
             {
                 size_t recordNum = attrConfig->GetFieldConfig()->GetFixedMultiValueCount();
                 CompressTypeOption compressType = attrConfig->GetFieldConfig()->GetCompressType();

                 char buffer[recordNum * sizeof(T)];
                 ConstString data((char*)buff, dataLen);
                 if (compressType.HasFp16EncodeCompress())
                 {
                     Fp16Encoder::Decode(data, buffer, recordNum * sizeof(float));
                 }
                 else if (compressType.HasBlockFpEncodeCompress())
                 {
                     BlockFpEncoder::Decode(data, buffer, recordNum * sizeof(float));
                 }
                 else if (compressType.HasInt8EncodeCompress())
                 {
                     FloatInt8Encoder::Decode(compressType.GetInt8AbsMax(),
                             data, buffer, recordNum * sizeof(float));
                 }
                 else
                 {
                     memcpy(buffer, buff, dataLen);
                 }
                 T* value = (T*)buffer;
                 ASSERT_EQ(mapIt->second.size(), recordNum);
                 for (uint16_t i = 0; i < recordNum; ++i)
                 {
                     ASSERT_EQ(mapIt->second[i], value[i]);
                 }
             }
             else
             {
                 size_t encodeCountLen = 0;
                 uint32_t recordNum = VarNumAttributeFormatter::DecodeCount(
                         (const char*)buff, encodeCountLen);

                 ASSERT_EQ(recordNum, (dataLen - encodeCountLen) / sizeof(T));
                 T* value = (T*)(buff + encodeCountLen);
                 ASSERT_EQ(mapIt->second.size(), recordNum);
                 for (uint16_t i = 0; i < recordNum; ++i)
                 {
                     ASSERT_EQ(mapIt->second[i], value[i]);
                 }
             }
        }
    }

private:
    static const uint32_t MAX_DOC_COUNT = 100;
    static const uint32_t MAX_VALUE_COUNT = 30;
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchMergerTest, TestCaseForMergeOnePatch);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchMergerTest, TestCaseForMergeMultiPatches);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchMergerTest, TestCaseForMergeFixedLength);

IE_NAMESPACE_END(index);

