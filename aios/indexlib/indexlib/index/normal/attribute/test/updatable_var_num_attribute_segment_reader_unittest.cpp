#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/util/path_util.h"
#include <autil/mem_pool/Pool.h>

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);

class UpdatableVarNumAttributeSegmentReaderTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
        mSeg0Directory = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForWriteRead()
    {
        TestWriteAndRead<uint32_t>();
        TestWriteAndRead<uint64_t>();
        TestWriteAndRead<float>();
        TestWriteAndRead<int8_t>();
        TestWriteAndRead<uint8_t>();
    }
   
    void TestCaseForWriteReadWithUniqEncode()
    {
        TestWriteAndRead<uint32_t>(true);
        TestWriteAndRead<uint64_t>(true);
        TestWriteAndRead<float>(true);
        TestWriteAndRead<int8_t>(true);
        TestWriteAndRead<uint8_t>(true);
    }

    void TestCaseForNoPatch()
    {
        TestForNoPatch<int8_t>();
        TestForNoPatch<uint8_t>();
        TestForNoPatch<int16_t>();
        TestForNoPatch<uint16_t>();
        TestForNoPatch<int32_t>();
        TestForNoPatch<uint32_t>();
        TestForNoPatch<int64_t>();
        TestForNoPatch<uint64_t>();
        TestForNoPatch<float>();
        TestForNoPatch<double>();
    }

    void TestCaseForPatch()
    {
        TestForPatch<int8_t>();
        TestForPatch<uint8_t>();
        TestForPatch<int16_t>();
        TestForPatch<uint16_t>();
        TestForPatch<int32_t>();
        TestForPatch<uint32_t>();
        TestForPatch<int64_t>();
        TestForPatch<uint64_t>();
        TestForPatch<float>();
        TestForPatch<double>();
    }

    void TestCaseForUpdateFieldWithoutPatch()
    {
        TestUpdateFieldWithoutPatch<int8_t>(true);
        TestUpdateFieldWithoutPatch<uint8_t>(true);
        TestUpdateFieldWithoutPatch<int16_t>(true);
        TestUpdateFieldWithoutPatch<uint16_t>(true);
        TestUpdateFieldWithoutPatch<int32_t>(true);
        TestUpdateFieldWithoutPatch<int64_t>(true);
        TestUpdateFieldWithoutPatch<uint64_t>(true);

        TestUpdateFieldWithoutPatch<int8_t>();
        TestUpdateFieldWithoutPatch<uint8_t>();
        TestUpdateFieldWithoutPatch<int16_t>();
        TestUpdateFieldWithoutPatch<uint16_t>();
        TestUpdateFieldWithoutPatch<int32_t>();
        TestUpdateFieldWithoutPatch<int64_t>();
        TestUpdateFieldWithoutPatch<uint64_t>();
    }

    void TestCaseForUpdateFieldWithPatch()
    {
        TestUpdateFieldWithPatch<int8_t>(true);
        TestUpdateFieldWithPatch<uint8_t>(true);
        TestUpdateFieldWithPatch<int16_t>(true);
        TestUpdateFieldWithPatch<uint16_t>(true);
        TestUpdateFieldWithPatch<int32_t>(true);
        TestUpdateFieldWithPatch<int64_t>(true);
        TestUpdateFieldWithPatch<uint64_t>(true);

        TestUpdateFieldWithPatch<int8_t>();
        TestUpdateFieldWithPatch<uint8_t>();
        TestUpdateFieldWithPatch<int16_t>();
        TestUpdateFieldWithPatch<uint16_t>();
        TestUpdateFieldWithPatch<int32_t>();
        TestUpdateFieldWithPatch<int64_t>();
        TestUpdateFieldWithPatch<uint64_t>();
    }

private:
    template<typename T>
    void TestForNoPatch()
    {
        vector<vector<T> > expectedValues;
        FullBuild(expectedValues);
        CheckOpen(expectedValues, 1);
    }

    template<typename T>
    void TestForPatch()
    {
        TestOpenForPatch<T>();
    }

    void CheckOffsetFileLength(string &offsetFilePath, int64_t targetLength)
    {
        FileMeta fileMeta;
        fslib::ErrorCode err = FileSystem::getFileMeta(offsetFilePath, fileMeta);
        EXPECT_EQ(EC_OK, err);
        EXPECT_EQ(targetLength, fileMeta.fileLength);
    }

private:
    template <typename T>
    void TestWriteAndRead(bool uniqEncode = false)
    {
        TearDown();
        SetUp();
        AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<T>(true);
        attrConfig->SetUniqEncode(uniqEncode);
        attrConfig->SetUpdatableMultiValue(true);
        AttributeConvertorPtr convertor(
                AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
        VarNumAttributeWriter<T> writer(attrConfig);
        writer.SetAttrConvertor(convertor);
        writer.Init(FSWriterParamDeciderPtr(), NULL);

        vector<vector<T> > data;
        string lastDocStr;
        autil::mem_pool::Pool pool;
        for (uint32_t i = 0; i < DOC_NUM; ++i)
        {
            // make duplicate attribute doc
            if (i % 2 == 1)
            {
                data.push_back(*data.rbegin());
                ConstString encodeStr = convertor->Encode(ConstString(lastDocStr), &pool);
                writer.AddField((docid_t)i, encodeStr);
                continue;
            }
            uint32_t valueLen = i * 3 % 10;
            stringstream ss;
            vector<T> dataOneDoc;
            for (uint32_t j = 0; j < valueLen; j++)
            {
                if (j != 0)
                {
                    ss << MULTI_VALUE_SEPARATOR;
                }

                int value = (i + j) * 3 % 128;
                ss << value;
                dataOneDoc.push_back((T)value);
            }
            data.push_back(dataOneDoc);
            lastDocStr = ss.str();
            ConstString encodeStr = convertor->Encode(ConstString(lastDocStr), &pool);
            writer.AddField((docid_t)i, encodeStr);
        }

        file_system::DirectoryPtr attrDirectory = 
            mSeg0Directory->MakeDirectory(ATTRIBUTE_DIR_NAME);
        util::SimplePool dumpPool;
        writer.Dump(attrDirectory, &dumpPool);

        file_system::IndexlibFileSystemPtr fs = GET_FILE_SYSTEM();
        fs->Sync(true);

        std::string offsetFilePath = util::PathUtil::JoinPath(
                attrDirectory->GetPath(),
                attrConfig->GetAttrName(), 
                ATTRIBUTE_OFFSET_FILE_NAME);

        CheckOffsetFileLength(offsetFilePath, sizeof(uint32_t) * (DOC_NUM + 1));
        MultiValueAttributeSegmentReader<T> segReader(attrConfig);
        
        SegmentInfo segInfo;

        segInfo.docCount = DOC_NUM;

        index_base::SegmentData segData = IndexTestUtil::CreateSegmentData(
                mSeg0Directory, segInfo, 0, 0);
        segReader.Open(segData);

        for (uint32_t i = 0; i < DOC_NUM; ++i)
        {
            MultiValueType<T> multiValue;
            ASSERT_TRUE(segReader.Read((docid_t)i, multiValue));
            ASSERT_EQ(data[i].size(), multiValue.size());
            for (size_t j = 0; j < data[i].size(); j++)
            {
                ASSERT_EQ(data[i][j], multiValue[j]);
            }
        }
        CheckOffsetFileLength(offsetFilePath, sizeof(uint32_t) * (DOC_NUM + 1));
    }

    template<typename T>
    void FullBuild(vector<vector<T> >& expectedValues, bool needCompress = false)
    {
        TearDown();
        SetUp();
        expectedValues.clear();
        mAttrConfig = AttributeTestUtil::CreateAttrConfig<T>(true, needCompress);
        VarNumAttributeWriter<T> writer(mAttrConfig);
        mAttrConfig->SetUniqEncode(false);
        mAttrConfig->SetUpdatableMultiValue(true);
        AttributeConvertorPtr convertor(
                AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(mAttrConfig->GetFieldConfig()));
        writer.SetAttrConvertor(convertor);
        writer.Init(FSWriterParamDeciderPtr(), NULL);
        autil::mem_pool::Pool pool;
        for (uint32_t docId = 0; docId < DOC_NUM; ++docId)
        {
            vector<T> valueVector;
            uint32_t valueCreater = 0;
            stringstream ss;
            uint32_t valueCount = docId;
            for (uint32_t j = 0; j < valueCount; ++j)
            {
                T value = valueCreater++;
                valueVector.push_back(value);
                ss << (int)value << MULTI_VALUE_SEPARATOR;
            }
            ConstString encodeValue = convertor->Encode(ConstString(ss.str()), &pool);
            writer.AddField((docid_t)docId, encodeValue);

            expectedValues.push_back(valueVector);
        }

        file_system::DirectoryPtr attrDirectory = 
            mSeg0Directory->MakeDirectory(ATTRIBUTE_DIR_NAME);
        util::SimplePool dumpPool;
        writer.Dump(attrDirectory, &dumpPool);

        SegmentInfo segInfo;
        segInfo.docCount = DOC_NUM;
        segInfo.Store(mSeg0Directory);
    }

    template<typename T>
    void CheckOpen(const vector<vector<T> >& expectedValues, uint32_t segmentCount)
    {
        index_base::PartitionDataPtr partData = IndexTestUtil::CreatePartitionData(
                GET_FILE_SYSTEM(), segmentCount);
        index_base::SegmentData segData = partData->GetSegmentData(0);

        AttributeSegmentPatchIteratorPtr attrPatchIter(
                new VarNumAttributePatchReader<T>(mAttrConfig));
        attrPatchIter->Init(partData, 0);

        MultiValueAttributeSegmentReader<T> segReader(mAttrConfig);
        segReader.Open(segData, file_system::DirectoryPtr(), attrPatchIter);

        CheckRead(expectedValues, segReader);
    }

    template<typename T>
    void CheckRead(const vector<vector<T> >& expectedValues, 
                   MultiValueAttributeSegmentReader<T>& segReader)
    {
        for (size_t i = 0; i < expectedValues.size(); i++)
        {
            MultiValueType<T> value;
            segReader.Read(i, value);
            const vector<T>& expectedValue = expectedValues[i];
            ASSERT_EQ(expectedValue.size(), value.size());
            for (size_t j = 0; j < expectedValue.size(); ++j)
            {
                ASSERT_EQ(expectedValue[j], value[j]);
            }
            
            uint64_t dataLen = segReader.GetDataLength((docid_t)i);
            uint8_t* buffer = (uint8_t*)mPool.allocate(dataLen);
            ASSERT_TRUE(segReader.Read((docid_t)i, buffer, dataLen));

            //repeatedly update same value test
            uint64_t offset = segReader.GetOffset((docid_t)i);
            for (size_t k = 0; k < 10; k++)
            {
                ASSERT_TRUE(segReader.UpdateField((docid_t)i,
                                buffer, dataLen));
                ASSERT_EQ(offset, segReader.GetOffset((docid_t)i));
            }
        }
    }

    template<typename T>
    void CreatePatches(vector<vector<T> >& expectedValues, uint32_t patchCount)
    {
        for (uint32_t i = 1; i <= patchCount; i++)
        {
            CreateOnePatch(expectedValues, i);
        }
    }

    template<typename T>
    void CreateOnePatch(vector<vector<T> >& expectedValues, segmentid_t outputSegId)
    {
        stringstream ss;
        ss << GET_TEST_DATA_PATH() << SEGMENT_FILE_NAME_PREFIX << "_" << outputSegId << "_level_0/";
        IndexTestUtil::ResetDir(ss.str());

        SegmentInfo segInfo;
        segInfo.docCount = DOC_NUM;
        segInfo.Store(ss.str() + SEGMENT_INFO_FILE_NAME);

        ss << ATTRIBUTE_DIR_NAME << "/";
        IndexTestUtil::ResetDir(ss.str());
        ss << mAttrConfig->GetAttrName() << "/";
        IndexTestUtil::ResetDir(ss.str());
        string patchFile = ss.str() + autil::StringUtil::toString<segmentid_t>(outputSegId) 
                           + "_0." + ATTRIBUTE_PATCH_FILE_NAME;
        unique_ptr<fslib::fs::File> file(fs::FileSystem::openFile(patchFile.c_str(), WRITE)); 

        uint32_t maxItemLen = 0;
        uint32_t patchCount = 0;
        for (uint32_t docId = 0; docId < DOC_NUM; ++docId)
        {
            if ((outputSegId + docId) % 10 == 1)
            {
                vector<T>& valueVector = expectedValues[docId];
                valueVector.clear();
                for (uint32_t valueId = 0; valueId < docId; ++valueId)
                {
                    T value = (outputSegId + docId + valueId) % 23;
                    valueVector.push_back(value);
                }
                uint32_t count = valueVector.size();
                file->write((void*)(&docId), sizeof(docid_t));

                char countBuff[4];
                size_t encodeLen = VarNumAttributeFormatter::EncodeCount(
                        count, countBuff, 4);
                file->write(countBuff, encodeLen);
                file->write((void*)(valueVector.data()), sizeof(T) * count);

                maxItemLen = max(maxItemLen, (uint32_t)(sizeof(T) * count + encodeLen));
                patchCount++;
            }
        }

        file->write(&patchCount, sizeof(uint32_t));
        file->write(&maxItemLen, sizeof(uint32_t));
        file->close();
    }

    template<typename T>
    void TestOpenForPatch()
    {
        vector<vector<T> > expectedValues;
        FullBuild(expectedValues);
        CreatePatches(expectedValues, 3);
        CheckOpen(expectedValues, 4);
    }

    template<typename T>
    void MakeUpdateData(map<docid_t, vector<T> >& toUpdateDocs)
    {
        for (uint32_t i = 0; i < UPDATE_DOC_NUM; i++)
        {
            docid_t docId = rand() % DOC_NUM;
            uint32_t valueCount = i;
            vector<T> & valueVector = toUpdateDocs[docId];
            for (uint32_t valueId = 0; valueId < valueCount; ++valueId)
            {
                T value = valueId + docId;
                valueVector.push_back(value);
            }
        }
    }

    template<typename T>
    void UpdateAndCheck(uint32_t segCount, vector<vector<T> >& expectedValues)
    {
        index_base::PartitionDataPtr partData = IndexTestUtil::CreatePartitionData(
                GET_FILE_SYSTEM(), segCount);
        index_base::SegmentData segData = partData->GetSegmentData(0);

        AttributeSegmentPatchIteratorPtr attrPatchIter(
                new VarNumAttributePatchReader<T>(mAttrConfig));
        attrPatchIter->Init(partData, 0);

        MultiValueAttributeSegmentReader<T> segReader(mAttrConfig);
        segReader.Open(segData, file_system::DirectoryPtr(), attrPatchIter);
        map<docid_t, vector<T> > toUpdateDocs;
        MakeUpdateData(toUpdateDocs);

        // update field
        typename map<docid_t, vector<T> >::iterator it = toUpdateDocs.begin();
        for (; it != toUpdateDocs.end(); it++)
        {
            docid_t docId = it->first;
            vector<T> value = it->second;

            size_t buffLen = VarNumAttributeFormatter::GetEncodedCountLength(value.size())
                             + sizeof(T) * value.size();
            uint8_t buff[buffLen];

            size_t encodeLen = VarNumAttributeFormatter::EncodeCount(
                    value.size(), (char*)buff, buffLen);
            memcpy(buff + encodeLen, value.data(), sizeof(T) * value.size());
            segReader.UpdateField(docId, buff, buffLen);
            expectedValues[docId] = value;
        }
        CheckRead(expectedValues, segReader);
    }

    template<typename T>
    void TestUpdateFieldWithoutPatch(bool needCompress = false)
    {
        vector<vector<T> > expectedValues;
        FullBuild(expectedValues, needCompress);
        UpdateAndCheck(1, expectedValues);
    }

    template<typename T>
    void TestUpdateFieldWithPatch(bool needCompress = false)
    {
        vector<vector<T> > expectedValues;
        FullBuild(expectedValues, needCompress);

        uint32_t patchCount = 3;
        CreatePatches(expectedValues, patchCount);
        UpdateAndCheck(patchCount + 1, expectedValues);
    }


private:
    AttributeConfigPtr mAttrConfig;
    file_system::DirectoryPtr mSeg0Directory;
    static const uint32_t DOC_NUM = 100;
    static const uint32_t UPDATE_DOC_NUM = 20;
    autil::mem_pool::Pool mPool;

};

INDEXLIB_UNIT_TEST_CASE(UpdatableVarNumAttributeSegmentReaderTest, TestCaseForWriteRead);
INDEXLIB_UNIT_TEST_CASE(UpdatableVarNumAttributeSegmentReaderTest, TestCaseForWriteReadWithUniqEncode);
INDEXLIB_UNIT_TEST_CASE(UpdatableVarNumAttributeSegmentReaderTest, TestCaseForNoPatch);
INDEXLIB_UNIT_TEST_CASE(UpdatableVarNumAttributeSegmentReaderTest, TestCaseForPatch);
INDEXLIB_UNIT_TEST_CASE(UpdatableVarNumAttributeSegmentReaderTest, TestCaseForUpdateFieldWithoutPatch);
INDEXLIB_UNIT_TEST_CASE(UpdatableVarNumAttributeSegmentReaderTest, TestCaseForUpdateFieldWithPatch);

IE_NAMESPACE_END(index);
