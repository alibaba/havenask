#include <autil/StringUtil.h>
#include <autil/MultiValueCreator.h>
#include <autil/mem_pool/Pool.h>
#include <fslib/fs/FileSystem.h>
#include <fslib/fs/File.h>
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common_define.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/index/normal/attribute/accessor/multi_string_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_writer.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/test/single_field_partition_data_provider.h"

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

class VarNumAttributeReaderTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForRead();
    void TestCaseForReadWithSmallThreshold();
    void TestCaseForReadWithUniqEncode();
    void TestCaseForReadWithUniqEncodeAndSmallThreshold();
    void TestOpenForUInt32();
    void TestOpenWithPatchForUInt32();
    void TestCaseForUpdateFieldWithoutPatch();
    void TestCaseForUpdateFieldWithPatch();
    void TestCaseForReadBuildingSegmentReader();
    void TestCaseForUpdateMassDataLongCaseTest();
    void TestOpenWithPatchForEncodeFloat();

private:
    void WriteSegmentInfo(segmentid_t segId, const SegmentInfo& segInfo);
    
    template<typename T>
    void TestRead(uint64_t thresHold = VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 
                  bool uniqEncode = false);

    template<typename T>
    void FullBuild(const vector<uint32_t>& docCounts, 
                   vector<vector<T> >& ans, 
                   uint64_t thresHold, bool uniqEncode,
                   bool isUpdatable = false);

    template<typename T>
    void CreateDataForOneSegment(segmentid_t segId, uint32_t docCount, 
                                 vector<vector<T> >& ans,
                                 uint64_t thresHold = VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD);
    
    template<typename T>
    void TestOpen(const vector<uint32_t>& docCounts, bool update = false,
                  bool uniqEncode = false);

    template<typename T>
    void MakeUpdateDocs(uint32_t totalDocCount,
                map<docid_t, vector<T> >& toUpdateDocs);

    template<typename T>
    void UpdateField(VarNumAttributeReader<T> &reader, 
                     map<docid_t, vector<T> > &toUpdateDocs,
                     vector<vector<T> > &expectedData);
    
    template<typename T>
    void TestOpenWithPatches(const vector<uint32_t>& fullBuildDocCounts,
                             uint32_t incSegmentCount,
                             bool update = false,
                             bool uniqEncode = false);

    template<typename T>
    void CreatePatches(const vector<uint32_t>& docCounts,
                       segmentid_t outputSegId, 
                       vector<vector<T> >& expectedData);
    
    template<typename T>
    void CreateOnePatch(segmentid_t segIdToPatch,
                        docid_t baseDocIdInSegToPatch,
                        uint32_t docCountInSegToPatch,
                        segmentid_t outputSegId, 
                        vector<vector<T> >& expectedData);

    void TestOpenWithPatchesForEncodeFloat(
            const vector<uint32_t>& fullBuildDocCounts,
            uint32_t incSegmentCount, int32_t fixedCount,
            const std::string& compressType);

    void CreatePatchesForEncodeFloat(int32_t fixedCount,
            const config::CompressTypeOption compressType,
            const vector<uint32_t>& docCounts,
            segmentid_t outputSegId, 
            vector<vector<float> >& expectedData);

    void CreateOnePatchForEncodeFloat(
            segmentid_t segIdToPatch, docid_t baseDocIdInSegToPatch,
            uint32_t docCountInSegToPatch, segmentid_t outputSegId, 
            vector<vector<float> >& expectedData, int32_t fixedCount,
            const config::CompressTypeOption compressType);

    template<class T>
    tr1::shared_ptr<VarNumAttributeReader<T> > CreateAttrReader(uint32_t segCount);

    template<typename T>
    void CheckRead(const VarNumAttributeReader<T>& reader, const vector<vector<T> >& ans);

    template<typename T>
    void CheckIterator(const VarNumAttributeReader<T>& reader,
                       const vector<vector<T> >& ans);

    template<typename T>
    void ConvertToOriginalString(
            const autil::MultiValueType<T>& value, std::string& attrValue);

    template<typename T>
    void InnerTestForReadBuildingSegmentReader(
            const string& fieldType, const string& buildingValue,
            uint32_t valueCount);

    template<typename T>
    void InnerTestCaseForUpdateMassData(bool isUniqEncode);

    template<typename T>
    std::string MakeData(uint32_t count, T* valueArray, T step);

private:
    string mRootDir;
    config::AttributeConfigPtr mAttrConfig;
    autil::mem_pool::Pool _pool;
    static const uint32_t UPDATE_DOC_NUM = 20;
};

///////////////////////////////////////
template<typename T>
void VarNumAttributeReaderTest::InnerTestForReadBuildingSegmentReader(
        const string& fieldType, const string& buildingValue,
        uint32_t valueCount)
{
    TearDown();
    SetUp();
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, fieldType, SFP_ATTRIBUTE);
    provider.Build("", SFP_OFFLINE);
    provider.Build(buildingValue, SFP_REALTIME);
    
    index_base::PartitionDataPtr partitionData = provider.GetPartitionData();
    config::AttributeConfigPtr attrConfig = provider.GetAttributeConfig();

    VarNumAttributeReader<T> reader;
    reader.Open(attrConfig, partitionData);

    autil::MultiValueType<T> value;
    reader.Read(0, value);
    ASSERT_EQ(valueCount, value.size());
    
    string resultStr;
    ConvertToOriginalString(value, resultStr);
    ASSERT_EQ(buildingValue, resultStr);

    reader.Read(0, resultStr);
    ASSERT_EQ(buildingValue, resultStr);

    // docId out of range
    ASSERT_FALSE(reader.Read(1, resultStr));
    
    typedef AttributeIteratorTyped<autil::MultiValueType<T>, AttributeReaderTraits<autil::MultiValueType<T> > > AttributeIterator;

    //check iterator
    AttributeIterator* iterator = 
         dynamic_cast<AttributeIterator*>(
                 reader.CreateIterator(&_pool));
    iterator->Seek(0, value);

    ASSERT_EQ(valueCount, value.size());
    ConvertToOriginalString(value, resultStr);
    ASSERT_EQ(buildingValue, resultStr);

    iterator->SeekInRandomMode(0, value);
    ASSERT_EQ(valueCount, value.size());
    ConvertToOriginalString(value, resultStr);
    ASSERT_EQ(buildingValue, resultStr);
    IE_POOL_COMPATIBLE_DELETE_CLASS(&_pool, iterator);
}

template<typename T>
void VarNumAttributeReaderTest::ConvertToOriginalString(
        const autil::MultiValueType<T>& value, std::string& attrValue)
{
    uint32_t size = value.size();
    attrValue.clear();
    for (uint32_t i= 0; i < size; i++)
    {
        std::string item = autil::StringUtil::toString<T>(value[i]);
        if (i != 0)
        {
            attrValue += MULTI_VALUE_SEPARATOR;
        }
        attrValue += item;
    }
}

template<typename T>
void VarNumAttributeReaderTest::TestRead(
        uint64_t thresHold, bool uniqEncode)
{
    vector<uint32_t> docCounts;
    docCounts.push_back(100);
    docCounts.push_back(33);
    docCounts.push_back(22);
    vector<vector<T> > ans;
    FullBuild(docCounts, ans, thresHold, uniqEncode);

    PartitionDataPtr partitionData = 
        IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), docCounts.size());
    VarNumAttributeReader<T> reader;
    reader.Open(mAttrConfig, partitionData);

    CheckRead<T>(reader, ans);
    CheckIterator<T>(reader, ans);
}

template<typename T>
void VarNumAttributeReaderTest::FullBuild(
        const vector<uint32_t>& docCounts, vector<vector<T> >& ans, 
        uint64_t thresHold, bool uniqEncode, bool isUpdatable)
{
    TearDown();
    SetUp();
    mAttrConfig = AttributeTestUtil::CreateAttrConfig<T>(true);
    mAttrConfig->GetFieldConfig()->ClearCompressType();
    mAttrConfig->SetUniqEncode(uniqEncode);
    mAttrConfig->SetUpdatableMultiValue(isUpdatable);

    for (uint32_t i = 0; i < docCounts.size(); i++)
    {
        segmentid_t segId = i;
        CreateDataForOneSegment(segId, docCounts[i], ans, thresHold);

        SegmentInfo segInfo;
        segInfo.docCount = docCounts[i];
        WriteSegmentInfo(segId, segInfo);
    }
}

template<typename T>
void VarNumAttributeReaderTest::CreateDataForOneSegment(
        segmentid_t segId, uint32_t docCount, 
        vector<vector<T> >& ans, uint64_t thresHold)
{
    mAttrConfig->GetFieldConfig()->SetU32OffsetThreshold(thresHold);
    VarNumAttributeWriter<T> writer(mAttrConfig);
    common::AttributeConvertorPtr convertor(
            common::AttributeConvertorFactory::GetInstance()
            ->CreateAttrConvertor(mAttrConfig->GetFieldConfig()));
    writer.SetAttrConvertor(convertor);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    autil::mem_pool::Pool pool;

    string lastDocStr;
    for (uint32_t i = 0; i < docCount; ++i)
    {
        // make duplicate attribute doc
        if (i % 2 == 1)
        {
            ans.push_back(*ans.rbegin());
            ConstString encodeStr = convertor->Encode(ConstString(lastDocStr), &pool);
            writer.AddField((docid_t)i, encodeStr);
            continue;
        }
        uint32_t valueLen = mAttrConfig->IsLengthFixed() ?
                            mAttrConfig->GetFieldConfig()->GetFixedMultiValueCount() :
                            (i + rand()) * 3 % 10;
        stringstream ss;
        vector<T> dataOneDoc;
        for (uint32_t j = 0; j < valueLen; j++)
        {
            if (j != 0)
            {
                ss << MULTI_VALUE_SEPARATOR;
            }
            ss << (i + j) * 3 % 128;
            dataOneDoc.push_back((T)((i + j) * 3 % 128));
        }
        ans.push_back(dataOneDoc);
        lastDocStr = ss.str();
        ConstString encodeStr = convertor->Encode(ConstString(lastDocStr), &pool);
        writer.AddField((docid_t)i, encodeStr);
    }

    stringstream segPath;
    segPath << mRootDir << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0/";
    IndexTestUtil::ResetDir(segPath.str());

    string attrPath = segPath.str() + ATTRIBUTE_DIR_NAME + "/";
    IndexTestUtil::ResetDir(attrPath);
    AttributeWriterHelper::DumpWriter(writer, attrPath);
}

template<typename T>
void VarNumAttributeReaderTest::TestOpen(
        const vector<uint32_t>& docCounts, bool update, bool uniqEncode)
{
    vector<vector<T> > expectedData;
    FullBuild(docCounts, expectedData, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 
            uniqEncode, true);

    uint32_t segCount = docCounts.size();
    tr1::shared_ptr<VarNumAttributeReader<T> > reader = CreateAttrReader<T>(segCount);
    CheckRead(*reader, expectedData);
    CheckIterator(*reader, expectedData);

    if (update)
    {
        map<docid_t, vector<T> > toUpdateDocs;
        MakeUpdateDocs(expectedData.size(), toUpdateDocs);
        UpdateField(*reader, toUpdateDocs, expectedData);
        CheckRead(*reader, expectedData);
        CheckIterator(*reader, expectedData);
    }
}

template<typename T>
void VarNumAttributeReaderTest::MakeUpdateDocs(uint32_t totalDocCount,
        map<docid_t, vector<T> >& toUpdateDocs)
{
    for (uint32_t i = 0; i < UPDATE_DOC_NUM; i++)
    {
        docid_t docId = rand() % totalDocCount;
        vector<T> values;
        uint32_t valueLen = (i + rand()) * 3 % 10;
        for (uint32_t j = 0; j < valueLen; j++)
        {
            T value = (i + j) * 3 % 128;
            values.push_back(value);
        }
        toUpdateDocs[docId] = values;
    }
}

template<typename T>
void VarNumAttributeReaderTest::UpdateField(
        VarNumAttributeReader<T> &reader, 
        map<docid_t, vector<T> > &toUpdateDocs,
        vector<vector<T> > &expectedData)
{
    typename map<docid_t, vector<T> >::iterator it = toUpdateDocs.begin();
    for (; it != toUpdateDocs.end(); it++)
    {
        docid_t docId = it->first;
        vector<T> value = it->second;

        size_t encodeLen = 
            VarNumAttributeFormatter::GetEncodedCountLength(value.size());
        size_t buffLen = encodeLen + sizeof(T) * value.size();
        uint8_t buff[buffLen];
        
        VarNumAttributeFormatter::EncodeCount(value.size(), (char*)buff, buffLen);
        memcpy(buff + encodeLen, value.data(), sizeof(T) * value.size());
        reader.UpdateField(docId, buff, buffLen);
        expectedData[docId] = value;
    }
}

template<typename T>
void VarNumAttributeReaderTest::TestOpenWithPatches(
        const vector<uint32_t>& fullBuildDocCounts, uint32_t incSegmentCount,
        bool update, bool uniqEncode)
{
    vector<vector<T> > expectedData;
    FullBuild(fullBuildDocCounts, expectedData, 
            VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, uniqEncode, true);

    for (size_t i = 0; i < incSegmentCount; ++i)
    {
        segmentid_t segId = (segmentid_t)fullBuildDocCounts.size() + i;
        CreatePatches(fullBuildDocCounts, segId, expectedData);
        SegmentInfo segInfo;
        segInfo.docCount = 0;
        WriteSegmentInfo(segId, segInfo);
    }

    uint32_t segCount = fullBuildDocCounts.size() + incSegmentCount;
    tr1::shared_ptr<VarNumAttributeReader<T> > reader = CreateAttrReader<T>(segCount);

    index_base::PartitionDataPtr partitionData = 
        IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segCount);
    partition::PatchLoader::LoadAttributePatch(
            *reader, mAttrConfig, partitionData);
    CheckRead(*reader, expectedData);
    CheckIterator(*reader, expectedData);

    if (update)
    {
        map<docid_t, vector<T> > toUpdateDocs;
        MakeUpdateDocs(expectedData.size(), toUpdateDocs);
        UpdateField(*reader, toUpdateDocs, expectedData);
        CheckRead(*reader, expectedData);
        CheckIterator(*reader, expectedData);
    }
}

template<typename T>
void VarNumAttributeReaderTest::CreatePatches(
        const vector<uint32_t>& docCounts,
        segmentid_t outputSegId, 
        vector<vector<T> >& expectedData)
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < docCounts.size(); ++i)
    {
        CreateOnePatch(i, baseDocId, docCounts[i],
                outputSegId, expectedData);
        baseDocId += docCounts[i];
    }
}

template<typename T>
void VarNumAttributeReaderTest::CreateOnePatch(segmentid_t segIdToPatch,
        docid_t baseDocIdInSegToPatch,
        uint32_t docCountInSegToPatch,
        segmentid_t outputSegId, 
        vector<vector<T> >& expectedData)
{
    stringstream ss;
    ss << mRootDir << SEGMENT_FILE_NAME_PREFIX << "_" << outputSegId << "_level_0/";
    IndexTestUtil::MkDir(ss.str());
    ss << ATTRIBUTE_DIR_NAME << "/";
    IndexTestUtil::MkDir(ss.str());
    ss << mAttrConfig->GetAttrName() << "/";
    IndexTestUtil::MkDir(ss.str());
    ss << outputSegId << "_" << segIdToPatch << "." << ATTRIBUTE_PATCH_FILE_NAME;
    string patchFile = ss.str();

    unique_ptr<fslib::fs::File> file(fs::FileSystem::openFile(patchFile.c_str(), WRITE)); 

    uint32_t patchCount = 0;
    uint32_t maxPatchLen = 0;
    for (uint32_t i = 0; i < docCountInSegToPatch; i++)
    {
        if ((outputSegId + i) % 10 == 1)
        {
            patchCount++;
            file->write((void*)(&i), sizeof(docid_t));
            uint16_t valueLen = (i + rand()) * 3 % 10;
            char countBuffer[4];
            size_t encodeLen = VarNumAttributeFormatter::EncodeCount(
                    valueLen, countBuffer, 4);
            file->write(countBuffer, encodeLen);
            vector<T> values;
            //std::cout << "docid: " << i << ", num: " << valueLen << ", value: ";
            for (uint32_t j = 0; j < valueLen; j++)
            {
                T value = (i + j) * 5 % 128;
                values.push_back(value);
            }
            expectedData[i + baseDocIdInSegToPatch] = values;
            file->write((void*)(values.data()), values.size() * sizeof(T));
            maxPatchLen = std::max(maxPatchLen, 
                    (uint32_t)(encodeLen + values.size() * sizeof(T)));
        }
    }

    file->write(&patchCount, sizeof(uint32_t));
    file->write(&maxPatchLen, sizeof(uint32_t));
    file->close();
}

template<class T>
std::tr1::shared_ptr<VarNumAttributeReader<T> > VarNumAttributeReaderTest::CreateAttrReader(uint32_t segCount)
{
    PartitionDataPtr partitionData = 
        IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segCount);
    std::tr1::shared_ptr<VarNumAttributeReader<T> > reader;
    reader.reset(new VarNumAttributeReader<T>());
    reader->Open(mAttrConfig, partitionData);
    return reader;
}

template<typename T>
void VarNumAttributeReaderTest::CheckRead(
            const VarNumAttributeReader<T>& reader,
            const vector<vector<T> >& ans)
{
    for (docid_t i = 0; i < (docid_t)ans.size(); i++)
    {
        autil::MultiValueType<T> multiValue;
        string strValue;
        EXPECT_TRUE(reader.Read(i, multiValue, &_pool));
        EXPECT_TRUE(reader.Read(i, strValue, &_pool));
        ASSERT_EQ(ans[i].size(), multiValue.size());

        string ansStrValue;
        for (size_t j = 0; j < ans[i].size(); j++)
        {
            ASSERT_EQ(ans[i][j], multiValue[j]);
            string item = autil::StringUtil::toString(multiValue[j]);
            if (j != 0)
            {
                ansStrValue += MULTI_VALUE_SEPARATOR;
            }
            ansStrValue += item;
        }

        ASSERT_EQ(ansStrValue, strValue);
    }
}

template<typename T>
void VarNumAttributeReaderTest::CheckIterator(
        const VarNumAttributeReader<T>& reader,
        const vector<vector<T> >& ans)
{
    AttributeIteratorBase* attrIter = reader.CreateIterator(&_pool);
    typename VarNumAttributeReader<T>::AttributeIterator* typedIter = 
        dynamic_cast<typename VarNumAttributeReader<T>::AttributeIterator*> (attrIter);
    for (docid_t i = 0; i < (docid_t)ans.size(); i++)
    {
        autil::MultiValueType<T> multiValue;
        EXPECT_TRUE(typedIter->Seek(i, multiValue));
        ASSERT_EQ(ans[i].size(), multiValue.size());

        for (size_t j = 0; j < ans[i].size(); j++)
        {
            ASSERT_EQ(ans[i][j], multiValue[j]);
        }
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(&_pool, attrIter);
}

template<typename T>
void VarNumAttributeReaderTest::InnerTestCaseForUpdateMassData(bool isUniqEncode)
{
    vector<uint32_t> docCounts;
    docCounts.push_back(1);
    vector<vector<T> > ans;
    FullBuild(docCounts, ans, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, isUniqEncode, true);

    std::tr1::shared_ptr<VarNumAttributeReader<T> > reader = 
        CreateAttrReader<T>(docCounts.size());
    CheckRead<T>(*reader, ans);
    CheckIterator<T>(*reader, ans);

    #define COUNT 10000
    T valueArray1[COUNT];
    T valueArray2[COUNT];
    string value1 = MakeData<T>(COUNT, valueArray1, 1);
    string value2 = MakeData<T>(COUNT, valueArray2, 2);
    vector<T> expectValue1Vec(valueArray1, valueArray1 + COUNT);
    vector<T> expectValue2Vec(valueArray2, valueArray2 + COUNT);
    char* buf1 = MultiValueCreator::createMultiValueBuffer(expectValue1Vec);
    char* buf2 = MultiValueCreator::createMultiValueBuffer(expectValue2Vec);
    autil::MultiValueType<T> expectValue1(buf1);
    autil::MultiValueType<T> expectValue2(buf2);

    common::VarNumAttributeConvertor<T> convertor(true);
    string encodeValue1 = convertor.Encode(value1);
    string encodeValue2 = convertor.Encode(value2);
    ConstString updateValue1(encodeValue1);
    ConstString updateValue2(encodeValue2);
    common::AttrValueMeta meta1 = convertor.Decode(updateValue1);
    common::AttrValueMeta meta2 = convertor.Decode(updateValue2);

    size_t length = meta1.data.size() + meta2.data.size();
    size_t totalLen = (size_t)5 * 1024 * 1024 * 1024; // 5G
    size_t currentLen = 0;
    while (currentLen < totalLen)
    {
        reader->UpdateField(0, (uint8_t*)meta1.data.data(), meta1.data.size());
        autil::MultiValueType<T> value;
        reader->Read(0, value);
        ASSERT_EQ(expectValue1, value);

        reader->UpdateField(0, (uint8_t*)meta2.data.data(), meta2.data.size());
        reader->Read(0, value);
        ASSERT_EQ(expectValue2, value);
        currentLen += length;
    }
    delete [] buf1;
    delete [] buf2;
}

template<typename T>
string VarNumAttributeReaderTest::MakeData(uint32_t count, 
        T* valueArray, T step)
{
    stringstream ss;
    for (size_t i = 0; i < COUNT; i++)
    {
        valueArray[i] = (T)i + step;
        ss << (T)i + step;
        if (i != COUNT - 1)
        {
            ss << "";
        }
    }
    return ss.str();
}

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeReaderTest, TestCaseForRead);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeReaderTest, TestCaseForReadWithSmallThreshold);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeReaderTest, TestCaseForReadWithUniqEncode);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeReaderTest, TestCaseForReadWithUniqEncodeAndSmallThreshold);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeReaderTest, TestOpenForUInt32);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeReaderTest, TestOpenWithPatchForUInt32);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeReaderTest, TestCaseForUpdateFieldWithoutPatch);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeReaderTest, TestCaseForUpdateFieldWithPatch);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeReaderTest, TestCaseForReadBuildingSegmentReader);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeReaderTest, TestCaseForUpdateMassDataLongCaseTest);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeReaderTest, TestOpenWithPatchForEncodeFloat);

IE_NAMESPACE_END(index);
