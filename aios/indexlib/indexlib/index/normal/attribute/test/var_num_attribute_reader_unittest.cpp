#include "indexlib/index/normal/attribute/test/var_num_attribute_reader_unittest.h"
#include "indexlib/util/fp16_encoder.h"
#include "indexlib/util/block_fp_encoder.h"
#include "indexlib/util/float_int8_encoder.h"

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);

void VarNumAttributeReaderTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    //srand(time(NULL));
    srand(0);
}

void VarNumAttributeReaderTest::CaseTearDown()
{
}

void VarNumAttributeReaderTest::TestCaseForRead()
{
    TestRead<uint32_t>();
    TestRead<int32_t>();
    TestRead<uint64_t>();
    TestRead<int64_t>();
    TestRead<int8_t>();
    TestRead<uint8_t>();
    TestRead<float>();
    TestRead<double>();
}

void VarNumAttributeReaderTest::TestCaseForReadWithSmallThreshold()
{
    uint64_t smallOffsetThresHold = 16;
    TestRead<uint32_t>(smallOffsetThresHold);
    TestRead<int32_t>(smallOffsetThresHold);
    TestRead<uint64_t>(smallOffsetThresHold);
    TestRead<int64_t>(smallOffsetThresHold);
    TestRead<int8_t>(smallOffsetThresHold);
    TestRead<uint8_t>(smallOffsetThresHold);
    TestRead<float>(smallOffsetThresHold);
    TestRead<double>(smallOffsetThresHold);
}

void VarNumAttributeReaderTest::TestCaseForReadWithUniqEncode()
{
    TestRead<uint32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, true);
    TestRead<int32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, true);
    TestRead<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, true);
    TestRead<int64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, true);
    TestRead<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, true);
    TestRead<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, true);
    TestRead<float>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, true);
    TestRead<double>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, true);
}

void VarNumAttributeReaderTest::TestCaseForReadWithUniqEncodeAndSmallThreshold()
{
    uint64_t smallOffsetThresHold = 16;
    TestRead<uint32_t>(smallOffsetThresHold, true);
    TestRead<int32_t>(smallOffsetThresHold, true);
    TestRead<uint64_t>(smallOffsetThresHold, true);
    TestRead<int64_t>(smallOffsetThresHold, true);
    TestRead<int8_t>(smallOffsetThresHold, true);
    TestRead<uint8_t>(smallOffsetThresHold, true);
    TestRead<float>(smallOffsetThresHold, true);
    TestRead<double>(smallOffsetThresHold, true);
}

void VarNumAttributeReaderTest::TestOpenForUInt32()
{
    vector<uint32_t> docCounts;
    docCounts.push_back(100);
    TestOpen<uint32_t>(docCounts); // test one segment
    TestOpen<uint32_t>(docCounts, false, true);

    docCounts.push_back(0);  // empty segment
    docCounts.push_back(230);
    docCounts.push_back(440);
    docCounts.push_back(30);
    TestOpen<uint32_t>(docCounts);  // test multi segments
    TestOpen<uint32_t>(docCounts, false, true);
}

void VarNumAttributeReaderTest::TestOpenWithPatchForUInt32()
{
    vector<uint32_t> fullBuildDocCounts;
    fullBuildDocCounts.push_back(100);
    TestOpen<uint32_t>(fullBuildDocCounts); // test one segment
    TestOpen<uint32_t>(fullBuildDocCounts, false, false);

    fullBuildDocCounts.push_back(0);  // empty segment
    fullBuildDocCounts.push_back(230);
    fullBuildDocCounts.push_back(440);
    fullBuildDocCounts.push_back(30);

    TestOpenWithPatches<uint32_t>(fullBuildDocCounts, 3);  // test multi segments
    TestOpenWithPatches<uint32_t>(fullBuildDocCounts, 3, false, true);
}


void VarNumAttributeReaderTest::TestOpenWithPatchForEncodeFloat()
{
    vector<uint32_t> fullBuildDocCounts;
    fullBuildDocCounts.push_back(100);
    fullBuildDocCounts.push_back(0);  // empty segment
    fullBuildDocCounts.push_back(230);
    fullBuildDocCounts.push_back(440);
    fullBuildDocCounts.push_back(30);

    TestOpenWithPatchesForEncodeFloat(fullBuildDocCounts, 3, 4, "fp16");
    TestOpenWithPatchesForEncodeFloat(fullBuildDocCounts, 3, 6, "block_fp");
    TestOpenWithPatchesForEncodeFloat(fullBuildDocCounts, 3, 3, "int8#127");
}


void VarNumAttributeReaderTest::TestCaseForUpdateFieldWithoutPatch()
{
    vector<uint32_t> docCounts;
    docCounts.push_back(100);

    // test one segment
    TestOpen<uint8_t>(docCounts, true);
    TestOpen<int8_t>(docCounts, true);
    TestOpen<uint16_t>(docCounts, true);
    TestOpen<int16_t>(docCounts, true);
    TestOpen<uint32_t>(docCounts, false);
    TestOpen<int32_t>(docCounts, false);
    TestOpen<uint64_t>(docCounts, true);
    TestOpen<int64_t>(docCounts, true);

    TestOpen<uint8_t>(docCounts, true, true);
    TestOpen<int8_t>(docCounts, true, true);
    TestOpen<uint16_t>(docCounts, true, true);
    TestOpen<int16_t>(docCounts, true, true);
    TestOpen<uint32_t>(docCounts, false, true);
    TestOpen<int32_t>(docCounts, false, true);
    TestOpen<uint64_t>(docCounts, true, true);
    TestOpen<int64_t>(docCounts, true, true);

    docCounts.push_back(0);  // empty segment
    docCounts.push_back(230);
    docCounts.push_back(440);
    docCounts.push_back(30);

    // test for many segments
    TestOpen<uint8_t>(docCounts, true, true);
    TestOpen<int8_t>(docCounts, true, true);
    TestOpen<uint16_t>(docCounts, true, true);
    TestOpen<int16_t>(docCounts, true, true);
    TestOpen<uint32_t>(docCounts, true, true);
    TestOpen<int32_t>(docCounts, true, true);
    TestOpen<uint64_t>(docCounts, true, true);
    TestOpen<int64_t>(docCounts, true, true);
}

void VarNumAttributeReaderTest::TestCaseForUpdateFieldWithPatch()
{
    vector<uint32_t> docCounts;
    docCounts.push_back(100);
    uint32_t incSegCount = 3;

    // test one segment
    TestOpenWithPatches<uint8_t>(docCounts, incSegCount, true);
    TestOpenWithPatches<int8_t>(docCounts, incSegCount, true);;
    TestOpenWithPatches<uint16_t>(docCounts, incSegCount, true);
    TestOpenWithPatches<int16_t>(docCounts, incSegCount, true);
    TestOpenWithPatches<uint32_t>(docCounts, incSegCount, false);
    TestOpenWithPatches<int32_t>(docCounts, incSegCount, false);
    TestOpenWithPatches<uint64_t>(docCounts, incSegCount, true);
    TestOpenWithPatches<int64_t>(docCounts, incSegCount, true);

    TestOpenWithPatches<uint8_t>(docCounts, incSegCount, true, true);
    TestOpenWithPatches<int8_t>(docCounts, incSegCount, true, true);
    TestOpenWithPatches<uint16_t>(docCounts, incSegCount, true, true);
    TestOpenWithPatches<int16_t>(docCounts, incSegCount, true, true);
    TestOpenWithPatches<uint32_t>(docCounts, incSegCount, false, true);
    TestOpenWithPatches<int32_t>(docCounts, incSegCount, false, true);
    TestOpenWithPatches<uint64_t>(docCounts, incSegCount, true, true);
    TestOpenWithPatches<int64_t>(docCounts, incSegCount, true, true);

    docCounts.push_back(0);  // empty segment
    docCounts.push_back(230);
    docCounts.push_back(440);
    docCounts.push_back(30);

    // test for many segments
    TestOpenWithPatches<uint8_t>(docCounts, incSegCount, true);
    TestOpenWithPatches<int8_t>(docCounts, incSegCount, true);
    TestOpenWithPatches<uint16_t>(docCounts, incSegCount, true);
    TestOpenWithPatches<int16_t>(docCounts, incSegCount, true);
    TestOpenWithPatches<uint32_t>(docCounts, incSegCount, true);
    TestOpenWithPatches<int32_t>(docCounts, incSegCount, true);
    TestOpenWithPatches<uint64_t>(docCounts, incSegCount, true);
    TestOpenWithPatches<int64_t>(docCounts, incSegCount, true);

    TestOpenWithPatches<uint8_t>(docCounts, incSegCount, true, true);
    TestOpenWithPatches<int8_t>(docCounts, incSegCount, true, true);
    TestOpenWithPatches<uint16_t>(docCounts, incSegCount, true, true);
    TestOpenWithPatches<int16_t>(docCounts, incSegCount, true, true);
    TestOpenWithPatches<uint32_t>(docCounts, incSegCount, true, true);
    TestOpenWithPatches<int32_t>(docCounts, incSegCount, true, true);
    TestOpenWithPatches<uint64_t>(docCounts, incSegCount, true, true);
    TestOpenWithPatches<int64_t>(docCounts, incSegCount, true, true);
}

void VarNumAttributeReaderTest::TestCaseForReadBuildingSegmentReader()
{
    {
        //test multi string
        stringstream ss;
        ss << "hello" << MULTI_VALUE_SEPARATOR << "world";
        string buildingValue = ss.str();
        InnerTestForReadBuildingSegmentReader<MultiChar>(
                "string:true", buildingValue, (uint32_t)2);
    }

    {
        //test normal multi value
        stringstream ss;
        ss << "1" << MULTI_VALUE_SEPARATOR << "2";
        string buildingValue = ss.str();
        InnerTestForReadBuildingSegmentReader<uint32_t>(
                "uint32:true", buildingValue, (uint32_t)2);
    }
    
}

void VarNumAttributeReaderTest::TestCaseForUpdateMassDataLongCaseTest()
{
    InnerTestCaseForUpdateMassData<uint32_t>(true);
    InnerTestCaseForUpdateMassData<uint32_t>(false);

    InnerTestCaseForUpdateMassData<uint64_t>(true);
    InnerTestCaseForUpdateMassData<int64_t>(false);
}

void VarNumAttributeReaderTest::WriteSegmentInfo(
        segmentid_t segId, const SegmentInfo& segInfo)
{
    stringstream segPath;
    segPath << mRootDir << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0/";
    string segInfoPath = segPath.str() + SEGMENT_INFO_FILE_NAME;
    segInfo.Store(segInfoPath);
}

void VarNumAttributeReaderTest::TestOpenWithPatchesForEncodeFloat(
        const vector<uint32_t>& fullBuildDocCounts,
        uint32_t incSegmentCount, int32_t fixedCount,
        const std::string& compressType)
{
    TearDown();
    SetUp();

    mAttrConfig = AttributeTestUtil::CreateAttrConfig(
            ft_float, true, "attr_name", 0, compressType, fixedCount);
    mAttrConfig->SetUpdatableMultiValue(true);
    vector<vector<float> > expectedData;
    for (uint32_t i = 0; i < fullBuildDocCounts.size(); i++)
    {
        segmentid_t segId = i;
        CreateDataForOneSegment(segId, fullBuildDocCounts[i], expectedData,
                                VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD);
        SegmentInfo segInfo;
        segInfo.docCount = fullBuildDocCounts[i];
        WriteSegmentInfo(segId, segInfo);
    }

    for (size_t i = 0; i < incSegmentCount; ++i)
    {
        segmentid_t segId = (segmentid_t)fullBuildDocCounts.size() + i;
        CreatePatchesForEncodeFloat(fixedCount, mAttrConfig->GetCompressType(),
                fullBuildDocCounts, segId, expectedData);
        SegmentInfo segInfo;
        segInfo.docCount = 0;
        WriteSegmentInfo(segId, segInfo);
    }

    uint32_t segCount = fullBuildDocCounts.size() + incSegmentCount;
    tr1::shared_ptr<VarNumAttributeReader<float> > reader = CreateAttrReader<float>(segCount);

    index_base::PartitionDataPtr partitionData = 
        IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segCount);
    partition::PatchLoader::LoadAttributePatch(*reader, mAttrConfig, partitionData);
    CheckRead(*reader, expectedData);
    CheckIterator(*reader, expectedData);
}

void VarNumAttributeReaderTest::CreatePatchesForEncodeFloat(int32_t fixedCount,
        const CompressTypeOption compressType, const vector<uint32_t>& docCounts,
        segmentid_t outputSegId, vector<vector<float> >& expectedData)
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < docCounts.size(); ++i)
    {
        CreateOnePatchForEncodeFloat(i, baseDocId, docCounts[i],
                outputSegId, expectedData, fixedCount, compressType);
        baseDocId += docCounts[i];
    }
}

void VarNumAttributeReaderTest::CreateOnePatchForEncodeFloat(
        segmentid_t segIdToPatch, docid_t baseDocIdInSegToPatch,
        uint32_t docCountInSegToPatch, segmentid_t outputSegId, 
        vector<vector<float> >& expectedData, int32_t fixedCount,
        const CompressTypeOption compressType)
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
            vector<float> values;
            //std::cout << "docid: " << i << ", num: " << valueLen << ", value: ";
            for (uint32_t j = 0; j < (uint32_t)fixedCount; j++)
            {
                float value = (i + j) * 5 % 128;
                values.push_back(value);
            }
            expectedData[i + baseDocIdInSegToPatch] = values;

            int32_t encodeLen = 0;
            char buffer[values.size() * sizeof(float)];
            if (compressType.HasFp16EncodeCompress())
            {
                encodeLen = Fp16Encoder::Encode((const float*)values.data(),
                        values.size(), buffer, values.size() * sizeof(float));
            }
            else if (compressType.HasBlockFpEncodeCompress())
            { 
                encodeLen = BlockFpEncoder::Encode((const float*)values.data(),
                        values.size(), buffer, values.size() * sizeof(float));
            }
            else if (compressType.HasInt8EncodeCompress())
            {
                encodeLen = FloatInt8Encoder::Encode(compressType.GetInt8AbsMax(),
                        (const float*)values.data(), values.size(), buffer, values.size() * sizeof(float));
            }
            file->write((void*)buffer, encodeLen);
            maxPatchLen = std::max((uint32_t)encodeLen, maxPatchLen);
        }
    }
    file->write(&patchCount, sizeof(uint32_t));
    file->write(&maxPatchLen, sizeof(uint32_t));
    file->close();
}


IE_NAMESPACE_END(index);
