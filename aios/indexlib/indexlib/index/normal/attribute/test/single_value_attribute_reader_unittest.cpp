#include <stdlib.h>
#include <time.h>
#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "autil/mem_pool/Pool.h"

#include "fslib/fs/FileSystem.h"
#include "fslib/fs/File.h"

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(index);

class SingleValueAttributeReaderTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
        mRoot = GET_TEST_DATA_PATH() + "/";
        mSetCompress = false;
        srand(time(NULL));
    }

    void CaseTearDown() override
    {
    }

    void TestOpen()
    {
        vector<uint32_t> docCounts;
        docCounts.push_back(100);

        // test one segment
        TestOpen<uint8_t>(docCounts); 
        TestOpen<int8_t>(docCounts); 
        TestOpen<uint16_t>(docCounts); 
        TestOpen<int16_t>(docCounts); 
        TestOpen<uint32_t>(docCounts); 
        TestOpen<int32_t>(docCounts); 
        TestOpen<uint64_t>(docCounts); 
        TestOpen<int64_t>(docCounts); 
        TestOpen<autil::uint128_t>(docCounts); 

        docCounts.push_back(0);  // empty segment
        docCounts.push_back(230);
        docCounts.push_back(440);
        docCounts.push_back(30);

        TestOpen<uint8_t>(docCounts); 
        TestOpen<int8_t>(docCounts); 
        TestOpen<uint16_t>(docCounts); 
        TestOpen<int16_t>(docCounts); 
        TestOpen<uint32_t>(docCounts); 
        TestOpen<int32_t>(docCounts); 
        TestOpen<uint64_t>(docCounts); 
        TestOpen<int64_t>(docCounts); 
        TestOpen<autil::uint128_t>(docCounts); 
    }

    void TestOpenForEqualCompress()
    {
        mSetCompress = true;
        vector<uint32_t> docCounts;
        docCounts.push_back(100);

        // test one segment
        TestOpen<uint8_t>(docCounts); 
        TestOpen<int8_t>(docCounts); 
        TestOpen<uint16_t>(docCounts); 
        TestOpen<int16_t>(docCounts); 
        TestOpen<uint32_t>(docCounts); 
        TestOpen<int32_t>(docCounts); 
        TestOpen<uint64_t>(docCounts); 
        TestOpen<int64_t>(docCounts); 

        docCounts.push_back(0);  // empty segment
        docCounts.push_back(230);
        docCounts.push_back(440);
        docCounts.push_back(30);

        TestOpen<uint8_t>(docCounts); 
        TestOpen<int8_t>(docCounts); 
        TestOpen<uint16_t>(docCounts); 
        TestOpen<int16_t>(docCounts); 
        TestOpen<uint32_t>(docCounts); 
        TestOpen<int32_t>(docCounts); 
        TestOpen<uint64_t>(docCounts); 
        TestOpen<int64_t>(docCounts); 
    }

    void TestOpenWithPatchForUInt32()
    {
        vector<uint32_t> fullBuildDocCounts;
        fullBuildDocCounts.push_back(100);
        TestOpen<uint32_t>(fullBuildDocCounts); // test one segment

        fullBuildDocCounts.push_back(0);  // empty segment
        fullBuildDocCounts.push_back(230);
        fullBuildDocCounts.push_back(440);
        fullBuildDocCounts.push_back(30);

        TestOpenWithPatches<uint32_t>(fullBuildDocCounts, 3);  // test multi segments
    }

    void TestCaseForUpdateFieldWithoutPatch()
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

        docCounts.push_back(0);  // empty segment
        docCounts.push_back(230);
        docCounts.push_back(440);
        docCounts.push_back(30);

        // test for many segments
        TestOpen<uint8_t>(docCounts, true);
        TestOpen<int8_t>(docCounts, true);
        TestOpen<uint16_t>(docCounts, true);
        TestOpen<int16_t>(docCounts, true);
        TestOpen<uint32_t>(docCounts, true);
        TestOpen<int32_t>(docCounts, true);
        TestOpen<uint64_t>(docCounts, true);
        TestOpen<int64_t>(docCounts, true);
    }

    void TestCaseForUpdateFieldWithPatch()
    {
        vector<uint32_t> docCounts;
        docCounts.push_back(100);
        uint32_t incSegCount = 3;

        // test one segment
        TestOpenWithPatches<uint8_t>(docCounts, incSegCount, true);
        TestOpenWithPatches<int8_t>(docCounts, incSegCount, true);
        TestOpenWithPatches<uint16_t>(docCounts, incSegCount, true);
        TestOpenWithPatches<int16_t>(docCounts, incSegCount, true);
        TestOpenWithPatches<uint32_t>(docCounts, incSegCount, false);
        TestOpenWithPatches<int32_t>(docCounts, incSegCount, false);
        TestOpenWithPatches<uint64_t>(docCounts, incSegCount, true);
        TestOpenWithPatches<int64_t>(docCounts, incSegCount, true);

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
    }

    void TestCaseForReadBuildingSegmentReader()
    {
        SingleFieldPartitionDataProvider provider;
        provider.Init(mRoot, "uint32", SFP_ATTRIBUTE);
        provider.Build("", SFP_OFFLINE);
        provider.Build("1", SFP_REALTIME);
        PartitionDataPtr partData = provider.GetPartitionData();
        AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
        SingleValueAttributeReader<uint32_t> reader;
        reader.Open(attrConfig, partData);

        uint32_t actualValue;
        ASSERT_TRUE(reader.Read(0, actualValue));
        ASSERT_EQ((uint32_t)1, actualValue);

        string strValue;
        ASSERT_TRUE(reader.Read(0, strValue));
        ASSERT_EQ(string("1"), strValue);

        // docId out of range
        ASSERT_FALSE(reader.Read(1, strValue));
    }

    void TestCaseForUpdateBuildingSegmentReader()
    {
        SingleFieldPartitionDataProvider provider;
        provider.Init(mRoot, "uint32", SFP_ATTRIBUTE);
        provider.Build("", SFP_OFFLINE);
        provider.Build("1", SFP_REALTIME);
        provider.Build("2", SFP_REALTIME);
        PartitionDataPtr partData = provider.GetPartitionData();
        AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
        SingleValueAttributeReader<uint32_t> reader;
        reader.Open(attrConfig, partData);

        uint32_t updateValue = 3;
        ASSERT_TRUE(reader.UpdateField(0, (uint8_t*)&updateValue, sizeof(uint32_t)));
        ASSERT_FALSE(reader.UpdateField(2, (uint8_t*)&updateValue, sizeof(uint32_t)));

        AttributeIteratorBase* attrIter = reader.CreateIterator(&mPool);
        AttributeIteratorTyped<uint32_t>* typedIter = 
            dynamic_cast<AttributeIteratorTyped<uint32_t>* > (attrIter);
        ASSERT_TRUE(typedIter->UpdateValue(1, updateValue));
        ASSERT_FALSE(typedIter->UpdateValue(2, updateValue));
        IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, attrIter);
        
        uint32_t actualValue;
        ASSERT_TRUE(reader.Read(0, actualValue));
        ASSERT_EQ(updateValue, actualValue);
        ASSERT_TRUE(reader.Read(1, actualValue));
        ASSERT_EQ(updateValue, actualValue);
    }

private:
    typedef map<docid_t, autil::uint128_t> UInt128DocMap;
    void MakeUpdateDocs(uint32_t totalDocCount, UInt128DocMap& toUpdateDocs)
    {
        for (uint32_t i = 0; i < UPDATE_DOC_NUM; i++)
        {
            docid_t docId = rand() % totalDocCount;
            autil::uint128_t value((docId + 10) % 128);
            toUpdateDocs[docId] = value;
        }
    }

    template<typename T>
    void FullBuild(const vector<uint32_t>& docCounts, vector<T>& expectedData)
    {
        vector<vector<T> > segmentDatas(docCounts.size());
        for (size_t i = 0; i < docCounts.size(); i++)
        {
            GenerateData(docCounts[i], segmentDatas[i]);
            expectedData.insert(expectedData.end(), 
                    segmentDatas[i].begin(), segmentDatas[i].end());
        }
        DoFullBuild(segmentDatas);
    }

    template<typename T>
    void DoFullBuild(const vector<vector<T> > &segmentDatas)
    {
        TearDown();
        SetUp();
        mAttrConfig = AttributeTestUtil::CreateAttrConfig<T>(
                false, mSetCompress);
        mAttrConfig->GetFieldConfig()->ClearCompressType();
        for (uint32_t i = 0; i < segmentDatas.size(); i++)
        {
            segmentid_t segId = i;
            CreateDataForOneSegment(segId, segmentDatas[i]);
            
            SegmentInfo segInfo;
            segInfo.docCount = segmentDatas[i].size();
            WriteSegmentInfo(segId, segInfo);
        }
    }

    template<typename T>
    void TestOpen(const vector<uint32_t>& docCounts, bool update = false)
    {
        vector<T> expectedData;
        FullBuild(docCounts, expectedData);

        uint32_t segCount = docCounts.size();
	tr1::shared_ptr<SingleValueAttributeReader<T> > reader = CreateAttrReader<T>(segCount);
        CheckRead(*reader, expectedData);
        CheckIterator(*reader, expectedData);

        if (update)
        {
            map<docid_t, T> toUpdateDocs;
            MakeUpdateDocs(expectedData.size(), toUpdateDocs);
            UpdateField(*reader, toUpdateDocs, expectedData);
            CheckRead(*reader, expectedData);
            CheckIterator(*reader, expectedData);
        }
    }

    template<typename T>
    void MakeUpdateDocs(uint32_t totalDocCount, map<docid_t, T>& toUpdateDocs)
    {
        for (uint32_t i = 0; i < UPDATE_DOC_NUM; i++)
        {
            docid_t docId = rand() % totalDocCount;
            T value = (T)(docId + 10) % 128;
            toUpdateDocs[docId] = value;
        }
    }
    
    template<typename T>
    void UpdateField(SingleValueAttributeReader<T> &reader, 
                     map<docid_t, T> &toUpdateDocs,
                     vector<T> &expectedData)
    {
        typename map<docid_t, T>::iterator it = toUpdateDocs.begin();
        for (; it != toUpdateDocs.end(); it++)
        {
            docid_t docId = it->first;
            T value = it->second;
            reader.UpdateField(docId, (uint8_t *)&value, sizeof(value));
            expectedData[docId] = value;
        }
    }

    template<typename T>
    void TestOpenWithPatches(const vector<uint32_t>& fullBuildDocCounts,
                             uint32_t incSegmentCount,
                             bool update = false)
    {
        vector<T> expectedData;
        FullBuild(fullBuildDocCounts, expectedData);
        for (size_t i = 0; i < incSegmentCount; ++i)
        {
            segmentid_t segId = (segmentid_t)fullBuildDocCounts.size() + i;
            CreatePatches(fullBuildDocCounts, segId, expectedData);
            SegmentInfo segInfo;
            segInfo.docCount = 0;
            WriteSegmentInfo(segId, segInfo);
        }

        uint32_t segCount = fullBuildDocCounts.size() + incSegmentCount;
	tr1::shared_ptr<SingleValueAttributeReader<T> > reader = CreateAttrReader<T>(segCount);
        PartitionDataPtr partitionData = 
           IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segCount);
        PatchLoader::LoadAttributePatch(*reader, mAttrConfig, partitionData);
        CheckRead(*reader, expectedData);
        CheckIterator(*reader, expectedData);

        if (update)
        {
            map<docid_t, T> toUpdateDocs;
            MakeUpdateDocs(expectedData.size(), toUpdateDocs);
            UpdateField(*reader, toUpdateDocs, expectedData);
            CheckRead(*reader, expectedData);
            CheckIterator(*reader, expectedData);
        }
    }

    template <typename T>
    void GenerateData(uint32_t docCount, vector<T> &data)
    {
        for (uint32_t i = 0; i < docCount; ++i)
        {
            T value = (i + rand()) * 3 % 10;
            data.push_back(value);
        }
    }

    template<typename T>
    void CreateDataForOneSegment(segmentid_t segId, const vector<T>& data)
    {
        SingleValueAttributeWriter<T> writer(mAttrConfig);
        writer.Init(FSWriterParamDeciderPtr(), NULL);
        for (uint32_t i = 0; i < data.size(); ++i)
        {
            writer.AddField(i, data[i]);
        }

        stringstream segPath;
        segPath << mRoot << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0/";
        IndexTestUtil::ResetDir(segPath.str());

        string attrPath = segPath.str() + ATTRIBUTE_DIR_NAME + "/";
        IndexTestUtil::ResetDir(attrPath);
        AttributeWriterHelper::DumpWriter(writer, attrPath);
    }

    template<typename T>
    void CreatePatches(const vector<uint32_t>& docCounts,
                       segmentid_t outputSegId, 
                       vector<T>& expectedData)
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
    void CreateOnePatch(segmentid_t segIdToPatch,
                        docid_t baseDocIdInSegToPatch,
                        uint32_t docCountInSegToPatch,
                        segmentid_t outputSegId, 
                        vector<T>& expectedData)
    {
        stringstream ss;
        ss << mRoot << SEGMENT_FILE_NAME_PREFIX << "_" << outputSegId << "_level_0/";
        IndexTestUtil::MkDir(ss.str());
        ss << ATTRIBUTE_DIR_NAME << "/";
        IndexTestUtil::MkDir(ss.str());
        ss << mAttrConfig->GetAttrName() << "/";
        IndexTestUtil::MkDir(ss.str());
        ss << outputSegId << "_" << 
            segIdToPatch << "." << ATTRIBUTE_PATCH_FILE_NAME;
        string patchFile = ss.str();

        unique_ptr<fs::File> file(fs::FileSystem::openFile(patchFile.c_str(), WRITE)); 

        for (uint32_t j = 0; j < docCountInSegToPatch; j++)
        {
            if ((outputSegId + j) % 10 == 1)
            {
                T value = (outputSegId + j) % 23;
                expectedData[j + baseDocIdInSegToPatch] = value;
                file->write((void*)(&j), sizeof(docid_t));
                file->write((void*)(&value), sizeof(T));
            }
        }
        file->close();
    }

    void WriteSegmentInfo(segmentid_t segId, const SegmentInfo& segInfo)
    {
        stringstream segPath;
        segPath << mRoot << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0/";
        string segInfoPath = segPath.str() + SEGMENT_INFO_FILE_NAME;
        segInfo.Store(segInfoPath);
    }

    template<typename T>
    void CheckRead(const SingleValueAttributeReader<T>& reader,
                   vector<T>& expectedData)
    {
        for (docid_t i = 0; i < (docid_t)expectedData.size(); i++)
        {
            T value;
            INDEXLIB_TEST_TRUE(reader.Read(i, (uint8_t*)&value, sizeof(T)));
            INDEXLIB_TEST_EQUAL(expectedData[i], value);
        }
    }

    template<typename T>
    void CheckIterator(const SingleValueAttributeReader<T>& reader,
                       const vector<T>& expectedData)
    {
        AttributeIteratorBase* attrIter = reader.CreateIterator(&mPool);
        AttributeIteratorTyped<T>* typedIter = 
            dynamic_cast<AttributeIteratorTyped<T>* > (attrIter);
        for (docid_t i = 0; i < (docid_t)expectedData.size(); i++)
        {
            T value;
            bool ret = typedIter->Seek(i, value);
            INDEXLIB_TEST_TRUE(ret);
            INDEXLIB_TEST_EQUAL(expectedData[i], value);            
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, attrIter);
    }

    template<class T>
    tr1::shared_ptr<SingleValueAttributeReader<T> > CreateAttrReader(uint32_t segCount)
    {
        PartitionDataPtr partitionData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segCount);
	tr1::shared_ptr<SingleValueAttributeReader<T> > reader;
        reader.reset(new SingleValueAttributeReader<T>);
        reader->Open(mAttrConfig, partitionData);
        return reader;
    }

private:
    string mRoot;
    AttributeConfigPtr mAttrConfig;
    bool mSetCompress;
    autil::mem_pool::Pool mPool;
    static const uint32_t UPDATE_DOC_NUM = 20;
};

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeReaderTest, TestOpen);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeReaderTest, TestOpenForEqualCompress);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeReaderTest, TestOpenWithPatchForUInt32);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeReaderTest, TestCaseForUpdateFieldWithoutPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeReaderTest, TestCaseForUpdateFieldWithPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeReaderTest, TestCaseForReadBuildingSegmentReader);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeReaderTest, TestCaseForUpdateBuildingSegmentReader);
    
IE_NAMESPACE_END(index);
