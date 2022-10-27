#include <autil/StringUtil.h>
#include "indexlib/index/normal/attribute/test/single_attribute_segment_reader_for_merge_unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/merger/segment_directory.h"

using namespace autil;
using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SingleAttributeSegmentReaderForMergeTest);

SingleAttributeSegmentReaderForMergeTest::SingleAttributeSegmentReaderForMergeTest()
{
}

SingleAttributeSegmentReaderForMergeTest::~SingleAttributeSegmentReaderForMergeTest()
{
}

void SingleAttributeSegmentReaderForMergeTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void SingleAttributeSegmentReaderForMergeTest::CaseTearDown()
{
}

void SingleAttributeSegmentReaderForMergeTest::TestOpenWithPatch()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    string docsStr = "0,1,2,3,4#5,6,7,update_field:0:8#"
                     "9,10,update_field:1:11,update_field:5:12,update_field:0:13" ;
    provider.Build(docsStr, SFP_OFFLINE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    CheckReaderLoadPatch(attrConfig, 0, "13,11,2,3,4");
    CheckReaderLoadPatch(attrConfig, 1, "12,6,7");
    CheckReaderLoadPatch(attrConfig, 2, "9,10");
}

void SingleAttributeSegmentReaderForMergeTest::CheckReaderLoadPatch(
        const AttributeConfigPtr& attrConfig,
        segmentid_t segId, const string& expectResults)
{
    
    DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    Version version;
    VersionLoader::GetVersion(directory, version, INVALID_VERSION);
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(mRootDir, version));
    segDir->Init(false);

    SegmentInfo segmentInfo;
    stringstream ss;
    ss << "segment_" << segId << "_level_0";
    segmentInfo.Load(directory->GetDirectory(ss.str(), true));

    SingleAttributeSegmentReaderForMerge<int32_t> segmentReader(attrConfig);
    segmentReader.Open((index::SegmentDirectoryBasePtr)segDir, segmentInfo, segId);

    vector<int32_t> expectValues;
    StringUtil::fromString(expectResults, expectValues, ",");

    ASSERT_EQ((size_t)segmentInfo.docCount, expectValues.size());
    for (docid_t i = 0; i < (docid_t)expectValues.size(); i++)
    {
        int32_t value;
        segmentReader.Read(i, value);
        ASSERT_EQ(expectValues[i], value);
    }
}

void SingleAttributeSegmentReaderForMergeTest::TestOpenWithoutPatch()
{
    uint32_t data[2] = {1, 2};
    string filePath = FileSystemWrapper::JoinPath(mRootDir, "data");
    string content((char*)data, sizeof(data));
    FileSystemWrapper::AtomicStore(filePath, content);

    {
        //TestNeedCompressData
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig(
                        "field", ft_integer, false, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        SingleAttributeSegmentReaderForMerge<uint32_t> reader(attrConfig);
        SegmentInfo segInfo;
        segInfo.docCount = 1;
        ASSERT_NO_THROW(reader.OpenWithoutPatch(
                        GET_PARTITION_DIRECTORY(), segInfo, 0));
        ASSERT_EQ((uint32_t)1, reader.mDocCount);
    }

    {
        //Test not CompressData
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig(
                        "field", ft_integer, true, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        SingleAttributeSegmentReaderForMerge<uint32_t> reader(attrConfig);
        SegmentInfo segInfo;
        segInfo.docCount = 2;
        ASSERT_NO_THROW(reader.OpenWithoutPatch(
                        GET_PARTITION_DIRECTORY(), segInfo, 0));
        ASSERT_EQ((uint32_t)2, reader.mDocCount);
    }
    
}

IE_NAMESPACE_END(index);

