#include "indexlib/index/normal/attribute/test/fixed_value_attribute_merger_unittest.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/file_system/directory_creator.h"

using namespace std;
using namespace std;
using testing::Return;
using testing::_;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(attribute, FixedValueAttributeMergerTest);

FixedValueAttributeMergerTest::FixedValueAttributeMergerTest()
{
}

FixedValueAttributeMergerTest::~FixedValueAttributeMergerTest()
{
}

void FixedValueAttributeMergerTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void FixedValueAttributeMergerTest::CaseTearDown()
{
}

void FixedValueAttributeMergerTest::TestFlushBuffer()
{
    MergerResource resource;
    resource.targetSegmentCount = 1;

    OutputSegmentMergeInfo info;
    info.targetSegmentIndex = 0;
    info.path = mRootDir + "/";
    info.directory = DirectoryCreator::Create(info.path);
    {
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig("field1", 
                        ft_integer, false, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        MockFixedValueAttributeMerger merger;
        ReclaimMapPtr reclaimMap;
        merger.Init(attrConfig);
        merger.PrepareOutputDatas(reclaimMap, { info });
        merger.mSegOutputMapper.mOutputs[0].bufferCursor = 1;

        EXPECT_CALL(merger, FlushCompressDataBuffer(merger.mSegOutputMapper.GetOutputs()[0]))
            .WillOnce(Return());
        SegmentMergeInfos infos;
        merger.FlushDataBuffer(merger.mSegOutputMapper.mOutputs[0]);
        merger.CloseFiles();
    }

    {
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig("field2", ft_integer, false, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        MockFixedValueAttributeMerger merger;
        merger.Init(attrConfig);

        EXPECT_CALL(merger, DumpCompressDataBuffer())
            .Times(0);
        SegmentMergeInfos infos;
        // TODO: ???
        // merger.FlushDataBuffer(merger.mOutputDatas[0]);
        merger.CloseFiles();
    }

    {
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig("field2", ft_integer, true, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        MockFixedValueAttributeMerger merger;
        merger.Init(attrConfig);
        ReclaimMapPtr reclaimMap;
        merger.PrepareOutputDatas(reclaimMap, { info });
        merger.mSegOutputMapper.mOutputs[0].bufferCursor = 1;

        EXPECT_CALL(merger, DumpCompressDataBuffer())
            .Times(0);
        SegmentMergeInfos infos;
        merger.FlushDataBuffer(merger.mSegOutputMapper.mOutputs[0]);
        merger.CloseFiles();        
        merger.DestroyBuffers();
    }
}

void FixedValueAttributeMergerTest::TestMergeData()
{
    MergerResource resource;
    resource.targetSegmentCount = 1;

    OutputSegmentMergeInfo info;
    info.targetSegmentIndex = 0;
    info.path = mRootDir + "/";
    info.directory = DirectoryCreator::Create(info.path);
    {
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig("field1", ft_integer, false, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        MockFixedValueAttributeMerger merger;
        merger.Init(attrConfig);

        EXPECT_CALL(merger, DumpCompressDataBuffer())
            .WillOnce(Return());
        SegmentMergeInfos infos;
        merger.MergeData(resource, infos, { info });
    }

    {
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig("field2", ft_integer, true, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        MockFixedValueAttributeMerger merger;
        merger.Init(attrConfig);

        EXPECT_CALL(merger, DumpCompressDataBuffer())
            .Times(0);
        SegmentMergeInfos infos;
        merger.MergeData(resource, infos, { info });
    }
}

IE_NAMESPACE_END(index);

