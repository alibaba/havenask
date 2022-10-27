#include "indexlib/index/normal/attribute/test/attribute_fs_writer_param_decider_unittest.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeFsWriterParamDeciderTest);

AttributeFsWriterParamDeciderTest::AttributeFsWriterParamDeciderTest()
{
}

AttributeFsWriterParamDeciderTest::~AttributeFsWriterParamDeciderTest()
{
}

void AttributeFsWriterParamDeciderTest::CaseSetUp()
{
}

void AttributeFsWriterParamDeciderTest::CaseTearDown()
{
}

void AttributeFsWriterParamDeciderTest::TestMakeParamForSingleAttribute()
{
    AttributeConfigPtr attrConfig =
        AttributeTestUtil::CreateAttrConfig<int64_t>(false);
    FSWriterParam param(false, false);

    // ct_normal
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_normal, "data", param);

    // ct_pk
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_pk, "data", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_pk, "data", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_pk, "data", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_pk, "data", param);

    // ct_section
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_section, "data", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_section, "data", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_section, "data", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_section, "data", param);


    param.copyOnDump = true;
    // ct_normal, online sync
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
}


void AttributeFsWriterParamDeciderTest::TestMakeParamForVarNumAttribute()
{
    FSWriterParam param(false, false);
    AttributeConfigPtr attrConfig =
        AttributeTestUtil::CreateAttrConfig<int64_t>(true);

    // ct_normal
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);

    // ct_pk
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_pk, "data", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_pk, "data", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_pk, "data", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_pk, "data", param);
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_pk, "offset", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_pk, "offset", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_pk, "offset", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_pk, "offset", param);


    // ct_section
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_section, "data", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_section, "data", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_section, "data", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_section, "data", param);
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_section, "offset", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_section, "offset", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_section, "offset", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_section, "offset", param);
}

void AttributeFsWriterParamDeciderTest::TestMakeParamForUpdatableVarNumAttribute()
{
    FSWriterParam param(false, false);
    AttributeConfigPtr attrConfig =
        AttributeTestUtil::CreateAttrConfig<int64_t>(true);
    attrConfig->GetFieldConfig()->SetUpdatableMultiValue(true);

    // ct_normal
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);

    // ct_pk
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_pk, "data", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_pk, "data", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_pk, "data", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_pk, "data", param);
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_pk, "offset", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_pk, "offset", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_pk, "offset", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_pk, "offset", param);


    // ct_section
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_section, "data", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_section, "data", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_section, "data", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_section, "data", param);
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_section, "offset", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_section, "offset", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_section, "offset", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_section, "offset", param);


    // ct_normal, updatable var num attribute, onlineSync = true
    param.copyOnDump = true;
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
}

void AttributeFsWriterParamDeciderTest::TestMakeParamForSingleString()
{
    FSWriterParam param(false, false);
    AttributeConfigPtr attrConfig =
        AttributeTestUtil::CreateAttrConfig<MultiChar>(false);
    attrConfig->GetFieldConfig()->SetUpdatableMultiValue(false);

    // ct_normal
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);

    attrConfig->GetFieldConfig()->SetUpdatableMultiValue(true);
    param.copyOnDump = true;
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
}

void AttributeFsWriterParamDeciderTest::TestMakeParamForMultiString()
{
    FSWriterParam param(false, false);
    AttributeConfigPtr attrConfig =
        AttributeTestUtil::CreateAttrConfig<MultiChar>(true);
    attrConfig->GetFieldConfig()->SetUpdatableMultiValue(false);

    // ct_normal
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_normal, "data", param);
    InnerTestMakeParam(false, true, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
    InnerTestMakeParam(false, false, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
    InnerTestMakeParam(true, false, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);

    attrConfig->GetFieldConfig()->SetUpdatableMultiValue(true);
    param.copyOnDump = true;
    InnerTestMakeParam(true, true, attrConfig,
                       AttributeConfig::ct_normal, "offset", param);
}

void AttributeFsWriterParamDeciderTest::InnerTestMakeParam(
    bool isOnline, bool needFlushRt,
    const AttributeConfigPtr& attrConfig,
    AttributeConfig::ConfigType confType,
    const string& fileName,
    const FSWriterParam& expectParam)
{
    IndexPartitionOptions options;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = needFlushRt;
    options.SetIsOnline(isOnline);

    attrConfig->SetConfigType(confType);
    AttributeFSWriterParamDecider decider(attrConfig, options);
    FSWriterParam param = decider.MakeParam(fileName);
    ASSERT_EQ(expectParam.copyOnDump, param.copyOnDump);
    ASSERT_EQ(expectParam.atomicDump, param.atomicDump);

    param = decider.MakeParam("not_valid_data");
    ASSERT_FALSE(param.copyOnDump);
    ASSERT_FALSE(param.copyOnDump);
}

IE_NAMESPACE_END(index);

