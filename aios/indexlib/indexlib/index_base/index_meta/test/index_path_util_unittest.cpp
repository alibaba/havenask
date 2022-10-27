#include <autil/StringUtil.h>
#include "indexlib/index_base/index_meta/test/index_path_util_unittest.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, IndexPathUtilTest);

IndexPathUtilTest::IndexPathUtilTest()
{
}

IndexPathUtilTest::~IndexPathUtilTest()
{
}

void IndexPathUtilTest::CaseSetUp()
{
}

void IndexPathUtilTest::CaseTearDown()
{
}

void IndexPathUtilTest::TestGetSegmentId()
{
    segmentid_t segId;
    ASSERT_TRUE(IndexPathUtil::GetSegmentId("segment_0", segId) && segId == 0);
    ASSERT_TRUE(IndexPathUtil::GetSegmentId("segment_12", segId) && segId == 12);
    ASSERT_TRUE(IndexPathUtil::GetSegmentId("segment_0_level_1", segId) && segId == 0);
    ASSERT_TRUE(IndexPathUtil::GetSegmentId("segment_12_level_0", segId) && segId == 12);

    ASSERT_FALSE(IndexPathUtil::GetSegmentId("segment__12_level_0", segId));
    ASSERT_FALSE(IndexPathUtil::GetSegmentId("segment_12_level_a", segId));
    ASSERT_FALSE(IndexPathUtil::GetSegmentId("segment_1a_level_0", segId));
    ASSERT_FALSE(IndexPathUtil::GetSegmentId("segment_12_level_0 ", segId));
    ASSERT_FALSE(IndexPathUtil::GetSegmentId("segment_12_level_0_", segId));
    ASSERT_FALSE(IndexPathUtil::GetSegmentId("/segment_12_level_0", segId));
    ASSERT_FALSE(IndexPathUtil::GetSegmentId("segment_12_level_0/", segId));
    ASSERT_FALSE(IndexPathUtil::GetSegmentId("schema.json", segId));
}

void IndexPathUtilTest::TestGetVersionId()
{
    versionid_t vid;
    ASSERT_TRUE(IndexPathUtil::GetVersionId("version.0", vid) && vid == 0);
    ASSERT_TRUE(IndexPathUtil::GetVersionId("version.93", vid) && vid == 93);
    
    ASSERT_FALSE(IndexPathUtil::GetVersionId("version.0 ", vid));
    ASSERT_FALSE(IndexPathUtil::GetVersionId("version_0", vid));
    ASSERT_FALSE(IndexPathUtil::GetVersionId("version_0_", vid));
    ASSERT_FALSE(IndexPathUtil::GetVersionId("version_0a", vid));
    ASSERT_FALSE(IndexPathUtil::GetVersionId("version__0", vid));
    ASSERT_FALSE(IndexPathUtil::GetVersionId("version_-2", vid));
}

void IndexPathUtilTest::TestGetDeployMetaId()
{
    versionid_t vid;
    ASSERT_TRUE(IndexPathUtil::GetDeployMetaId("deploy_meta.0", vid) && vid == 0);
    ASSERT_TRUE(IndexPathUtil::GetDeployMetaId("deploy_meta.93", vid) && vid == 93);
    
    ASSERT_FALSE(IndexPathUtil::GetDeployMetaId("deploy_meta.0 ", vid));
    ASSERT_FALSE(IndexPathUtil::GetDeployMetaId("deploy_meta_0", vid));
    ASSERT_FALSE(IndexPathUtil::GetDeployMetaId("deploy_meta_0_", vid));
    ASSERT_FALSE(IndexPathUtil::GetDeployMetaId("deploy_meta_0a", vid));
    ASSERT_FALSE(IndexPathUtil::GetDeployMetaId("deploy_meta__0", vid));
    ASSERT_FALSE(IndexPathUtil::GetDeployMetaId("deploy_meta_-2", vid));
}

void IndexPathUtilTest::TestGetPatchMetaId()
{
    versionid_t vid;
    ASSERT_TRUE(IndexPathUtil::GetPatchMetaId("patch_meta.0", vid) && vid == 0);
    ASSERT_TRUE(IndexPathUtil::GetPatchMetaId("patch_meta.93", vid) && vid == 93);
    
    ASSERT_FALSE(IndexPathUtil::GetPatchMetaId("patch_meta.0 ", vid));
    ASSERT_FALSE(IndexPathUtil::GetPatchMetaId("patch_meta_0", vid));
    ASSERT_FALSE(IndexPathUtil::GetPatchMetaId("patch_meta_0_", vid));
    ASSERT_FALSE(IndexPathUtil::GetPatchMetaId("patch_meta_0a", vid));
    ASSERT_FALSE(IndexPathUtil::GetPatchMetaId("patch_meta__0", vid));
    ASSERT_FALSE(IndexPathUtil::GetPatchMetaId("patch_meta_-2", vid));
}

void IndexPathUtilTest::TestGetSchemaId()
{
    schemavid_t sid;
    ASSERT_TRUE(IndexPathUtil::GetSchemaId("schema.json", sid) && sid == DEFAULT_SCHEMAID);
    ASSERT_TRUE(IndexPathUtil::GetSchemaId("schema.json.0", sid) && sid == 0);
    ASSERT_TRUE(IndexPathUtil::GetSchemaId("schema.json.93", sid) && sid == 93);
    
    ASSERT_FALSE(IndexPathUtil::GetSchemaId("schema.json.", sid));
    ASSERT_FALSE(IndexPathUtil::GetSchemaId("schema.json.a", sid));
    ASSERT_FALSE(IndexPathUtil::GetSchemaId("_schema.json", sid));
    ASSERT_FALSE(IndexPathUtil::GetSchemaId("schema.json.-3", sid));
}

void IndexPathUtilTest::TestGetPatchIndexId()
{
    schemavid_t sid;    
    ASSERT_TRUE(IndexPathUtil::GetPatchIndexId("patch_index_0", sid) && sid == 0);
    ASSERT_TRUE(IndexPathUtil::GetPatchIndexId("patch_index_93", sid) && sid == 93);
    
    ASSERT_FALSE(IndexPathUtil::GetPatchIndexId("patch_index_0 ", sid));
    ASSERT_FALSE(IndexPathUtil::GetPatchIndexId("patch_index", sid));
    ASSERT_FALSE(IndexPathUtil::GetPatchIndexId("patch_index_", sid));
    ASSERT_FALSE(IndexPathUtil::GetPatchIndexId("patch_index_a", sid));
    ASSERT_FALSE(IndexPathUtil::GetPatchIndexId("patch_index_-3", sid));
    ASSERT_FALSE(IndexPathUtil::GetPatchIndexId("_patch_index_3", sid));
    ASSERT_FALSE(IndexPathUtil::GetPatchIndexId("patch_index_3/", sid));
}

IE_NAMESPACE_END(index_base);
