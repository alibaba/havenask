#ifndef __INDEXLIB_DEPLOYINDEXMETATEST_H
#define __INDEXLIB_DEPLOYINDEXMETATEST_H

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/version_maker.h"

namespace indexlib { namespace index_base {

class DeployIndexMetaTest : public INDEXLIB_TESTBASE
{
public:
    DeployIndexMetaTest();
    ~DeployIndexMetaTest();

    DECLARE_CLASS_NAME(DeployIndexMetaTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDeployFileMeta();
    void TestDeployIndexMeta();
    void TestLegacyDeployIndexMetaFile();
    void CreateFiles(const std::string& dir, const std::string& filePaths);
    void CreateFiles(const file_system::DirectoryPtr& dir, const std::string& filePaths);
    void StoreDefaultVersionContent(const file_system::DirectoryPtr& dir) const;

private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DeployIndexMetaTest, TestDeployFileMeta);
INDEXLIB_UNIT_TEST_CASE(DeployIndexMetaTest, TestDeployIndexMeta);
INDEXLIB_UNIT_TEST_CASE(DeployIndexMetaTest, TestLegacyDeployIndexMetaFile);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_DEPLOYINDEXMETATEST_H
