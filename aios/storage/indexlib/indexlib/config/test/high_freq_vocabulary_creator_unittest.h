#pragma once

#include "autil/Log.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace config {

class HighFreqVocabularyCreatorTest : public INDEXLIB_TESTBASE
{
public:
    HighFreqVocabularyCreatorTest();
    ~HighFreqVocabularyCreatorTest();

    DECLARE_CLASS_NAME(HighFreqVocabularyCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreateVocabulary();
    void TestLoadAdaptiveVocabularyWithDir();

private:
    void PrepareAdaptiveDictFile(const std::string& fileName, bool useArchiveFile = true);

private:
    std::shared_ptr<file_system::IDirectory> mRootDir;
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HighFreqVocabularyCreatorTest, TestCreateVocabulary);
INDEXLIB_UNIT_TEST_CASE(HighFreqVocabularyCreatorTest, TestLoadAdaptiveVocabularyWithDir);
}} // namespace indexlib::config
