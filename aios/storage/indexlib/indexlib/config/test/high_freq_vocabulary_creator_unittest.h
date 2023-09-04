#ifndef __INDEXLIB_HIGHFREQVOCABULARYCREATORTEST_H
#define __INDEXLIB_HIGHFREQVOCABULARYCREATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

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
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HighFreqVocabularyCreatorTest, TestCreateVocabulary);
INDEXLIB_UNIT_TEST_CASE(HighFreqVocabularyCreatorTest, TestLoadAdaptiveVocabularyWithDir);
}} // namespace indexlib::config

#endif //__INDEXLIB_HIGHFREQVOCABULARYCREATORTEST_H
