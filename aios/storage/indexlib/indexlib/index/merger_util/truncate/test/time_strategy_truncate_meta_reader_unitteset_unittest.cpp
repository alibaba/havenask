#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/merger_util/truncate/time_strategy_truncate_meta_reader.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;

namespace indexlib::index::legacy {

class TimeStrategyTruncateMetaReaderUnittesetTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TimeStrategyTruncateMetaReaderUnittesetTest);
    void CaseSetUp() override
    {
        string rootDir = GET_TEMP_DATA_PATH();
        mFileName = util::PathUtil::JoinPath(rootDir, "truncate_meta");
    }

    void CaseTearDown() override {}

    void TestCaseForSimple()
    {
        InnerTestForSimple(182735, 283716, true);
        InnerTestForSimple(18273531, 28371681, false);
    }

    void InnerTestForSimple(int64_t minTime, int64_t maxTime, bool desc)
    {
        map<dictkey_t, int64_t> answer;
        GenData(1000, minTime, maxTime, answer, desc);

        auto fileReader = GET_PARTITION_DIRECTORY()->CreateFileReader("truncate_meta", file_system::FSOT_BUFFERED);
        TimeStrategyTruncateMetaReader reader(minTime, maxTime, desc);
        reader.Open(fileReader);

        INDEXLIB_TEST_EQUAL(answer.size(), reader.Size());

        map<dictkey_t, int64_t>::iterator it = answer.begin();
        for (; it != answer.end(); it++) {
            int64_t min, max;
            INDEXLIB_TEST_TRUE(reader.Lookup(index::DictKeyInfo(it->first), min, max));
            if (!desc) {
                INDEXLIB_TEST_EQUAL(minTime, min);
                INDEXLIB_TEST_EQUAL(it->second, max);
            } else {
                INDEXLIB_TEST_EQUAL(it->second, min);
                INDEXLIB_TEST_EQUAL(maxTime, max);
            }
        }
    }

private:
    void GenData(uint32_t lineNum, int64_t minTime, int64_t maxTime, map<dictkey_t, int64_t>& answer, bool desc)
    {
        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        GET_PARTITION_DIRECTORY()->RemoveFile("truncate_meta", removeOption);
        auto writer = GET_PARTITION_DIRECTORY()->CreateFileWriter("truncate_meta");
        for (uint32_t i = 0; i < lineNum; i++) {
            dictkey_t key = i;
            int64_t value = rand();

            string keyStr = StringUtil::toString(key);
            string valueStr = StringUtil::toString(value);
            string line = keyStr + "\t" + valueStr + "\n";
            writer->Write(line.c_str(), line.size()).GetOrThrow();

            if (!desc) {
                if (value < maxTime) {
                    value = maxTime;
                }
            } else {
                if (value > minTime) {
                    value = minTime;
                }
            }
            answer[key] = value;
        }
        ASSERT_EQ(FSEC_OK, writer->Close());
    }

private:
    string mFileName;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, TimeStrategyTruncateMetaReaderUnittesetTest);

INDEXLIB_UNIT_TEST_CASE(TimeStrategyTruncateMetaReaderUnittesetTest, TestCaseForSimple);
} // namespace indexlib::index::legacy
