#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/merger_util/truncate/truncate_meta_reader.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;

namespace indexlib::index::legacy {

class TruncateMetaReaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TruncateMetaReaderTest);
    void CaseSetUp() override
    {
        string rootDir = GET_TEMP_DATA_PATH();
        mFileName = util::PathUtil::JoinPath(rootDir, "truncate_meta");
    }

    void CaseTearDown() override {}

    void TestCaseForSimple()
    {
        map<dictkey_t, int64_t> answer;
        GenData(1000, answer);

        auto fileReader = GET_PARTITION_DIRECTORY()->CreateFileReader("truncate_meta", file_system::FSOT_BUFFERED);
        TruncateMetaReader reader(true);
        reader.Open(fileReader);

        INDEXLIB_TEST_EQUAL(answer.size(), reader.Size());

        int64_t expectedMax = numeric_limits<int64_t>::max();

        map<dictkey_t, int64_t>::iterator it = answer.begin();
        for (; it != answer.end(); it++) {
            int64_t min, max;
            INDEXLIB_TEST_TRUE(reader.Lookup(index::DictKeyInfo(it->first), min, max));
            INDEXLIB_TEST_EQUAL(it->second, min);
            INDEXLIB_TEST_EQUAL(expectedMax, max);
        }
    }

private:
    void GenData(uint32_t lineNum, map<dictkey_t, int64_t>& answer)
    {
        auto writer = GET_PARTITION_DIRECTORY()->CreateFileWriter("truncate_meta");
        for (uint32_t i = 0; i < lineNum; i++) {
            dictkey_t key = i;
            int64_t value = rand();

            string keyStr = StringUtil::toString(key);
            string valueStr = StringUtil::toString(value);
            string line = keyStr + "\t" + valueStr + "\n";
            writer->Write(line.c_str(), line.size()).GetOrThrow();
            answer[key] = value;
        }
        ASSERT_EQ(FSEC_OK, writer->Close());
    }

private:
    string mFileName;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, TruncateMetaReaderTest);

INDEXLIB_UNIT_TEST_CASE(TruncateMetaReaderTest, TestCaseForSimple);
} // namespace indexlib::index::legacy
