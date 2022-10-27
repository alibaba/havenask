#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_meta_reader.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);

class TruncateMetaReaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TruncateMetaReaderTest);
    void CaseSetUp() override
    {
        string rootDir = GET_TEST_DATA_PATH();
        mFileName = storage::FileSystemWrapper::JoinPath(rootDir, "truncate_meta");
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForSimple()
    {
        map<dictkey_t, int64_t> answer;
        GenData(1000, answer);

        TruncateMetaReader reader(true);
        reader.Open(mFileName);

        INDEXLIB_TEST_EQUAL(answer.size(), reader.Size());

        int64_t expectedMax = numeric_limits<int64_t>::max();
        
        map<dictkey_t, int64_t>::iterator it = answer.begin();
        for (; it != answer.end(); it++)
        {
            int64_t min, max;
            INDEXLIB_TEST_TRUE(reader.Lookup(it->first, min, max));
            INDEXLIB_TEST_EQUAL(it->second, min);
            INDEXLIB_TEST_EQUAL(expectedMax, max);
        }
    }



private:
    void GenData(uint32_t lineNum, map<dictkey_t, int64_t> &answer)
    {
        BufferedFileWriterPtr writer(new BufferedFileWriter());
        writer->Open(mFileName);

        for (uint32_t i = 0; i < lineNum; i++)
        {
            dictkey_t key = i;
            int64_t value = rand();

            string keyStr = StringUtil::toString(key);
            string valueStr = StringUtil::toString(value);
            string line = keyStr + "\t" + valueStr + "\n";
            writer->Write(line.c_str(), line.size());
            answer[key] = value;
        }
        writer->Close();
    }

private:
    string mFileName;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, TruncateMetaReaderTest);

INDEXLIB_UNIT_TEST_CASE(TruncateMetaReaderTest, TestCaseForSimple);

IE_NAMESPACE_END(index);
