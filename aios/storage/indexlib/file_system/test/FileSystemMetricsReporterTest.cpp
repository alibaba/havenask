#include "indexlib/file_system/FileSystemMetricsReporter.h"

#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class FileSystemMetricsReporterTest : public INDEXLIB_TESTBASE
{
public:
    FileSystemMetricsReporterTest();
    ~FileSystemMetricsReporterTest();

    DECLARE_CLASS_NAME(FileSystemMetricsReporterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FileSystemMetricsReporterTest, TestSimpleProcess);
AUTIL_LOG_SETUP(indexlib.file_system, FileSystemMetricsReporterTest);

FileSystemMetricsReporterTest::FileSystemMetricsReporterTest() {}

FileSystemMetricsReporterTest::~FileSystemMetricsReporterTest() {}

void FileSystemMetricsReporterTest::CaseSetUp() {}

void FileSystemMetricsReporterTest::CaseTearDown() {}

void FileSystemMetricsReporterTest::TestSimpleProcess()
{
    util::MetricProviderPtr metricProvider;
    FileSystemMetricsReporter reporter(metricProvider);
    {
        string path = "segment_0_level_1/index/phrase/dictionary";
        map<string, string> tags;
        reporter.ExtractTagInfoFromPath(path, tags);

        ASSERT_EQ(string("dictionary"), tags["file_name"]);
        ASSERT_EQ(string("0"), tags["segment_id"]);
        ASSERT_EQ(string("index"), tags["data_type"]);
        ASSERT_EQ(string("phrase"), tags["identifier"]);
    }

    {
        string path = "segment_0_level_1/index/phrase/section/dictionary";
        map<string, string> tags;
        reporter.ExtractTagInfoFromPath(path, tags);

        ASSERT_EQ(string("dictionary"), tags["file_name"]);
        ASSERT_EQ(string("0"), tags["segment_id"]);
        ASSERT_EQ(string("index"), tags["data_type"]);
        ASSERT_EQ(string("phrase-section"), tags["identifier"]);
    }

    {
        string path = "segment_0_level_1/dictionary";
        map<string, string> tags;
        reporter.ExtractTagInfoFromPath(path, tags);

        ASSERT_EQ(string("dictionary"), tags["file_name"]);
        ASSERT_EQ(string("0"), tags["segment_id"]);
        ASSERT_EQ(string("_none_"), tags["data_type"]);
        ASSERT_EQ(string("_none_"), tags["identifier"]);
    }

    {
        string path = "version.0";
        map<string, string> tags;
        reporter.ExtractTagInfoFromPath(path, tags);

        ASSERT_EQ(string("version.0"), tags["file_name"]);
        ASSERT_EQ(string("-1"), tags["segment_id"]);
        ASSERT_EQ(string("_none_"), tags["data_type"]);
        ASSERT_EQ(string("_none_"), tags["identifier"]);
    }

    {
        string path = "truncate_meta/meta_file";
        map<string, string> tags;
        reporter.ExtractTagInfoFromPath(path, tags);

        ASSERT_EQ(string("meta_file"), tags["file_name"]);
        ASSERT_EQ(string("-1"), tags["segment_id"]);
        ASSERT_EQ(string("truncate_meta"), tags["data_type"]);
        ASSERT_EQ(string("_none_"), tags["identifier"]);
    }

    {
        string path = "truncate_meta/dir1/dir2/meta_file";
        map<string, string> tags;
        reporter.ExtractTagInfoFromPath(path, tags);

        ASSERT_EQ(string("meta_file"), tags["file_name"]);
        ASSERT_EQ(string("-1"), tags["segment_id"]);
        ASSERT_EQ(string("truncate_meta"), tags["data_type"]);
        ASSERT_EQ(string("dir1-dir2"), tags["identifier"]);
    }
}

}} // namespace indexlib::file_system
