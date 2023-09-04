#include "indexlib/index/inverted_index/format/TermMetaLoader.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/MemDirectory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class TermMetaLoaderTest : public TESTBASE
{
public:
    void setUp() override
    {
        file_system::FileSystemOptions fileSystemOptions;
        fileSystemOptions.needFlush = true;
        std::string caseDir = GET_TEMP_DATA_PATH();
        file_system::FslibWrapper::DeleteDirE(caseDir, file_system::DeleteOption::NoFence(true));
        file_system::FslibWrapper::MkDirE(caseDir);

        util::MemoryQuotaControllerPtr mqc(new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        util::PartitionMemoryQuotaControllerPtr quotaController(new util::PartitionMemoryQuotaController(mqc));
        fileSystemOptions.memoryQuotaController = quotaController;
        _fileSystem = file_system::FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                                             /*rootPath=*/caseDir, fileSystemOptions,
                                                             std::shared_ptr<util::MetricProvider>(),
                                                             /*isOverride=*/false)
                          .GetOrThrow();
        std::shared_ptr<file_system::Directory> directory =
            file_system::IDirectory::ToLegacyDirectory(std::make_shared<file_system::MemDirectory>("", _fileSystem));
        _rootDir = directory->MakeDirectory("segment/index/");
        _fileSystem->Sync(true).GetOrThrow();
    }
    void tearDown() override {}

private:
    std::shared_ptr<file_system::Directory> _rootDir;
    std::shared_ptr<file_system::IFileSystem> _fileSystem;
};

TEST_F(TermMetaLoaderTest, testStoreSize)
{
    {
        TermMeta termMeta(1, 2, 3);
        TermMetaDumper tmDumper;
        // docFreq < 127 compressSize = 1,
        // totalTermFreq < 127 compressSize = 1
        ASSERT_EQ((uint32_t)6, tmDumper.CalculateStoreSize(termMeta));
    }
    {
        TermMeta termMeta(128, 2, 3);
        TermMetaDumper tmDumper;
        // docFreq > 127 compressSize = 2,
        // totalTermFreq < 127 compressSize = 1
        ASSERT_EQ((uint32_t)7, tmDumper.CalculateStoreSize(termMeta));
    }
}

TEST_F(TermMetaLoaderTest, testDump)
{
    TermMeta termMeta(1, 2, 3);
    file_system::FileWriterPtr postingFileWriter = _rootDir->CreateFileWriter(POSTING_FILE_NAME);

    TermMetaDumper tmDumper;
    tmDumper.Dump(postingFileWriter, termMeta);

    ASSERT_EQ((int64_t)tmDumper.CalculateStoreSize(termMeta), postingFileWriter->GetLength());
    ASSERT_EQ(file_system::FSEC_OK, postingFileWriter->Close());
    _rootDir->RemoveFile(POSTING_FILE_NAME);
}

TEST_F(TermMetaLoaderTest, testLoad)
{
    TermMeta oldTermMeta(1, 2, 3);
    std::shared_ptr<file_system::FileWriter> postingFileWriter = _rootDir->CreateFileWriter(POSTING_FILE_NAME);
    TermMetaDumper tmDumper;
    tmDumper.Dump(postingFileWriter, oldTermMeta);
    ASSERT_EQ(file_system::FSEC_OK, postingFileWriter->Close());
    {
        std::shared_ptr<file_system::FileReader> reader =
            _rootDir->CreateFileReader(POSTING_FILE_NAME, file_system::FSOT_BUFFERED);

        TermMeta newTermMeta;
        TermMetaLoader tmLoader;
        tmLoader.Load(reader, newTermMeta);
        ASSERT_EQ(oldTermMeta._docFreq, newTermMeta._docFreq);
        ASSERT_EQ(oldTermMeta._totalTermFreq, newTermMeta._totalTermFreq);
        ASSERT_EQ(oldTermMeta._payload, newTermMeta._payload);
        ASSERT_EQ(file_system::FSEC_OK, reader->Close());
    }

    // test cursor load
    std::string fileContent;
    _rootDir->Load(POSTING_FILE_NAME, fileContent);
    {
        uint8_t* dataCursor = (uint8_t*)fileContent.c_str();
        size_t leftSize = fileContent.size();

        TermMeta newTermMeta;
        TermMetaLoader tmLoader;
        tmLoader.Load(dataCursor, leftSize, newTermMeta);
        ASSERT_TRUE(oldTermMeta == newTermMeta);
    }

    {
        uint8_t* dataCursor = (uint8_t*)fileContent.c_str();
        size_t leftSize = fileContent.size();

        TermMeta newTermMeta;
        TermMetaLoader tmLoader;
        tmLoader.Load(dataCursor, leftSize, newTermMeta);

        ASSERT_EQ(oldTermMeta._docFreq, newTermMeta._docFreq);
        ASSERT_EQ(oldTermMeta._totalTermFreq, newTermMeta._totalTermFreq);
        ASSERT_EQ(oldTermMeta._payload, newTermMeta._payload);
    }
}

} // namespace indexlib::index
