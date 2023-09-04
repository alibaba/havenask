#include "indexlib/index/kkv/dump/PKeyDumper.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/kkv/pkey_table/ClosedHashPrefixKeyTableIterator.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableBase.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableCreator.h"
#include "indexlib/index/kkv/pkey_table/SeparateChainPrefixKeyTable.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace std;
using namespace indexlib::file_system;
namespace indexlibv2::index {

class PKeyDumperTest : public TESTBASE
{
public:
    PKeyDumperTest() {}
    ~PKeyDumperTest() {}

public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
        auto rootDiretory = indexlib::file_system::Directory::Get(fs);
        _dumpDirectory =
            rootDiretory->MakeDirectory("PKeyDumperTest", indexlib::file_system::DirectoryOption())->GetIDirectory();
        ASSERT_NE(nullptr, _dumpDirectory);
    }
    void tearDown() override {}

protected:
    std::shared_ptr<indexlib::file_system::IDirectory> _dumpDirectory;
};

TEST_F(PKeyDumperTest, TestGeneral)
{
    indexlib::config::KKVIndexPreference indexPreference;
    size_t totalPKeyCount = 1024;
    PKeyDumper dumper;
    ASSERT_TRUE(dumper.Init(_dumpDirectory, indexPreference, totalPKeyCount).IsOK());
    ASSERT_EQ(0, dumper.GetPKeyCount());
    ASSERT_TRUE(dumper.Dump(0, ChunkItemOffset::Of(0, 3)).IsOK());
    ASSERT_EQ(1, dumper.GetPKeyCount());
    ASSERT_TRUE(dumper.Dump(0, ChunkItemOffset::Of(0, 1024)).IsOK());
    ASSERT_TRUE(dumper.Dump(1, ChunkItemOffset::Of(8192, 6)).IsOK());
    ASSERT_EQ(2, dumper.GetPKeyCount());
    ASSERT_TRUE(dumper.Close().IsOK());

    using PKeyTable = PrefixKeyTableBase<OnDiskPKeyOffset>;
    std::shared_ptr<PKeyTable> pkeyTable;
    pkeyTable.reset((PKeyTable*)PrefixKeyTableCreator<OnDiskPKeyOffset>::Create(_dumpDirectory, indexPreference,
                                                                                PKeyTableOpenType::READ, 0, 0));

    {
        OnDiskPKeyOffset firstSkeyOffset;
        ASSERT_TRUE(FL_COAWAIT((PKeyTable*)pkeyTable.get())->FindForRead(0, firstSkeyOffset, nullptr));
        ASSERT_EQ(0, firstSkeyOffset.GetBlockOffset());
        ASSERT_EQ(8192, firstSkeyOffset.GetHintSize());
        ASSERT_EQ(3, firstSkeyOffset.inChunkOffset);
        ASSERT_EQ(0, firstSkeyOffset.chunkOffset);
    }
    {
        OnDiskPKeyOffset firstSkeyOffset;
        ASSERT_TRUE(FL_COAWAIT((PKeyTable*)pkeyTable.get())->FindForRead(1, firstSkeyOffset, nullptr));
        ASSERT_EQ(8192, firstSkeyOffset.GetBlockOffset());
        ASSERT_EQ(4096, firstSkeyOffset.GetHintSize());
        ASSERT_EQ(6, firstSkeyOffset.inChunkOffset);
        ASSERT_EQ(8192, firstSkeyOffset.chunkOffset);
    }
}
} // namespace indexlibv2::index