#include "indexlib/index/ann/aitheta2/util/CustomizedCkptManager.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/util/testutil/unittest.h"

using namespace indexlib::file_system;
using namespace std;
using namespace aitheta2;

namespace indexlibv2::index::ann {

class CustomizedCkptManagerTest : public TESTBASE
{
public:
    CustomizedCkptManagerTest() = default;
    ~CustomizedCkptManagerTest() = default;

public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        _fs = indexlib::file_system::FileSystemCreator::Create("CustomizedCkptManagerTest", testRoot).GetOrThrow();
        _dir = indexlib::file_system::Directory::Get(_fs);
    }

private:
    std::shared_ptr<IFileSystem> _fs;
    std::shared_ptr<Directory> _dir;
    AUTIL_LOG_DECLARE();
};

TEST_F(CustomizedCkptManagerTest, TestGeneral)
{
    CustomizedCkptManager manager(_dir);
    IndexParams params;
    ASSERT_EQ(0, manager.init(params, "ckpt_directory"));

    auto dumper0 = manager.create_ckpt_dumper("ckpt_a");
    ASSERT_NE(nullptr, dumper0);

    std::string helloWorld = "helloWorld";
    ASSERT_EQ(helloWorld.size(), dumper0->write(helloWorld.data(), helloWorld.size()));
    ASSERT_EQ(0, dumper0->append(std::to_string(0), helloWorld.size(), 0, 0));
    ASSERT_EQ(0, dumper0->close());
    dumper0.reset();
    {
        auto dumper00 = manager.create_ckpt_dumper("ckpt_a");
        ASSERT_EQ(nullptr, dumper00);
    }
    ASSERT_EQ(0, manager.dump_ckpt_finish("ckpt_a"));

    auto dumper1 = manager.create_ckpt_dumper("ckpt_b");
    ASSERT_NE(nullptr, dumper1);
    ASSERT_EQ(0, dumper1->close());
    dumper1.reset();

    auto ids = manager.get_ckpt_ids();
    ASSERT_EQ(1, ids.size());
    ASSERT_EQ("ckpt_a", ids[0]);

    auto container0 = manager.get_ckpt_container("ckpt_a");
    ASSERT_NE(nullptr, container0);
    container0.reset();
    auto container1 = manager.get_ckpt_container("ckpt_b");
    ASSERT_EQ(nullptr, container1);

    ASSERT_EQ(0, manager.dump_ckpt_finish("ckpt_b"));
    ids = manager.get_ckpt_ids();
    ASSERT_EQ(2, ids.size());
    ASSERT_EQ("ckpt_a", ids[0]);
    ASSERT_EQ("ckpt_b", ids[1]);
    auto ckptDirectory = _dir->MakeDirectory("ckpt_directory");
    ASSERT_NE(nullptr, ckptDirectory);
    {
        std::vector<std::string> fileNames;
        ckptDirectory->ListDir("", fileNames);
        ASSERT_EQ(4, fileNames.size());
    }
    ASSERT_EQ(0, manager.cleanup());
    {
        std::vector<std::string> fileNames;
        _dir->ListDir("", fileNames);
        ASSERT_EQ(0, fileNames.size());
    }
}

AUTIL_LOG_SETUP(indexlib.index, CustomizedCkptManagerTest);
} // namespace indexlibv2::index::ann
