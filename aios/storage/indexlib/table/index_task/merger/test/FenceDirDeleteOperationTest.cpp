#include "indexlib/table/index_task/merger/FenceDirDeleteOperation.h"

#include "gmock/gmock.h"

#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "unittest/unittest.h"

using std::string;

namespace indexlibv2 { namespace table {

class FenceDirDeleteOperationTest : public TESTBASE
{
public:
    FenceDirDeleteOperationTest() {}
    ~FenceDirDeleteOperationTest() {}

public:
    void setUp() override {}
    void tearDown() override {}
};

class FakeIndexTaskContext : public framework::IndexTaskContext
{
public:
    Status GetAllFenceRootInTask(std::vector<std::string>* fenceResults) const override
    {
        *fenceResults = _fenceDirs;
        return _status;
    }
    void AddFenceDir(std::string fenceDir) { _fenceDirs.push_back(fenceDir); }
    void SetGetFenceDirStatus(Status status) { _status = status; }

private:
    Status _status;
    std::vector<std::string> _fenceDirs;
};

TEST_F(FenceDirDeleteOperationTest, TestGetFenceDirFailed)
{
    FakeIndexTaskContext context;
    context.SetGetFenceDirStatus(Status::IOError());
    framework::IndexOperationDescription opDesc;
    FenceDirDeleteOperation op(opDesc);
    auto status = op.Execute(context);
    ASSERT_TRUE(status.IsIOError());
}

TEST_F(FenceDirDeleteOperationTest, TestPartFenceDirDeleted)
{
    // fence dir1 is missing
    FakeIndexTaskContext context;
    string fenceDir1 = GET_TEMP_DATA_PATH() + "/fence_1";
    context.AddFenceDir(fenceDir1);
    string fenceDir2 = GET_TEMP_DATA_PATH() + "/fence_2";
    auto status = indexlib::file_system::toStatus(indexlib::file_system::FslibWrapper::MkDir(fenceDir2).Code());
    ASSERT_TRUE(status.IsOK());
    context.AddFenceDir(fenceDir2);
    context.SetGetFenceDirStatus(Status::OK());
    framework::IndexOperationDescription opDesc;
    FenceDirDeleteOperation op(opDesc);
    status = op.Execute(context);
    ASSERT_TRUE(status.IsOK());
    bool ret = false;
    ASSERT_EQ(indexlib::file_system::FSEC_OK, indexlib::file_system::FslibWrapper::IsExist(fenceDir1, ret));
    ASSERT_FALSE(ret);
    ASSERT_EQ(indexlib::file_system::FSEC_OK, indexlib::file_system::FslibWrapper::IsExist(fenceDir2, ret));
    ASSERT_FALSE(ret);
}

TEST_F(FenceDirDeleteOperationTest, TestSimpleProcess)
{
    // test normal case
    FakeIndexTaskContext context;
    string fenceDir1 = GET_TEMP_DATA_PATH() + "/fence_1";
    auto status = indexlib::file_system::toStatus(indexlib::file_system::FslibWrapper::MkDir(fenceDir1).Code());
    ASSERT_TRUE(status.IsOK());
    context.AddFenceDir(fenceDir1);
    string fenceDir2 = GET_TEMP_DATA_PATH() + "/fence_2";
    status = indexlib::file_system::FslibWrapper::MkDir(fenceDir2).Status();
    ASSERT_TRUE(status.IsOK());
    context.AddFenceDir(fenceDir2);
    context.SetGetFenceDirStatus(Status::OK());
    framework::IndexOperationDescription opDesc;
    FenceDirDeleteOperation op(opDesc);
    status = op.Execute(context);
    ASSERT_TRUE(status.IsOK());
    bool ret = false;
    ASSERT_EQ(indexlib::file_system::FSEC_OK, indexlib::file_system::FslibWrapper::IsExist(fenceDir1, ret));
    ASSERT_FALSE(ret);
    ASSERT_EQ(indexlib::file_system::FSEC_OK, indexlib::file_system::FslibWrapper::IsExist(fenceDir2, ret));
    ASSERT_FALSE(ret);
}

}} // namespace indexlibv2::table
