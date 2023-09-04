#include "indexlib/framework/Fence.h"

#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/IpConvertor.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace framework {

class FenceTest : public TESTBASE
{
    uint64_t RangeToNum(uint32_t from, uint32_t to) { return (((uint64_t)from) << 32) | to; }
};

TEST_F(FenceTest, TestGetLatestLeaseTs)
{
    std::string globalRoot = GET_TEMP_DATA_PATH();
    std::string fenceName = "__FENCE__1";
    Fence fence(globalRoot, fenceName, /*fileSystem=*/nullptr);

    ASSERT_TRUE(fence.RenewFenceLease(/*createIfNotExist=*/true).IsOK());
    std::string leaseFilePath = PathUtil::JoinPath(globalRoot, fenceName, FENCE_LEASE_FILE_NAME);
    auto [ec, isExist] = indexlib::file_system::FslibWrapper::IsExist(leaseFilePath);
    ASSERT_EQ(ec, indexlib::file_system::FSEC_OK);
    ASSERT_EQ(isExist, true);
    int64_t ts;
    ASSERT_TRUE(Fence::GetLatestLeaseTs(globalRoot, fenceName, &ts).IsOK());
    std::string fenceDirLeasePath = PathUtil::JoinPath(globalRoot, fenceName, std::string(FENCE_LEASE_FILE_NAME));
    auto status = indexlib::file_system::FslibWrapper::AtomicStore(fenceDirLeasePath, std::string("0\n"),
                                                                   /*removeIfExist=*/true)
                      .Status();
    ASSERT_TRUE(status.IsOK());
    ASSERT_FALSE(Fence::GetLatestLeaseTs(globalRoot, fenceName, &ts).IsOK());
    status = indexlib::file_system::FslibWrapper::AtomicStore(fenceDirLeasePath, std::string(""),
                                                              /*removeIfExist=*/true)
                 .Status();
    ASSERT_TRUE(status.IsOK());
    ASSERT_FALSE(Fence::GetLatestLeaseTs(globalRoot, fenceName, &ts).IsOK());
    status = indexlib::file_system::FslibWrapper::AtomicStore(fenceDirLeasePath, std::string("abcdefghij"),
                                                              /*removeIfExist=*/true)
                 .Status();
    ASSERT_TRUE(status.IsOK());
    ASSERT_FALSE(Fence::GetLatestLeaseTs(globalRoot, fenceName, &ts).IsOK());
}

TEST_F(FenceTest, TestRenewLease)
{
    std::string globalRoot = GET_TEMP_DATA_PATH();
    std::string fenceName = "__FENCE__1";
    Fence fence(globalRoot, fenceName, /*fileSystem=*/nullptr);

    ASSERT_TRUE(fence.RenewFenceLease(/*createIfNotExist=*/true).IsOK());
    std::string leaseFilePath = PathUtil::JoinPath(globalRoot, fenceName, FENCE_LEASE_FILE_NAME);
    auto [ec, isExist] = indexlib::file_system::FslibWrapper::IsExist(leaseFilePath);

    ASSERT_EQ(ec, indexlib::file_system::FSEC_OK);
    ASSERT_EQ(isExist, true);

    std::string content;
    ASSERT_TRUE(indexlib::file_system::FslibWrapper::Load(leaseFilePath, content).OK());
    std::vector<std::string> tsVec;
    autil::StringUtil::split(tsVec, content, '\n');
    ASSERT_EQ(tsVec.size(), 1u);
    tsVec.clear();
    ASSERT_TRUE(fence.RenewFenceLease(/*createIfNotExist=*/true).IsOK());
    ASSERT_TRUE(indexlib::file_system::FslibWrapper::Load(leaseFilePath, content).OK());
    autil::StringUtil::split(tsVec, content, '\n');
    ASSERT_EQ(tsVec.size(), 1u);
    tsVec.clear();
    ASSERT_TRUE(fence.RenewFenceLease(/*createIfNotExist=*/true).IsOK());
    ASSERT_TRUE(indexlib::file_system::FslibWrapper::Load(leaseFilePath, content).OK());
    autil::StringUtil::split(tsVec, content, '\n');
    ASSERT_EQ(tsVec.size(), 1u);
    tsVec.clear();
}

TEST_F(FenceTest, TestRenewLease2)
{
    std::string globalRoot = GET_TEMP_DATA_PATH();
    std::string fenceName = "__FENCE__1";
    Fence fence(globalRoot, fenceName, /*fileSystem=*/nullptr);
    ASSERT_TRUE(fence.RenewFenceLease(/*createIfNotExist=*/false).IsOK());
    std::string leaseFilePath = PathUtil::JoinPath(globalRoot, fenceName, FENCE_LEASE_FILE_NAME);
    auto [ec, isExist] = indexlib::file_system::FslibWrapper::IsExist(leaseFilePath);
    ASSERT_EQ(ec, indexlib::file_system::FSEC_OK);
    ASSERT_EQ(isExist, false);
}

TEST_F(FenceTest, TestGenerateFenceName)
{
    indexlib::framework::TabletId tid;
    tid.SetRange(65534, 65535);
    tid.SetIp("255.255.255.255");
    tid.SetPort(65535);
    ASSERT_EQ("__FENCE__kYxSGTmgKGrFZLTZB", Fence::GenerateNewFenceName(/*isPublicFence=*/true, /*ts=*/1, tid));
}

TEST_F(FenceTest, TestDecodePublicFence)
{
    {
        indexlib::framework::TabletId tid;
        tid.SetRange(65534, 65535);
        tid.SetIp("255.255.255.255");
        tid.SetPort(65535);
        std::string fenceName = Fence::GenerateNewFenceName(/*isPublicFence=*/true, tid);
        auto [res, fenceMeta] = Fence::DecodePublicFence(fenceName);
        ASSERT_TRUE(res);
        ASSERT_EQ("255.255.255.255:65535", fenceMeta.address);
        ASSERT_EQ(65534, fenceMeta.range.first);
        ASSERT_EQ(65535, fenceMeta.range.second);
    }
    {
        indexlib::framework::TabletId tid;
        tid.SetRange(65534, 65535);
        std::string fenceName = Fence::GenerateNewFenceName(/*isPublicFence=*/true, tid);
        auto [res, fenceMeta] = Fence::DecodePublicFence(fenceName);
        ASSERT_TRUE(res);
        ASSERT_EQ("0.0.0.0:0", fenceMeta.address);
        ASSERT_EQ(65534, fenceMeta.range.first);
        ASSERT_EQ(65535, fenceMeta.range.second);
    }
    {
        indexlib::framework::TabletId tid;
        std::string fenceName = Fence::GenerateNewFenceName(/*isPublicFence=*/true, tid);
        auto [res, fenceMeta] = Fence::DecodePublicFence(fenceName);
        ASSERT_TRUE(res);
        ASSERT_EQ("0.0.0.0:0", fenceMeta.address);
        ASSERT_EQ(0, fenceMeta.range.first);
        ASSERT_EQ(65535, fenceMeta.range.second);
    }
    {
        indexlib::framework::TabletId tid;
        tid.SetIp("255.255.255.255");
        tid.SetPort(1688);
        std::string fenceName = Fence::GenerateNewFenceName(/*isPublicFence=*/true, tid);
        auto [res, fenceMeta] = Fence::DecodePublicFence(fenceName);
        ASSERT_TRUE(res);
        ASSERT_EQ("255.255.255.255:1688", fenceMeta.address);
        ASSERT_EQ(0, fenceMeta.range.first);
        ASSERT_EQ(65535, fenceMeta.range.second);
    }
    {
        // compatible with legacy
        uint64_t ipNum = 0;
        ASSERT_TRUE(util::IpConvertor::IpPortToNum("127.0.0.1:1688", ipNum));
        std::string suffix;
        ASSERT_TRUE(
            util::Base62::EncodeInteger(autil::StringUtil::toString(autil::TimeUtility::currentTimeInMicroSeconds()) +
                                            autil::StringUtil::toString(ipNum),
                                        suffix)
                .IsOK());
        std::string fenceName = std::string(FENCE_DIR_NAME_PREFIX) + suffix;
        auto [res, fenceMeta] = Fence::DecodePublicFence(fenceName);
        ASSERT_TRUE(res);
        ASSERT_EQ("127.0.0.1:1688", fenceMeta.address);
        ASSERT_EQ(0, fenceMeta.range.first);
        ASSERT_EQ(65535, fenceMeta.range.second);
    }
    {
        // compatible with legacy
        uint64_t ipNum = 0;
        std::string suffix;
        ASSERT_TRUE(
            util::Base62::EncodeInteger(autil::StringUtil::toString(autil::TimeUtility::currentTimeInMicroSeconds()) +
                                            autil::StringUtil::toString(ipNum),
                                        suffix)
                .IsOK());
        std::string fenceName = std::string(FENCE_DIR_NAME_PREFIX) + suffix;
        auto [res, fenceMeta] = Fence::DecodePublicFence(fenceName);
        ASSERT_TRUE(res);
        ASSERT_EQ("0.0.0.0:0", fenceMeta.address);
        ASSERT_EQ(0, fenceMeta.range.first);
        ASSERT_EQ(65535, fenceMeta.range.second);
    }
}
TEST_F(FenceTest, TestFindLastLine)
{
    {
        std::string str;
        Fence::FindLastLine(std::string("123456"), str);
        ASSERT_EQ(str, "123456");
    }
    {
        std::string str;
        Fence::FindLastLine(std::string("1234\n\n"), str);
        ASSERT_EQ(str, "1234");
    }

    {
        std::string str;
        Fence::FindLastLine(std::string("1234\n"), str);
        ASSERT_EQ(str, "1234");
    }
    {
        std::string str;
        Fence::FindLastLine(std::string("\n1234\n"), str);
        ASSERT_EQ(str, "1234");
    }
    {
        std::string str;
        Fence::FindLastLine(std::string("56\n1234\n"), str);
        ASSERT_EQ(str, "1234");
    }
    {
        std::string str;
        Fence::FindLastLine(std::string("56\n1234\n1\n"), str);
        ASSERT_EQ(str, "1");
    }
    {
        std::string str;
        Fence::FindLastLine(std::string("\n\n\n\n"), str);
        ASSERT_EQ(str, "");
    }
    {
        std::string str;
        Fence::FindLastLine(std::string("\n"), str);
        ASSERT_EQ(str, "");
    }
    {
        std::string str;
        Fence::FindLastLine(std::string(""), str);
        ASSERT_EQ(str, "");
    }
}

TEST_F(FenceTest, TestDisablePanguECFile)
{
    {
        ASSERT_EQ("dfs://xxx?ec=false&rep=3/test/FENCE_LEASE", Fence::DisablePanguECFile("dfs://xxx/test/FENCE_LEASE"));
    }
    {
        // param overwrite
        ASSERT_EQ("dfs://xxx?kkk=123&ec=false&rep=3/test/FENCE_LEASE",
                  Fence::DisablePanguECFile("dfs://xxx?ec=1_2_3&rep=6&kkk=123/test/FENCE_LEASE"));
    }
    {
        ASSERT_EQ("pangu://xxx?ec=false&rep=3/test/FENCE_LEASE",
                  Fence::DisablePanguECFile("pangu://xxx/test/FENCE_LEASE"));
    }
    {
        ASSERT_EQ("/test/FENCE_LEASE", Fence::DisablePanguECFile("/test/FENCE_LEASE"));
    }
    {
        ASSERT_EQ("LOCAL:///test/FENCE_LEASE", Fence::DisablePanguECFile("LOCAL:///test/FENCE_LEASE"));
    }
}

}} // namespace indexlibv2::framework
