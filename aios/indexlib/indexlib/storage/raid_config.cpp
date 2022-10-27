#include "indexlib/storage/raid_config.h"
#include "indexlib/util/path_util.h"
#include <autil/StringUtil.h>
#include <fslib/fs/FileSystem.h>
#include <vector>

using namespace std;
using namespace fslib;

IE_NAMESPACE_BEGIN(storage);

namespace
{

string makeParam(const char* str, int16_t val)
{
    return std::string(str) + "=" + std::to_string(val);
}

}

string RaidConfig::FsConfig(FsType fsType) const
{
    if (!useRaid)
    {
        return "";
    }

    if (fsType == "pangu")
    {
        vector<string> params;
        params.push_back("use_raid=true");
        if (dataBlockNum != -1)
        {
            params.push_back(makeParam("raid_data_num", dataBlockNum));
        }
        if (parityBlockNum != -1)
        {
            params.push_back(makeParam("raid_parity_num", parityBlockNum));
        }
        if (packetSizeBits != -1)
        {
            params.push_back(makeParam("raid_packet_bits", packetSizeBits));
        }
        return autil::StringUtil::toString(params, "&");
    }
    return "";
}

string RaidConfig::MakeRaidPath(const std::string& path) const
{
    auto fsType = fslib::fs::FileSystem::getFsType(path);
    auto paramStr = this->FsConfig(fsType);
    if (!paramStr.empty())
    {
        return util::PathUtil::AddFsConfig(path, paramStr);
    }
    return path;
}

IE_NAMESPACE_END(storage);
