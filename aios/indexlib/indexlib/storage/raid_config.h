#ifndef __INDEXLIB_RAID_CONFIG_H
#define __INDEXLIB_RAID_CONFIG_H

#include <memory>
#include <string>
#include <autil/legacy/jsonizable.h>
#include <fslib/fs/FileSystem.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"


IE_NAMESPACE_BEGIN(storage);

struct RaidConfig : public autil::legacy::Jsonizable
{
    RaidConfig() = default;
    bool useRaid = false;
    size_t bufferSizeThreshold = 10 * 1024 * 1024; // 10 M
    int16_t dataBlockNum = -1;
    int16_t parityBlockNum = -1;
    int16_t packetSizeBits = -1;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        json.Jsonize("use_raid", useRaid, useRaid);
        json.Jsonize("buffer_size_threshold", bufferSizeThreshold, bufferSizeThreshold);
        json.Jsonize("data_block_num", dataBlockNum, dataBlockNum);
        json.Jsonize("parity_block_num", parityBlockNum, parityBlockNum);
        json.Jsonize("packet_size_bits", packetSizeBits, packetSizeBits);
    }

    bool operator==(const RaidConfig& other) const {
        return useRaid == other.useRaid && bufferSizeThreshold == other.bufferSizeThreshold
            && dataBlockNum == other.dataBlockNum && parityBlockNum == other.parityBlockNum
            && packetSizeBits == other.packetSizeBits;
    }

    std::string FsConfig(fslib::FsType fsType) const;
    std::string MakeRaidPath(const std::string& path) const;
};

using RaidConfigPtr = std::shared_ptr<RaidConfig>;


IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_RAID_CONFIG_H
