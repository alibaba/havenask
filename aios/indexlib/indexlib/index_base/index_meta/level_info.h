#ifndef __INDEXLIB_LEVEL_INFO_H
#define __INDEXLIB_LEVEL_INFO_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/configurator_define.h"
#include <autil/legacy/jsonizable.h>

IE_NAMESPACE_BEGIN(index_base);

class LevelMeta : public autil::legacy::Jsonizable
{
public:
    LevelMeta();
    ~LevelMeta();

    
    void Jsonize(JsonWrapper& json) override;

    static std::string TopologyToStr(LevelTopology topology);
    static LevelTopology StrToTopology(const std::string& str);
    bool RemoveSegment(segmentid_t segmentId);
    void Clear();

    bool operator == (const LevelMeta& other) const;

public:
    uint32_t levelIdx;
    uint32_t cursor;
    LevelTopology topology;
    std::vector<segmentid_t> segments;
};

class LevelInfo : public autil::legacy::Jsonizable
{
public:
    LevelInfo();
    ~LevelInfo();
public:
    
    void Jsonize(JsonWrapper& json) override;

    void Init(LevelTopology topo, uint32_t levelNum, size_t columnCount);

    void AddSegment(segmentid_t segmentId);
    void AddLevel(LevelTopology levelTopo);

    void RemoveSegment(segmentid_t segmentId);
    bool FindPosition(segmentid_t segmentId, uint32_t& levelIdx, uint32_t& inLevelIdx) const;
    void Clear();

    size_t GetLevelCount() const { return levelMetas.size(); }
    LevelTopology GetTopology() const;
    size_t GetColumnCount() const;

    bool operator == (const LevelInfo& other) const
    { return levelMetas == other.levelMetas; }

    // return segmentids ordered from new to old
    std::vector<segmentid_t> GetSegmentIds(size_t columnId) const;

public:
    std::vector<LevelMeta> levelMetas;
};

DEFINE_SHARED_PTR(LevelInfo);
////////////////////////////////////////////////////////////
inline std::string LevelMeta::TopologyToStr(LevelTopology topology)
{
    return config::TopologyToStr(topology);
}

inline LevelTopology LevelMeta::StrToTopology(const std::string& str)
{
    return config::StrToTopology(str);
}

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_LEVEL_INFO_H
