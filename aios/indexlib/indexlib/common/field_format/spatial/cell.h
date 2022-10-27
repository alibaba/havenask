#ifndef __INDEXLIB_CELL_H
#define __INDEXLIB_CELL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/rectangle.h"
#include "indexlib/common/field_format/spatial/geo_hash/geo_hash_util.h"

IE_NAMESPACE_BEGIN(common);
class Cell
{
public:
    static const double INVALID_LON;
    static const double INVALID_LAT;
public:
     Cell(uint64_t cellId, const DisjointEdges& edges = DisjointEdges(),
         double minX = INVALID_LON,
         double minY = INVALID_LAT,
         double maxX = INVALID_LON,
         double maxY = INVALID_LAT)
        : mCellId(cellId)
        , mRectangle(minX, minY, maxX, maxY)
        , mDisjointEdges(edges)
    {}

    ~Cell() {}

public:
    uint64_t GetCellId() const
    { return mCellId; }
    int8_t GetCellLevel() const;
    void GetSubCells(std::vector<Cell>& subCells) const;
    Rectangle* GetCellRectangle() const;
    std::string ToString() const;
    static void SetLeafTag(uint64_t& cellId)
    { GeoHashUtil::SetLeafTag(cellId); }
    static void RemoveLeafTag(uint64_t& cellId)
    { GeoHashUtil::RemoveLeafTag(cellId); }
    static bool IsLeafCell(uint64_t cellId)
    { return GeoHashUtil::IsLeafCell(cellId); }
    void SetLeafTag();
    void RemoveLeafTag();
    bool IsLeafCell();
    DisjointEdges& GetDisjointEdges() { return mDisjointEdges; }
    void ClearCachedDisjointEdges() { mDisjointEdges.clear(); }
private:
    uint64_t mCellId;
    mutable Rectangle mRectangle;
    //for speed up 
    DisjointEdges mDisjointEdges;
    
private:
    friend class CellTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Cell);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CELL_H
