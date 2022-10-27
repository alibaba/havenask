#ifndef __INDEXLIB_SHAPE_COVERER_H
#define __INDEXLIB_SHAPE_COVERER_H

#include <tr1/memory>
#include <queue>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/cell.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/geo_hash/geo_hash_util.h"

IE_NAMESPACE_BEGIN(common);

class ShapeCoverer
{
public:
    ShapeCoverer(int8_t indexTopLevel, int8_t indexDetailLevel,
                 bool onlyGetLeafCell);
    ~ShapeCoverer();

public:
    void GetPointCoveredCells(const PointPtr& point,
                              std::vector<uint64_t>& coverCell,
                              bool onlyGetDetailCell);
    //not support for point search
    void GetShapeCoveredCells(const ShapePtr& shape,
                              size_t maxSearchTerms,
                              int8_t searchDetailLevel,
                              std::vector<uint64_t>& coverCell);
private:
    void SearchSubCells(const ShapePtr& shape, 
                        const Cell& curCell,
                        std::queue<Cell>& searchCells,
                        std::vector<uint64_t>& resultCells,
                        int8_t searchDetailLevel);
    void HandleIntersectCell(Cell& curCell,
                             int8_t searchDetailLevel,
                             std::queue<Cell>& cellsToSearch, 
                             std::vector<uint64_t>& resultCells);
    void HandleWithinCell(Cell& curCell,
                          int8_t searchDetailLevel,
                          std::queue<Cell>& cellsToSearch,
                          std::vector<uint64_t>& resultCells);
    void HandleContainsCell(Cell& curCell,
                            std::vector<uint64_t>& resultCells);
    bool NeedKeepSearch(const std::queue<Cell>& searchQueue,
                        const std::vector<uint64_t>& resultCells,
                        size_t maxSearchTerms);
    void AppendVector(const std::vector<Cell>& srcCells, 
                      std::vector<uint64_t>& destCells);
    void AddAllSubCellsInLevel(const Cell& curCell, int8_t level,
                               std::vector<uint64_t>& cells);

private:
    int8_t mIndexTopLevel;
    int8_t mIndexDetailLevel;
    bool mOnlyGetLeafCell;
    
    int64_t mCellCount;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ShapeCoverer);
////////////////////////////////////////////////////////////////////
inline void ShapeCoverer::HandleWithinCell(Cell& curCell,
                                           int8_t searchDetailLevel,
                                           std::queue<Cell>& cellsToSearch,
                                           std::vector<uint64_t>& resultCells)
{
    //should not clear search queue
    if (curCell.GetCellLevel() == searchDetailLevel)
    {
        curCell.SetLeafTag();
        resultCells.push_back(curCell.GetCellId());
        return;
    }
    assert(curCell.GetCellLevel() < searchDetailLevel);
    if (!mOnlyGetLeafCell && curCell.GetCellLevel() >= mIndexTopLevel)
    {
        curCell.RemoveLeafTag();
        resultCells.push_back(curCell.GetCellId());
    }
    cellsToSearch.push(curCell);
}

inline void ShapeCoverer::HandleContainsCell(
    Cell& curCell, std::vector<uint64_t>& resultCells)
{
    if (curCell.GetCellLevel() < mIndexTopLevel)
    {
        AddAllSubCellsInLevel(curCell, mIndexTopLevel, resultCells);
    }
    else
    {
        curCell.SetLeafTag();
        resultCells.push_back(curCell.GetCellId());
    }
}

inline void ShapeCoverer::HandleIntersectCell(
    Cell& curCell, int8_t searchDetailLevel,
    std::queue<Cell>& cellsToSearch, std::vector<uint64_t>& resultCells)
{
    if (curCell.GetCellLevel() == searchDetailLevel)
    {
        curCell.SetLeafTag();
        resultCells.push_back(curCell.GetCellId());
        return;
    }
    if (!mOnlyGetLeafCell && curCell.GetCellLevel() >= mIndexTopLevel)
    {
        curCell.RemoveLeafTag();
        resultCells.push_back(curCell.GetCellId());
    }
    assert(curCell.GetCellLevel() < searchDetailLevel);
    cellsToSearch.push(curCell);
}

inline bool ShapeCoverer::NeedKeepSearch(
    const std::queue<Cell>& searchQueue,
    const std::vector<uint64_t>& resultCells,
    size_t maxSearchTerms)
{
    //search queue empty finish
    if (searchQueue.empty())
    {
        return false;
    }
    
    //search not finish and not reach max search terms, continue
    if (resultCells.size() + searchQueue.size() < maxSearchTerms)
    {
        return true;
    }

    Cell topLevelCell = searchQueue.front();
    int8_t cellLevel = topLevelCell.GetCellLevel();
    //search level not reach top index level, continue
    if (cellLevel < mIndexTopLevel)
    {
        return true;
    }

    return false;
}

inline void ShapeCoverer::GetShapeCoveredCells(
        const ShapePtr& shape, size_t maxSearchTerms,
        int8_t searchDetailLevel, std::vector<uint64_t>& coverCells)
{
    assert(shape->GetType() != Shape::POINT);
    coverCells.clear();
    std::queue<Cell> searchCells; //search cells: intersect, contains shape
    searchCells.push(Cell(GeoHashUtil::ZERO_LEVEL_HASH_ID));
    while (NeedKeepSearch(searchCells, coverCells, maxSearchTerms))
    {
        Cell curSearchCell = searchCells.front();
        searchCells.pop();
        SearchSubCells(shape, curSearchCell, searchCells, 
                       coverCells, searchDetailLevel);
    }

    while (!searchCells.empty())
    {
        Cell cell = searchCells.front();
        cell.SetLeafTag();
        coverCells.push_back(cell.GetCellId());
        searchCells.pop();
    }

}

inline void ShapeCoverer::SearchSubCells(const ShapePtr& shape, 
        const Cell& curCell, std::queue<Cell>& searchCells,
        std::vector<uint64_t>& resultCells, int8_t searchDetailLevel)
{
    std::vector<Cell> subCells;
    curCell.GetSubCells(subCells);
    for (size_t i = 0; i < subCells.size(); i++)
    {
        mCellCount++;
        Cell& curSubCell = subCells[i];
        const Rectangle* cellRect = curSubCell.GetCellRectangle();

        Shape::Relation relation =
            shape->GetRelation(cellRect, curSubCell.GetDisjointEdges());

        if (relation == Shape::WITHINS)
        {            
            HandleWithinCell(curSubCell, searchDetailLevel, searchCells, resultCells);
            //should not break, if point on side of cell, will fail
        }
        else if (relation == Shape::CONTAINS)
        {
            HandleContainsCell(curSubCell, resultCells);
        }        
        else if (relation == Shape::INTERSECTS)
        {
            HandleIntersectCell(curSubCell, searchDetailLevel, searchCells, resultCells);
        }
        else{
            assert(relation == Shape::DISJOINTS);
            //disjoints do nothing
        }
    }
}

inline void ShapeCoverer::AppendVector(const std::vector<Cell>& srcCells,
                                       std::vector<uint64_t>& destCells)
{
    for (size_t i = 0; i < srcCells.size(); i++)
    {
        destCells.push_back(srcCells[i].GetCellId());
    }
}

inline void ShapeCoverer::AddAllSubCellsInLevel(const Cell& curCell,
        int8_t level, std::vector<uint64_t>& cells)
{
    int8_t curLevel = curCell.GetCellLevel();
    if (curCell.GetCellLevel() >= level)
    {
        return;
    }
    std::vector<Cell> subCells;
    curCell.GetSubCells(subCells);
    if (curLevel + 1 == level)
    {
        for (size_t i = 0; i < subCells.size(); i++) {
            subCells[i].SetLeafTag();
        }
        AppendVector(subCells, cells);
        return;
    }
    for (size_t i = 0; i < subCells.size(); i++)
    {
        //todo optimize not recursive
        AddAllSubCellsInLevel(subCells[i], level, cells);
    }
}

inline void ShapeCoverer::GetPointCoveredCells(const PointPtr& point,
                                               std::vector<uint64_t>& coverCell,
                                               bool onlyGetDetailCell)
{
    //attention not set leaf cell
    coverCell.clear();
    if (onlyGetDetailCell)
    {
        uint64_t key = GeoHashUtil::Encode(
            point->GetX(), point->GetY(), mIndexDetailLevel);
        coverCell.push_back(key);
    }
    else
    {
        std::vector<uint64_t> keys;
        GeoHashUtil::Encode(point->GetX(), point->GetY(), keys,
                            mIndexTopLevel, mIndexDetailLevel);
        for (size_t i = 0; i < keys.size(); i++)
        {
            coverCell.push_back(keys[i]);
        }
    }
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SHAPE_COVERER_H
