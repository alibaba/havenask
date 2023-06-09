/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>
#include <queue>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/common/field_format/spatial/Cell.h"
#include "indexlib/index/common/field_format/spatial/geo_hash/GeoHashUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"

namespace indexlib::index {

class ShapeCoverer : private autil::NoCopyable
{
public:
    ShapeCoverer(int8_t indexTopLevel, int8_t indexDetailLevel, bool onlyGetLeafCell);
    ~ShapeCoverer() = default;

    std::vector<uint64_t> GetPointCoveredCells(const std::shared_ptr<Point>& point, bool onlyGetDetailCell);
    // not support for point search
    std::vector<uint64_t> GetShapeCoveredCells(const std::shared_ptr<Shape>& shape, size_t maxSearchTerms,
                                               int8_t searchDetailLevel);

private:
    void SearchSubCells(const std::shared_ptr<Shape>& shape, const Cell& curCell, std::queue<Cell>& searchCells,
                        std::vector<uint64_t>& resultCells, int8_t searchDetailLevel);
    void HandleIntersectCell(Cell& curCell, int8_t searchDetailLevel, std::queue<Cell>& cellsToSearch,
                             std::vector<uint64_t>& resultCells);
    void HandleWithinCell(Cell& curCell, int8_t searchDetailLevel, std::queue<Cell>& cellsToSearch,
                          std::vector<uint64_t>& resultCells);
    void HandleContainsCell(Cell& curCell, std::vector<uint64_t>& resultCells);
    bool NeedKeepSearch(const std::queue<Cell>& searchQueue, const std::vector<uint64_t>& resultCells,
                        size_t maxSearchTerms);
    void AppendVector(const std::vector<Cell>& srcCells, std::vector<uint64_t>& destCells);
    void AddAllSubCellsInLevel(const Cell& curCell, int8_t level, std::vector<uint64_t>& cells);

private:
    int8_t _indexTopLevel;
    int8_t _indexDetailLevel;
    bool _onlyGetLeafCell;
    int64_t _cellCount;

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////
inline void ShapeCoverer::HandleWithinCell(Cell& curCell, int8_t searchDetailLevel, std::queue<Cell>& cellsToSearch,
                                           std::vector<uint64_t>& resultCells)
{
    // should not clear search queue
    if (curCell.GetCellLevel() == searchDetailLevel) {
        curCell.SetLeafTag();
        resultCells.push_back(curCell.GetCellId());
        return;
    }
    assert(curCell.GetCellLevel() < searchDetailLevel);
    if (!_onlyGetLeafCell && curCell.GetCellLevel() >= _indexTopLevel) {
        curCell.RemoveLeafTag();
        resultCells.push_back(curCell.GetCellId());
    }
    cellsToSearch.push(curCell);
}

inline void ShapeCoverer::HandleContainsCell(Cell& curCell, std::vector<uint64_t>& resultCells)
{
    if (curCell.GetCellLevel() < _indexTopLevel) {
        AddAllSubCellsInLevel(curCell, _indexTopLevel, resultCells);
    } else {
        curCell.SetLeafTag();
        resultCells.push_back(curCell.GetCellId());
    }
}

inline void ShapeCoverer::HandleIntersectCell(Cell& curCell, int8_t searchDetailLevel, std::queue<Cell>& cellsToSearch,
                                              std::vector<uint64_t>& resultCells)
{
    if (curCell.GetCellLevel() == searchDetailLevel) {
        curCell.SetLeafTag();
        resultCells.push_back(curCell.GetCellId());
        return;
    }
    if (!_onlyGetLeafCell && curCell.GetCellLevel() >= _indexTopLevel) {
        curCell.RemoveLeafTag();
        resultCells.push_back(curCell.GetCellId());
    }
    assert(curCell.GetCellLevel() < searchDetailLevel);
    cellsToSearch.push(curCell);
}

inline bool ShapeCoverer::NeedKeepSearch(const std::queue<Cell>& searchQueue, const std::vector<uint64_t>& resultCells,
                                         size_t maxSearchTerms)
{
    // search queue empty finish
    if (searchQueue.empty()) {
        return false;
    }

    // search not finish and not reach max search terms, continue
    if (resultCells.size() + searchQueue.size() < maxSearchTerms) {
        return true;
    }

    Cell topLevelCell = searchQueue.front();
    int8_t cellLevel = topLevelCell.GetCellLevel();
    // search level not reach top index level, continue
    if (cellLevel < _indexTopLevel) {
        return true;
    }

    return false;
}

inline std::vector<uint64_t> ShapeCoverer::GetShapeCoveredCells(const std::shared_ptr<Shape>& shape,
                                                                size_t maxSearchTerms, int8_t searchDetailLevel)
{
    assert(shape->GetType() != Shape::ShapeType::POINT);
    std::vector<uint64_t> coverCells;
    std::queue<Cell> searchCells; // search cells: intersect, contains shape
    searchCells.push(Cell(GeoHashUtil::ZERO_LEVEL_HASH_ID, DisjointEdges(), /*minX*/ Cell::INVALID_LON,
                          /*minY*/ Cell::INVALID_LAT,
                          /*maxX*/ Cell::INVALID_LON, /*maxY*/ Cell::INVALID_LAT));
    while (NeedKeepSearch(searchCells, coverCells, maxSearchTerms)) {
        Cell curSearchCell = searchCells.front();
        searchCells.pop();
        SearchSubCells(shape, curSearchCell, searchCells, coverCells, searchDetailLevel);
    }

    while (!searchCells.empty()) {
        Cell cell = searchCells.front();
        cell.SetLeafTag();
        coverCells.push_back(cell.GetCellId());
        searchCells.pop();
    }
    return coverCells;
}

inline void ShapeCoverer::SearchSubCells(const std::shared_ptr<Shape>& shape, const Cell& curCell,
                                         std::queue<Cell>& searchCells, std::vector<uint64_t>& resultCells,
                                         int8_t searchDetailLevel)
{
    std::vector<Cell> subCells;
    curCell.GetSubCells(subCells);
    for (size_t i = 0; i < subCells.size(); i++) {
        _cellCount++;
        Cell& curSubCell = subCells[i];
        const Rectangle* cellRect = curSubCell.GetCellRectangle();

        Shape::Relation relation = shape->GetRelation(cellRect, curSubCell.GetDisjointEdges());

        if (relation == Shape::Relation::WITHINS) {
            HandleWithinCell(curSubCell, searchDetailLevel, searchCells, resultCells);
            // should not break, if point on side of cell, will fail
        } else if (relation == Shape::Relation::CONTAINS) {
            HandleContainsCell(curSubCell, resultCells);
        } else if (relation == Shape::Relation::INTERSECTS) {
            HandleIntersectCell(curSubCell, searchDetailLevel, searchCells, resultCells);
        } else {
            assert(relation == Shape::Relation::DISJOINTS);
            // disjoints do nothing
        }
    }
}

inline void ShapeCoverer::AppendVector(const std::vector<Cell>& srcCells, std::vector<uint64_t>& destCells)
{
    for (size_t i = 0; i < srcCells.size(); i++) {
        destCells.push_back(srcCells[i].GetCellId());
    }
}

inline void ShapeCoverer::AddAllSubCellsInLevel(const Cell& curCell, int8_t level, std::vector<uint64_t>& cells)
{
    int8_t curLevel = curCell.GetCellLevel();
    if (curCell.GetCellLevel() >= level) {
        return;
    }
    std::vector<Cell> subCells;
    curCell.GetSubCells(subCells);
    if (curLevel + 1 == level) {
        for (size_t i = 0; i < subCells.size(); i++) {
            subCells[i].SetLeafTag();
        }
        AppendVector(subCells, cells);
        return;
    }
    for (size_t i = 0; i < subCells.size(); i++) {
        // todo optimize not recursive
        AddAllSubCellsInLevel(subCells[i], level, cells);
    }
}

inline std::vector<uint64_t> ShapeCoverer::GetPointCoveredCells(const std::shared_ptr<Point>& point,
                                                                bool onlyGetDetailCell)
{
    // attention not set leaf cell
    std::vector<uint64_t> coverCells;
    if (onlyGetDetailCell) {
        uint64_t key = GeoHashUtil::Encode(point->GetX(), point->GetY(), _indexDetailLevel);
        coverCells.push_back(key);
    } else {
        std::vector<uint64_t> keys =
            GeoHashUtil::Encode(point->GetX(), point->GetY(), _indexTopLevel, _indexDetailLevel);
        for (auto key : keys) {
            coverCells.push_back(key);
        }
    }
    return coverCells;
}
} // namespace indexlib::index
