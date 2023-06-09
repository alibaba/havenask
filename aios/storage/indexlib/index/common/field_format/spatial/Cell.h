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

#include "indexlib/index/common/field_format/spatial/geo_hash/GeoHashUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/Rectangle.h"

namespace indexlib::index {

class Cell
{
public:
    Cell(uint64_t cellId)
        : _cellId(cellId)
        , _rectangle(/*minX*/ Cell::INVALID_LON,
                     /*minY*/ Cell::INVALID_LAT,
                     /*maxX*/ Cell::INVALID_LON, /*maxY*/ Cell::INVALID_LAT)
        , _disjointEdges(DisjointEdges())
    {
    }

    Cell(uint64_t cellId, const DisjointEdges& edges, double minX, double minY, double maxX, double maxY)
        : _cellId(cellId)
        , _rectangle(minX, minY, maxX, maxY)
        , _disjointEdges(edges)
    {
    }

    ~Cell() = default;

public:
    uint64_t GetCellId() const { return _cellId; }
    int8_t GetCellLevel() const;
    void GetSubCells(std::vector<Cell>& subCells) const;
    Rectangle* GetCellRectangle() const;
    std::string ToString() const;
    static void SetLeafTag(uint64_t& cellId) { GeoHashUtil::SetLeafTag(cellId); }
    static void RemoveLeafTag(uint64_t& cellId) { GeoHashUtil::RemoveLeafTag(cellId); }
    static bool IsLeafCell(uint64_t cellId) { return GeoHashUtil::IsLeafCell(cellId); }
    void SetLeafTag();
    void RemoveLeafTag();
    bool IsLeafCell() const;
    DisjointEdges& GetDisjointEdges() { return _disjointEdges; }
    void ClearCachedDisjointEdges() { _disjointEdges.Clear(); }

public:
    static constexpr double INVALID_LON = 181.0;
    static constexpr double INVALID_LAT = 91.0;

private:
    uint64_t _cellId;
    mutable Rectangle _rectangle;
    // for speed up
    DisjointEdges _disjointEdges;
};

} // namespace indexlib::index
