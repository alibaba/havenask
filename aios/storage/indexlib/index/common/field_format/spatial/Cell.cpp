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
#include "indexlib/index/common/field_format/spatial/Cell.h"

namespace indexlib::index {

void Cell::GetSubCells(std::vector<Cell>& subCells) const
{
    subCells.clear();
    std::vector<uint64_t> subCellIds;
    GEOHASH_area area = {{90, -90}, {180, -180}};
    if (_cellId != GeoHashUtil::ZERO_LEVEL_HASH_ID) {
        Rectangle* rect = GetCellRectangle();
        area.longitude.min = rect->GetMinX();
        area.longitude.max = rect->GetMaxX();
        area.latitude.min = rect->GetMinY();
        area.latitude.max = rect->GetMaxY();
    }

    GeoHashUtil::GetSubGeoHashIds(_cellId, subCellIds);
    subCells.reserve(subCellIds.size());
    for (size_t i = 0; i < subCellIds.size(); i++) {
        double minX, minY, maxX, maxY;
        GeoHashUtil::GetGeoHashAreaFromHigherLevel(subCellIds[i], area, minX, minY, maxX, maxY);
        subCells.push_back(Cell(subCellIds[i], _disjointEdges, minX, minY, maxX, maxY));
    }
}

bool Cell::IsLeafCell() const { return GeoHashUtil::IsLeafCell(_cellId); }

void Cell::SetLeafTag() { GeoHashUtil::SetLeafTag(_cellId); }

void Cell::RemoveLeafTag() { GeoHashUtil::RemoveLeafTag(_cellId); }

int8_t Cell::GetCellLevel() const { return GeoHashUtil::GetLevelOfHashId(_cellId); }

// TODO: optimize new??
Rectangle* Cell::GetCellRectangle() const
{
    if (_rectangle.GetMinX() == INVALID_LON) {
        double minLat = 0;
        double minLon = 0;
        double maxLat = 0;
        double maxLon = 0;
        GeoHashUtil::GetGeoHashArea(_cellId, minLon, minLat, maxLon, maxLat);
        _rectangle = Rectangle(minLon, minLat, maxLon, maxLat);
    }
    return &_rectangle;
}

std::string Cell::ToString() const { return GeoHashUtil::HashToGeoStr(_cellId); }
} // namespace indexlib::index
