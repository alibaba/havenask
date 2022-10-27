#include "indexlib/common/field_format/spatial/cell.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, Cell);

const double Cell::INVALID_LON = 181.0;
const double Cell::INVALID_LAT = 91.0;
void Cell::GetSubCells(std::vector<Cell>& subCells) const
{
    subCells.clear();    
    vector<uint64_t> subCellIds;
    GEOHASH_area area = { {90, -90}, {180, -180} };
    if (mCellId != GeoHashUtil::ZERO_LEVEL_HASH_ID)
    {
        Rectangle* rect = GetCellRectangle();
        area.longitude.min = rect->GetMinX();
        area.longitude.max = rect->GetMaxX();
        area.latitude.min = rect->GetMinY();
        area.latitude.max = rect->GetMaxY();
    }

    GeoHashUtil::GetSubGeoHashIds(mCellId, subCellIds);
    for (size_t i = 0; i < subCellIds.size(); i++)
    {
        double minX,minY,maxX,maxY;
        GeoHashUtil::GetGeoHashAreaFromHigherLevel(
                subCellIds[i], area, minX, minY, maxX, maxY);
        subCells.push_back(Cell(subCellIds[i], mDisjointEdges,
                                minX, minY, maxX, maxY));
    }
}

bool Cell::IsLeafCell()
{ return GeoHashUtil::IsLeafCell(mCellId); }

void Cell::SetLeafTag() 
{
    GeoHashUtil::SetLeafTag(mCellId);
}

void Cell::RemoveLeafTag() 
{
    GeoHashUtil::RemoveLeafTag(mCellId);
}

int8_t Cell::GetCellLevel() const
{
    return GeoHashUtil::GetLevelOfHashId(mCellId);
}

//TODO: optimize new??
Rectangle* Cell::GetCellRectangle() const
{
    if (mRectangle.GetMinX() == INVALID_LON)
    {
        double minLat = 0;
        double minLon = 0;
        double maxLat = 0;
        double maxLon = 0;
        GeoHashUtil::GetGeoHashArea(mCellId, minLon, minLat, maxLon, maxLat);
        mRectangle = Rectangle(minLon, minLat, maxLon, maxLat);
    }    
    return &mRectangle;
}

string Cell::ToString() const
{
    return GeoHashUtil::HashToGeoStr(mCellId);
}

IE_NAMESPACE_END(common);

