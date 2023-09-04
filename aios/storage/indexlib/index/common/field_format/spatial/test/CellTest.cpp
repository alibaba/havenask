#include "indexlib/index/common/field_format/spatial/Cell.h"

#include "indexlib/index/common/field_format/spatial/geo_hash/GeoHashUtil.h"
#include "indexlib/index/common/field_format/spatial/geo_hash/geohash.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class CellTest : public TESTBASE
{
public:
    CellTest() = default;
    ~CellTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void CellTest::setUp() {}

void CellTest::tearDown() {}

TEST_F(CellTest, TestGetSubCells)
{
    Cell cell(GeoHashUtil::GeoStrToHash("bc"));
    cell._disjointEdges.SetDisjointEdge(1);
    std::vector<Cell> subCells;
    cell.GetSubCells(subCells);
    ASSERT_EQ((size_t)32, subCells.size());

    const char* nodes = "0123456789bcdefghjkmnpqrstuvwxyz";
    for (size_t i = 0; i < strlen(nodes); i++) {
        uint64_t cellId = subCells[i].GetCellId();
        ASSERT_TRUE(subCells[i]._disjointEdges.IsDisjointEdge(1u));
        double minLat = 0;
        double minLon = 0;
        double maxLat = 0;
        double maxLon = 0;
        GeoHashUtil::GetGeoHashArea(cellId, minLon, minLat, maxLon, maxLat);
        ASSERT_EQ(subCells[i]._rectangle.GetMinX(), minLon);
        ASSERT_EQ(subCells[i]._rectangle.GetMinY(), minLat);
        ASSERT_EQ(subCells[i]._rectangle.GetMaxX(), maxLon);
        ASSERT_EQ(subCells[i]._rectangle.GetMaxY(), maxLat);
        std::string cellStr = GeoHashUtil::HashToGeoStr(cellId);
        std::string expectCellStr = std::string("bc").append(1, nodes[i]);
        ASSERT_EQ(expectCellStr, cellStr);
    }
}

TEST_F(CellTest, TestGetCellRectangle)
{
    auto InnerTestGetCellRectangle = [](const std::string& cellStr, int8_t level) {
        Cell cell(GeoHashUtil::GeoStrToHash(cellStr));
        ASSERT_EQ(cellStr, GeoHashUtil::HashToGeoStr(cell.GetCellId()));
        Rectangle* rectangle = cell.GetCellRectangle();
        uint64_t cellId = cell.GetCellId();

        // the smallest point and side is in cell
        ASSERT_EQ(cellId, GeoHashUtil::Encode(rectangle->GetMinX(), rectangle->GetMinY(), level));
        ASSERT_EQ(cellId, GeoHashUtil::Encode(rectangle->GetMinX(), rectangle->GetMinY() + 0.0001, level));
        ASSERT_EQ(cellId, GeoHashUtil::Encode(rectangle->GetMinX() + 0.0001, rectangle->GetMinY(), level));

        // test in cell
        ASSERT_EQ(cellId, GeoHashUtil::Encode(rectangle->GetMaxX() - 0.0001, rectangle->GetMaxY() - 0.0001, level));
        if (rectangle->GetMaxX() != 90.0 && rectangle->GetMaxY() != 180.0) {
            ASSERT_FALSE(cellId ==
                         GeoHashUtil::Encode(rectangle->GetMaxX() + 0.0001, rectangle->GetMaxY() + 0.0001, level));
        }

        if (rectangle->GetMinX() != -90.0 && rectangle->GetMinY() != -180.0) {
            ASSERT_FALSE(cellId ==
                         GeoHashUtil::Encode(rectangle->GetMinX() - 0.0001, rectangle->GetMinY() - 0.0001, level));
        }

        // test max point and side not in cell
        ASSERT_FALSE(cellId == GeoHashUtil::Encode(rectangle->GetMaxX(), rectangle->GetMaxY(), level));
    };
    InnerTestGetCellRectangle("b", 1);
    InnerTestGetCellRectangle("bc", 2);
    InnerTestGetCellRectangle("0", 1);
    InnerTestGetCellRectangle("02bcuvz", 7);
}

} // namespace indexlib::index
