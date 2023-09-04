#include "indexlib/index/common/field_format/spatial/shape/Circle.h"

#include <vector>

#include "indexlib/index/common/field_format/spatial/shape/Rectangle.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class RectanglePointIterator
{
public:
    typedef std::pair<double, double> SPoint;
    typedef std::vector<SPoint> SPointVec;
    typedef SPointVec::const_iterator Iterator;

public:
    RectanglePointIterator(const std::shared_ptr<Rectangle>& rec, double ratio = 0.1)
    {
        double minX = rec->GetMinX();
        double minY = rec->GetMinY();
        double maxX = rec->GetMaxX();
        double maxY = rec->GetMaxY();
        double rotateParam = 0;
        if (maxX < minX) {
            rotateParam = 360;
            maxX += 360;
        }

        double lonDelta = (maxX - minX) * ratio;
        double latDelta = (maxY - minY) * ratio;
        for (double lon = minX; lon <= maxX; lon += lonDelta) {
            for (double lat = minY; lat <= maxY; lat += latDelta) {
                _points.push_back(std::make_pair(lon - rotateParam, lat));
            }
        }
    }

    Iterator Begin() const { return _points.begin(); }
    Iterator End() const { return _points.end(); }

private:
    SPointVec _points;
};

class CircleTest : public TESTBASE
{
    CircleTest() = default;
    ~CircleTest() = default;
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(CircleTest, TestGetBoundingBox)
{
    auto circle = Circle::FromString("0 0,121000");
    std::shared_ptr<Rectangle> box = circle->GetBoundingBox();
    EXPECT_EQ(-1, (int)box->GetMinX());
    EXPECT_EQ(-1, (int)box->GetMinY());
    EXPECT_EQ(1, (int)box->GetMaxX());
    EXPECT_EQ(1, (int)box->GetMaxY());
}

TEST_F(CircleTest, TestFromString)
{
    // test abnormal
    ASSERT_FALSE(Circle::FromString("1 2, 3, 4"));
    ASSERT_FALSE(Circle::FromString("1 2, 3 4"));
    ASSERT_FALSE(Circle::FromString("1 2"));
    ASSERT_FALSE(Circle::FromString("1 200,3"));
    ASSERT_FALSE(Circle::FromString("1,2"));
    ASSERT_FALSE(Circle::FromString("1 2,a"));
    ASSERT_FALSE(Circle::FromString("1 2,-1"));

    // test normal
    auto circle = Circle::FromString("1 2,3");
    ASSERT_TRUE(circle);
    ASSERT_EQ("circle(1 2,3)", circle->ToString());
    ASSERT_EQ((double)3, circle->GetRadius());
    std::shared_ptr<Point> point = circle->GetCenter();
    ASSERT_EQ((double)1, point->GetX());
    ASSERT_EQ((double)2, point->GetY());
}

TEST_F(CircleTest, TestInsideRectangle)
{
    uint32_t count = 0;
    uint32_t noInsideBoxCount = 0;
    std::vector<double> lonVec = {-180, -90, 0, 90, 180};
    std::vector<double> latVec = {-90, -45, 0, 45, 90};
    std::vector<double> radiusVec = {1, 100, 10000000};
    for (double lon : lonVec) {
        for (double lat : latVec) {
            std::shared_ptr<Point> point(new Point(lon, lat));
            // 1m -> 10w km
            for (double radius : radiusVec) {
                count++;
                Circle circle(point, radius);
                std::shared_ptr<Rectangle> insideBox = circle._insideBox;
                if (insideBox) {
                    double dist1 = circle.GetDistance(insideBox->GetMinX(), insideBox->GetMinY());
                    ASSERT_TRUE(dist1 <= radius) << circle.ToString() << "#" << radius << ":" << dist1;

                    double dist2 = circle.GetDistance(insideBox->GetMaxX(), insideBox->GetMaxY());
                    ASSERT_TRUE(dist2 <= radius) << circle.ToString() << "#" << radius << ":" << dist2;

                    double dist3 = circle.GetDistance(insideBox->GetMinX(), insideBox->GetMaxY());
                    ASSERT_TRUE(dist3 <= radius) << circle.ToString() << "#" << radius << ":" << dist1;

                    double dist4 = circle.GetDistance(insideBox->GetMaxX(), insideBox->GetMinY());
                    ASSERT_TRUE(dist4 <= radius) << circle.ToString() << "#" << radius << ":" << dist2;
                } else {
                    noInsideBoxCount++;
                }
            }
        }
    }
}

TEST_F(CircleTest, TestIsInnerCoordinate)
{
    std::vector<double> lonVec = {-180, -90, 0, 90, 180};
    std::vector<double> latVec = {-90, -45, 0, 45, 90};
    std::vector<double> radiusVec = {100, 10000, 10000000};
    uint32_t count = 0;
    for (double lon : lonVec) {
        for (double lat : latVec) {
            std::shared_ptr<Point> point(new Point(lon, lat));
            // 100m -> 10w km
            for (double radius : radiusVec) {
                count++;
                Circle circle(point, radius);
                std::shared_ptr<Rectangle> boundBox = circle.GetBoundingBox();
                RectanglePointIterator pointIter(boundBox, 0.2);
                RectanglePointIterator::Iterator begin = pointIter.Begin();
                RectanglePointIterator::Iterator end = pointIter.End();
                for (RectanglePointIterator::Iterator iter = begin; iter != end; iter++) {
                    double lon = iter->first;
                    double lat = iter->second;

                    circle._optInnerCheck = true;
                    bool optExist = circle.IsInnerCoordinate(lon, lat);

                    circle._optInnerCheck = false;
                    bool noOptExist = circle.IsInnerCoordinate(lon, lat);
                    ASSERT_EQ(optExist, noOptExist) << circle.ToString() << "#" << lon << "," << lat;
                }
            }
        }
    }
}

} // namespace indexlib::index
