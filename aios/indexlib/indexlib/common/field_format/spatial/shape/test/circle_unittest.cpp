#include "indexlib/common/field_format/spatial/shape/test/circle_unittest.h"
#include "indexlib/common/field_format/spatial/shape/rectangle.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(shape, CircleTest);

class RectanglePointIterator
{
public:
    typedef std::pair<double, double> SPoint;
    typedef std::vector<SPoint> SPointVec;

    typedef SPointVec::const_iterator Iterator;

public:
    RectanglePointIterator(const RectanglePtr& rec, double ratio = 0.1)
    {
        double minX = rec->GetMinX();
        double minY = rec->GetMinY();
        double maxX = rec->GetMaxX();
        double maxY = rec->GetMaxY();
        double rotateParam = 0;
        if (maxX < minX)
        {
            rotateParam = 360;
            maxX += 360;
        }

        double lonDelta = (maxX - minX) * ratio;
        double latDelta = (maxY - minY) * ratio;
        for (double lon = minX; lon <= maxX; lon += lonDelta)
        {
            for (double lat = minY; lat <= maxY; lat += latDelta)
            {
                mPoints.push_back(make_pair(lon - rotateParam, lat));
            }
        }
    }

    Iterator Begin() const { return mPoints.begin(); }
    Iterator End() const { return mPoints.end(); }
    
private:
    SPointVec mPoints;
};

CircleTest::CircleTest()
{
}

CircleTest::~CircleTest()
{
}

void CircleTest::CaseSetUp()
{
}

void CircleTest::CaseTearDown()
{
}

void CircleTest::TestGetBoundingBox()
{
    CirclePtr circle = Circle::FromString("0 0,121000");
    RectanglePtr box = circle->GetBoundingBox();
    EXPECT_EQ(-1, (int)box->GetMinX());
    EXPECT_EQ(-1, (int)box->GetMinY());
    EXPECT_EQ(1, (int)box->GetMaxX());
    EXPECT_EQ(1, (int)box->GetMaxY());
}

void CircleTest::TestFromString()
{
    //test abnormal
    ASSERT_FALSE(Circle::FromString("1 2, 3, 4"));
    ASSERT_FALSE(Circle::FromString("1 2, 3 4"));
    ASSERT_FALSE(Circle::FromString("1 2"));
    ASSERT_FALSE(Circle::FromString("1 200,3"));
    ASSERT_FALSE(Circle::FromString("1,2"));
    ASSERT_FALSE(Circle::FromString("1 2,a"));
    ASSERT_FALSE(Circle::FromString("1 2,-1"));

    //test normal
    CirclePtr circle = Circle::FromString("1 2,3");
    ASSERT_TRUE(circle);
    ASSERT_EQ("circle(1 2,3)", circle->ToString());
    ASSERT_EQ((double)3, circle->GetRadius());
    PointPtr point = circle->GetCenter();
    ASSERT_EQ((double)1, point->GetX());
    ASSERT_EQ((double)2, point->GetY());
}

void CircleTest::TestInsideRectangle()
{
    uint32_t count = 0;
    uint32_t noInsideBoxCount = 0;
    for (double lon = -180; lon <= 180; lon += 0.2)
    {
        for (double lat = -90; lat <= 90; lat += 0.2)
        {
            PointPtr point(new Point(lon, lat));
            // 1m -> 10w km
            for (double radius = 1; radius <= 10000000; radius *= 10)
            {
                count++;
                Circle circle(point, radius);
                RectanglePtr insideBox = circle.mInsideBox;
                if (insideBox)
                {
                    double dist1 = circle.GetDistance(insideBox->GetMinX(),
                            insideBox->GetMinY());
                    ASSERT_TRUE(dist1 <= radius) << circle.ToString() << "#" << radius << ":" << dist1;

                    double dist2 = circle.GetDistance(insideBox->GetMaxX(),
                            insideBox->GetMaxY());
                    ASSERT_TRUE(dist2 <= radius) << circle.ToString() << "#" << radius << ":" << dist2;

                    double dist3 = circle.GetDistance(insideBox->GetMinX(),
                            insideBox->GetMaxY());
                    ASSERT_TRUE(dist3 <= radius) << circle.ToString() << "#" << radius << ":" << dist1;

                    double dist4 = circle.GetDistance(insideBox->GetMaxX(),
                            insideBox->GetMinY());
                    ASSERT_TRUE(dist4 <= radius) << circle.ToString() << "#" << radius << ":" << dist2;
                }
                else
                {
                    noInsideBoxCount++;
                }
                // if (count % 10000 == 0)
                // {
                //     cout << "#" << count << endl;
                // }
            }
        }
    }
    cout << "total:" << count << "," << "no inside box count:" << noInsideBoxCount << endl;
}

void CircleTest::TestIsInnerCoordinate()
{
    uint32_t count = 0;
    for (double lon = -180; lon <= 180; lon += 0.2)
    {
        for (double lat = -90; lat <= 90; lat += 0.2)
        {
            PointPtr point(new Point(lon, lat));
            // 100m -> 10w km
            for (double radius = 100; radius <= 10000000; radius *= 10)
            {
                count++;
                Circle circle(point, radius);
                RectanglePtr boundBox = circle.GetBoundingBox();
                RectanglePointIterator pointIter(boundBox, 0.2);
                RectanglePointIterator::Iterator begin = pointIter.Begin();
                RectanglePointIterator::Iterator end = pointIter.End();
                for (RectanglePointIterator::Iterator iter = begin; iter != end; iter++)
                {
                    double lon = iter->first;
                    double lat = iter->second;

                    circle.mOptInnerCheck = true;
                    bool optExist = circle.IsInnerCoordinate(lon, lat);

                    circle.mOptInnerCheck = false;
                    bool noOptExist = circle.IsInnerCoordinate(lon, lat);
                    ASSERT_EQ(optExist, noOptExist) << circle.ToString() << "#" << lon << "," << lat;
                }
                // if (count % 10000 == 0)
                // {
                //     cout << "#" << count << endl;
                // }
            }
        }
    }
}



IE_NAMESPACE_END(common);

