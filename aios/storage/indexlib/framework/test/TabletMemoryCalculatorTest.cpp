#include "indexlib/framework/TabletMemoryCalculator.h"

#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletReaderContainer.h"
#include "indexlib/framework/mock/MockDiskSegment.h"
#include "indexlib/framework/mock/MockMemSegment.h"
#include "indexlib/framework/mock/MockTabletWriter.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class TabletMemoryCalculatorTest : public TESTBASE
{
public:
    TabletMemoryCalculatorTest() = default;
    ~TabletMemoryCalculatorTest() = default;

    void setUp() override {}
    void tearDown() override {}
};

TEST_F(TabletMemoryCalculatorTest, testIncIndexMemsize)
{
    /**
        version.0 {inc segment: 0, 1} {rt segment: ...824}
        version.1 {inc segment: 0, 1, 2} {rt segment: ...824}
        result is {0, 1, 2} => (1 + 2 + 3 = 6)
     **/
    std::shared_ptr<TabletReaderContainer> readerContainer(new TabletReaderContainer("name"));
    std::shared_ptr<ITabletReader> reader;
    segmentid_t segmentId = Segment::MERGED_SEGMENT_ID_MASK;
    SegmentMeta segMeta1(segmentId);
    auto incDiskSegment1 = std::make_shared<MockDiskSegment>(segMeta1);

    ++segmentId;
    SegmentMeta segMeta2(segmentId);
    auto incDiskSegment2 = std::make_shared<MockDiskSegment>(segMeta2);

    ++segmentId;
    SegmentMeta segMeta3(segmentId);
    auto incDiskSegment3 = std::make_shared<MockDiskSegment>(segMeta3);

    segmentId = Segment::RT_SEGMENT_ID_MASK;
    SegmentMeta segMeta4(segmentId);
    auto rtDiskSegment = std::make_shared<MockDiskSegment>(segMeta4);

    Version version0(/*vid*/ 0, /*ts*/ 0);
    version0.AddSegment(segMeta1.segmentId);
    version0.AddSegment(segMeta2.segmentId);
    version0.AddSegment(segMeta4.segmentId);

    std::vector<std::shared_ptr<Segment>> segments {incDiskSegment1, incDiskSegment2, rtDiskSegment};
    auto tabletData1 = std::make_shared<TabletData>(/*empty name*/ "");
    ASSERT_TRUE(tabletData1->Init(version0, segments, std::make_shared<ResourceMap>()).IsOK());

    readerContainer->AddTabletReader(tabletData1, reader, /*version deploy description*/ nullptr);
    auto calculator = std::make_unique<TabletMemoryCalculator>(/*null writer*/ nullptr, readerContainer);

    EXPECT_CALL(*incDiskSegment1, EvaluateCurrentMemUsed()).WillRepeatedly(Return(1));
    EXPECT_CALL(*incDiskSegment2, EvaluateCurrentMemUsed()).WillRepeatedly(Return(2));
    EXPECT_CALL(*incDiskSegment3, EvaluateCurrentMemUsed()).WillRepeatedly(Return(3));
    EXPECT_CALL(*rtDiskSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(3));

    EXPECT_EQ(1 + 2, calculator->GetIncIndexMemsize());

    Version version1(/*vid*/ 1, /*ts*/ 0);
    version1.AddSegment(segMeta1.segmentId);
    version1.AddSegment(segMeta2.segmentId);
    version1.AddSegment(segMeta3.segmentId);
    segments.emplace_back(incDiskSegment3);
    auto tabletData2 = std::make_shared<TabletData>(/*empty name*/ "");
    ASSERT_TRUE(tabletData2->Init(version1, segments, std::make_shared<ResourceMap>()).IsOK());
    readerContainer->AddTabletReader(tabletData2, reader, /*version deploy description*/ nullptr);
    calculator.reset(new TabletMemoryCalculator(/*null writer*/ nullptr, readerContainer));

    EXPECT_CALL(*incDiskSegment1, EvaluateCurrentMemUsed()).WillRepeatedly(Return(1));
    EXPECT_CALL(*incDiskSegment2, EvaluateCurrentMemUsed()).WillRepeatedly(Return(2));
    EXPECT_CALL(*incDiskSegment3, EvaluateCurrentMemUsed()).WillRepeatedly(Return(3));
    EXPECT_CALL(*rtDiskSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(3));

    EXPECT_EQ(1 + 2 + 3, calculator->GetIncIndexMemsize());
}

TEST_F(TabletMemoryCalculatorTest, testRtDiskSegmentMemsize)
{
    /**
        version.0 {inc segment: 0, 1} {rt segment: ...824}
        version.1 {inc segment: 0, 1, 2} {rt segment: ...824, ...825}
        result is {824, 825} => (4 + 5 = 9)
     **/
    std::shared_ptr<TabletReaderContainer> readerContainer(new TabletReaderContainer("name"));
    std::shared_ptr<ITabletReader> reader;

    segmentid_t segmentId = Segment::MERGED_SEGMENT_ID_MASK;
    SegmentMeta segMeta1(segmentId);
    auto incDiskSegment1 = std::make_shared<MockDiskSegment>(segMeta1);

    ++segmentId;
    SegmentMeta segMeta2(segmentId);
    auto incDiskSegment2 = std::make_shared<MockDiskSegment>(segMeta2);

    ++segmentId;
    SegmentMeta segMeta3(segmentId);
    auto incDiskSegment3 = std::make_shared<MockDiskSegment>(segMeta3);

    segmentId = Segment::RT_SEGMENT_ID_MASK;
    SegmentMeta segMeta4(segmentId);
    auto rtDiskSegment1 = std::make_shared<MockDiskSegment>(segMeta4);

    ++segmentId;
    SegmentMeta segMeta5(segmentId);
    auto rtDiskSegment2 = std::make_shared<MockDiskSegment>(segMeta5);

    Version version0(/*vid*/ 0, /*ts*/ 0);
    version0.AddSegment(segMeta1.segmentId);
    version0.AddSegment(segMeta2.segmentId);
    version0.AddSegment(segMeta4.segmentId);

    std::vector<std::shared_ptr<Segment>> segments {incDiskSegment1, incDiskSegment2, rtDiskSegment1};
    auto tabletData1 = std::make_shared<TabletData>(/*empty name*/ "");
    ASSERT_TRUE(tabletData1->Init(version0, segments, std::make_shared<ResourceMap>()).IsOK());

    readerContainer->AddTabletReader(tabletData1, reader, /*version deploy description*/ nullptr);
    auto calculator = std::make_unique<TabletMemoryCalculator>(/*null writer*/ nullptr, readerContainer);

    EXPECT_CALL(*incDiskSegment1, EvaluateCurrentMemUsed()).WillRepeatedly(Return(1));
    EXPECT_CALL(*incDiskSegment2, EvaluateCurrentMemUsed()).WillRepeatedly(Return(2));
    EXPECT_CALL(*incDiskSegment3, EvaluateCurrentMemUsed()).WillRepeatedly(Return(3));
    EXPECT_CALL(*rtDiskSegment1, EvaluateCurrentMemUsed()).WillRepeatedly(Return(4));
    EXPECT_CALL(*rtDiskSegment2, EvaluateCurrentMemUsed()).WillRepeatedly(Return(5));
    EXPECT_EQ(4, calculator->GetRtBuiltSegmentsMemsize());

    Version version1(/*vid*/ 1, /*ts*/ 0);
    version1.AddSegment(segMeta1.segmentId);
    version1.AddSegment(segMeta2.segmentId);
    version1.AddSegment(segMeta3.segmentId);
    segments.emplace_back(incDiskSegment3);
    segments.emplace_back(rtDiskSegment2);
    auto tabletData2 = std::make_shared<TabletData>(/*empty name*/ "");
    ASSERT_TRUE(tabletData2->Init(version1, segments, std::make_shared<ResourceMap>()).IsOK());

    readerContainer->AddTabletReader(tabletData2, reader, /*version deploy description*/ nullptr);
    calculator.reset(new TabletMemoryCalculator(/*null writer*/ nullptr, readerContainer));

    EXPECT_EQ(4 + 5, calculator->GetRtBuiltSegmentsMemsize());
}

TEST_F(TabletMemoryCalculatorTest, testDumpingMemsize)
{
    /**
        state1: building1(wrtier1), dumping1, dumping2
        result1 => writer1(1), dumping1(2) + dumping2(3) = > 1 + 2 + 3

        state2: building2(writer2), dumping1, dumping2, dumping3
        result2 ==> writer2(10), dumping1(2), dumping2(3), dumping3(4) => 10 + 2 + 3 + 4

        state3: building2(writer2), dumping2, dumping3
        result3 ==> writer2(10), dumping2(3), dumping3(4) => 10 + 3 + 4
     **/
    std::shared_ptr<TabletReaderContainer> readerContainer(new TabletReaderContainer("name"));
    std::shared_ptr<ITabletReader> reader;
    Version version0;
    segmentid_t segmentId = Segment::RT_SEGMENT_ID_MASK;
    SegmentMeta segMeta1(segmentId);
    auto dumpingSegment1 = std::make_shared<MockMemSegment>(segMeta1, Segment::SegmentStatus::ST_DUMPING);
    ++segmentId;
    SegmentMeta segMeta2(segmentId);
    auto dumpingSegment2 = std::make_shared<MockMemSegment>(segMeta2, Segment::SegmentStatus::ST_DUMPING);
    ++segmentId;
    SegmentMeta segMeta3(segmentId);
    auto dumpingSegment3 = std::make_shared<MockMemSegment>(segMeta3, Segment::SegmentStatus::ST_DUMPING);

    std::vector<std::shared_ptr<Segment>> segments {dumpingSegment1, dumpingSegment2};

    auto tabletData1 = std::make_shared<TabletData>(/*empty name*/ "");
    ASSERT_TRUE(tabletData1->Init(version0, segments, std::make_shared<ResourceMap>()).IsOK());
    auto tabletWriter1 = std::make_shared<MockTabletWriter>();
    auto tabletWriter2 = std::make_shared<MockTabletWriter>();

    readerContainer->AddTabletReader(tabletData1, reader, /*version deploy description*/ nullptr);
    auto calculator = std::make_unique<TabletMemoryCalculator>(tabletWriter1, readerContainer);
    EXPECT_CALL(*tabletWriter1, GetTotalMemSize()).WillRepeatedly(Return(1));
    EXPECT_CALL(*dumpingSegment1, EvaluateCurrentMemUsed()).WillRepeatedly(Return(2));
    EXPECT_CALL(*dumpingSegment2, EvaluateCurrentMemUsed()).WillRepeatedly(Return(3));

    EXPECT_EQ(2 + 3, calculator->GetDumpingSegmentMemsize());
    EXPECT_EQ(1 + 2 + 3, calculator->GetRtIndexMemsize());

    std::vector<std::shared_ptr<Segment>> segments1 {dumpingSegment2, dumpingSegment3};
    auto tabletData2 = std::make_shared<TabletData>(/*empty name*/ "");
    ASSERT_TRUE(tabletData2->Init(version0, segments1, std::make_shared<ResourceMap>()).IsOK());
    readerContainer->AddTabletReader(tabletData2, reader, /*version deploy description*/ nullptr);
    calculator.reset(new TabletMemoryCalculator(tabletWriter2, readerContainer));
    EXPECT_CALL(*tabletWriter2, GetTotalMemSize()).WillRepeatedly(Return(10));
    EXPECT_CALL(*dumpingSegment3, EvaluateCurrentMemUsed()).WillRepeatedly(Return(4));

    EXPECT_EQ(2 + 3 + 4, calculator->GetDumpingSegmentMemsize());
    EXPECT_EQ(2 + 3 + 4 + 10, calculator->GetRtIndexMemsize());

    readerContainer.reset(new TabletReaderContainer("name"));
    readerContainer->AddTabletReader(tabletData2, reader, /*version deploy description*/ nullptr);
    calculator.reset(new TabletMemoryCalculator(tabletWriter2, readerContainer));

    EXPECT_EQ(3 + 4, calculator->GetDumpingSegmentMemsize());
    EXPECT_EQ(3 + 4 + 10, calculator->GetRtIndexMemsize());
}

TEST_F(TabletMemoryCalculatorTest, testRtIndexMemsize)
{
    /**
        version.0 {inc segment: 0, 1} {rt segment: ...824}
        version.1 {inc segment: 0, 1, 2} {rt segment: ...824, ...825}
        result is {824, 825} => (4 + 5 = 9)
     **/
    std::shared_ptr<TabletReaderContainer> readerContainer(new TabletReaderContainer("name"));
    std::shared_ptr<ITabletReader> reader;
    segmentid_t segmentId = Segment::MERGED_SEGMENT_ID_MASK;
    SegmentMeta segMeta1(segmentId);
    auto incDiskSegment1 = std::make_shared<MockDiskSegment>(segMeta1);

    ++segmentId;
    SegmentMeta segMeta2(segmentId);
    auto incDiskSegment2 = std::make_shared<MockDiskSegment>(segMeta2);

    ++segmentId;
    SegmentMeta segMeta3(segmentId);
    auto incDiskSegment3 = std::make_shared<MockDiskSegment>(segMeta3);

    segmentId = Segment::RT_SEGMENT_ID_MASK;
    SegmentMeta segMeta4(segmentId);
    auto dumpingSegment1 = std::make_shared<MockMemSegment>(segMeta4, Segment::SegmentStatus::ST_DUMPING);
    ++segmentId;
    SegmentMeta segMeta5(segmentId);
    auto dumpingSegment2 = std::make_shared<MockMemSegment>(segMeta5, Segment::SegmentStatus::ST_DUMPING);

    ++segmentId;
    SegmentMeta segMeta6(segmentId);
    auto rtDiskSegment1 = std::make_shared<MockDiskSegment>(segMeta6);

    ++segmentId;
    SegmentMeta segMeta7(segmentId);
    auto rtDiskSegment2 = std::make_shared<MockDiskSegment>(segMeta7);

    Version version0(/*vid*/ 0, /*ts*/ 0);
    version0.AddSegment(segMeta1.segmentId);
    version0.AddSegment(segMeta2.segmentId);
    version0.AddSegment(segMeta6.segmentId);

    std::vector<std::shared_ptr<Segment>> segments {incDiskSegment1, incDiskSegment2, rtDiskSegment1, dumpingSegment1};
    auto tabletData1 = std::make_shared<TabletData>(/*empty name*/ "");
    ASSERT_TRUE(tabletData1->Init(version0, segments, std::make_shared<ResourceMap>()).IsOK());

    auto tabletWriter1 = std::make_shared<MockTabletWriter>();
    auto tabletWriter2 = std::make_shared<MockTabletWriter>();

    readerContainer->AddTabletReader(tabletData1, reader, /*version deploy description*/ nullptr);
    auto calculator = std::make_unique<TabletMemoryCalculator>(tabletWriter1, readerContainer);

    EXPECT_CALL(*incDiskSegment1, EvaluateCurrentMemUsed()).WillRepeatedly(Return(1));
    EXPECT_CALL(*incDiskSegment2, EvaluateCurrentMemUsed()).WillRepeatedly(Return(2));
    EXPECT_CALL(*incDiskSegment3, EvaluateCurrentMemUsed()).WillRepeatedly(Return(3));
    EXPECT_CALL(*rtDiskSegment1, EvaluateCurrentMemUsed()).WillRepeatedly(Return(4));
    EXPECT_CALL(*rtDiskSegment2, EvaluateCurrentMemUsed()).WillRepeatedly(Return(5));
    EXPECT_CALL(*dumpingSegment1, EvaluateCurrentMemUsed()).WillRepeatedly(Return(6));
    EXPECT_CALL(*dumpingSegment2, EvaluateCurrentMemUsed()).WillRepeatedly(Return(7));
    EXPECT_CALL(*tabletWriter1, GetTotalMemSize()).WillRepeatedly(Return(10));
    EXPECT_CALL(*tabletWriter2, GetTotalMemSize()).WillRepeatedly(Return(20));

    EXPECT_EQ(4 + 6 + 10, calculator->GetRtIndexMemsize());

    Version version1(/*vid*/ 1, /*ts*/ 0);
    version1.AddSegment(segMeta1.segmentId);
    version1.AddSegment(segMeta2.segmentId);
    version1.AddSegment(segMeta3.segmentId);
    std::vector<std::shared_ptr<Segment>> segments1 {incDiskSegment1, incDiskSegment2, incDiskSegment3, rtDiskSegment1,
                                                     rtDiskSegment2,  dumpingSegment1, dumpingSegment2};

    auto tabletData2 = std::make_shared<TabletData>(/*empty name*/ "");
    ASSERT_TRUE(tabletData2->Init(version1, segments1, std::make_shared<ResourceMap>()).IsOK());

    readerContainer->AddTabletReader(tabletData2, reader, /*version deploy description*/ nullptr);
    calculator.reset(new TabletMemoryCalculator(tabletWriter2, readerContainer));

    EXPECT_EQ(4 + 5 + 6 + 7 + 20, calculator->GetRtIndexMemsize());
}
} // namespace indexlibv2::framework
