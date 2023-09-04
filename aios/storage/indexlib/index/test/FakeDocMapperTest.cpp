#include "indexlib/index/test/FakeDocMapper.h"

#include "indexlib/base/Constant.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace index {

class FakeDocMapperTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, FakeDocMapperTest);

TEST_F(FakeDocMapperTest, testSimpleProcess)
{
    vector<SrcSegmentInfo> infos;
    infos.emplace_back(SrcSegmentInfo(0, 2, 0, {}));
    infos.emplace_back(SrcSegmentInfo(1, 2, 2, {}));
    FakeDocMapper mapper({}, {});
    mapper.Init(infos, 2);
    ASSERT_EQ(4, mapper.GetNewDocCount());
    for (int i = 0; i < 4; ++i) {
        docid_t oldDocId = i;
        ASSERT_EQ(oldDocId, mapper.GetNewId(oldDocId));
        auto [newSegId, newLocalId] = mapper.Map(oldDocId);
        ASSERT_EQ(2, newSegId);
        ASSERT_EQ(oldDocId, newLocalId);
    }
    for (int i = 0; i < 4; ++i) {
        docid_t newDocId = i;
        auto [oldSegId, oldLocalId] = mapper.ReverseMap(newDocId);
        if (newDocId < 2) {
            ASSERT_EQ(0, oldSegId);
            ASSERT_EQ(newDocId, oldLocalId);
        } else {
            ASSERT_EQ(1, oldSegId);
            ASSERT_EQ(newDocId - 2, oldLocalId);
        }
    }
}

TEST_F(FakeDocMapperTest, testDeleteDoc)
{
    vector<SrcSegmentInfo> infos;
    infos.emplace_back(SrcSegmentInfo(0, 2, 0, {0}));
    infos.emplace_back(SrcSegmentInfo(1, 2, 2, {1}));
    FakeDocMapper mapper({}, {});
    mapper.Init(infos, 2);
    ASSERT_EQ(2, mapper.GetNewDocCount());
    ASSERT_EQ(INVALID_DOCID, mapper.GetNewId(0));
    ASSERT_EQ(0, mapper.GetNewId(1));
    ASSERT_EQ(1, mapper.GetNewId(2));
    ASSERT_EQ(INVALID_DOCID, mapper.GetNewId(3));
    {
        auto [newSegId, newLocalId] = mapper.Map(0);
        ASSERT_EQ(INVALID_SEGMENTID, newSegId);
        ASSERT_EQ(INVALID_DOCID, newLocalId);
    }
    {
        auto [newSegId, newLocalId] = mapper.Map(1);
        ASSERT_EQ(2, newSegId);
        ASSERT_EQ(0, newLocalId);
    }
    {
        auto [newSegId, newLocalId] = mapper.Map(2);
        ASSERT_EQ(2, newSegId);
        ASSERT_EQ(1, newLocalId);
    }
    {
        auto [newSegId, newLocalId] = mapper.Map(4);
        ASSERT_EQ(INVALID_SEGMENTID, newSegId);
        ASSERT_EQ(INVALID_DOCID, newLocalId);
    }
    {
        auto [oldSegId, oldLocalId] = mapper.ReverseMap(0);
        ASSERT_EQ(0, oldSegId);
        ASSERT_EQ(1, oldLocalId);
    }
    {
        auto [oldSegId, oldLocalId] = mapper.ReverseMap(1);
        ASSERT_EQ(1, oldSegId);
        ASSERT_EQ(0, oldLocalId);
    }
}

TEST_F(FakeDocMapperTest, testSplit)
{
    {
        vector<SrcSegmentInfo> infos;
        infos.emplace_back(SrcSegmentInfo(0, 2, 0, {}));
        infos.emplace_back(SrcSegmentInfo(1, 2, 2, {}));
        FakeDocMapper mapper({}, {});
        mapper.Init(infos, 2, 2);
        ASSERT_EQ(4, mapper.GetNewDocCount());
        for (int i = 0; i < 4; ++i) {
            docid_t oldDocId = i;
            ASSERT_EQ(oldDocId, mapper.GetNewId(oldDocId));
            auto [newSegId, newLocalId] = mapper.Map(oldDocId);
            if (oldDocId < 2) {
                ASSERT_EQ(2, newSegId);
                ASSERT_EQ(oldDocId, newLocalId);
            } else {
                ASSERT_EQ(3, newSegId);
                ASSERT_EQ(oldDocId - 2, newLocalId);
            }
        }
        for (int i = 0; i < 4; ++i) {
            docid_t newDocId = i;
            auto [oldSegId, oldLocalId] = mapper.ReverseMap(newDocId);
            if (newDocId < 2) {
                ASSERT_EQ(0, oldSegId);
                ASSERT_EQ(newDocId, oldLocalId);
            } else {
                ASSERT_EQ(1, oldSegId);
                ASSERT_EQ(newDocId - 2, oldLocalId);
            }
        }
    }
    {
        vector<SrcSegmentInfo> infos;
        infos.emplace_back(SrcSegmentInfo(0, 2, 0, {}));
        infos.emplace_back(SrcSegmentInfo(1, 2, 2, {}));
        FakeDocMapper mapper({}, {});
        mapper.Init(infos, 2, 3);
        ASSERT_EQ(4, mapper.GetNewDocCount());
        for (int i = 0; i < 4; ++i) {
            docid_t oldDocId = i;
            ASSERT_EQ(oldDocId, mapper.GetNewId(oldDocId));
        }
        {
            auto [newSegId, newLocalId] = mapper.Map(0);
            ASSERT_EQ(2, newSegId);
            ASSERT_EQ(0, newLocalId);
        }
        {
            auto [newSegId, newLocalId] = mapper.Map(1);
            ASSERT_EQ(3, newSegId);
            ASSERT_EQ(0, newLocalId);
        }
        {
            auto [newSegId, newLocalId] = mapper.Map(2);
            ASSERT_EQ(4, newSegId);
            ASSERT_EQ(0, newLocalId);
        }
        {
            auto [newSegId, newLocalId] = mapper.Map(3);
            ASSERT_EQ(4, newSegId);
            ASSERT_EQ(1, newLocalId);
        }
        for (int i = 0; i < 4; ++i) {
            docid_t newDocId = i;
            auto [oldSegId, oldLocalId] = mapper.ReverseMap(newDocId);
            if (newDocId < 2) {
                ASSERT_EQ(0, oldSegId);
                ASSERT_EQ(newDocId, oldLocalId);
            } else {
                ASSERT_EQ(1, oldSegId);
                ASSERT_EQ(newDocId - 2, oldLocalId);
            }
        }
    }
}

TEST_F(FakeDocMapperTest, testRoundRobin)
{
    {
        vector<SrcSegmentInfo> infos;
        infos.emplace_back(SrcSegmentInfo(0, 2, 0, {}));
        infos.emplace_back(SrcSegmentInfo(1, 2, 2, {}));
        FakeDocMapper mapper({}, {});
        mapper.Init(infos, 2, 2, true);
        ASSERT_EQ(4, mapper.GetNewDocCount());
        ASSERT_EQ(0, mapper.GetNewId(0));
        ASSERT_EQ(2, mapper.GetNewId(1));
        ASSERT_EQ(1, mapper.GetNewId(2));
        ASSERT_EQ(3, mapper.GetNewId(3));
        {
            auto [newSegId, newLocalId] = mapper.Map(0);
            ASSERT_EQ(2, newSegId);
            ASSERT_EQ(0, newLocalId);
        }
        {
            auto [newSegId, newLocalId] = mapper.Map(1);
            ASSERT_EQ(3, newSegId);
            ASSERT_EQ(0, newLocalId);
        }
        {
            auto [newSegId, newLocalId] = mapper.Map(2);
            ASSERT_EQ(2, newSegId);
            ASSERT_EQ(1, newLocalId);
        }
        {
            auto [newSegId, newLocalId] = mapper.Map(3);
            ASSERT_EQ(3, newSegId);
            ASSERT_EQ(1, newLocalId);
        }
        {
            auto [oldSegId, oldLocalId] = mapper.ReverseMap(0);
            ASSERT_EQ(0, oldSegId);
            ASSERT_EQ(0, oldLocalId);
        }
        {
            auto [oldSegId, oldLocalId] = mapper.ReverseMap(1);
            ASSERT_EQ(1, oldSegId);
            ASSERT_EQ(0, oldLocalId);
        }
        {
            auto [oldSegId, oldLocalId] = mapper.ReverseMap(2);
            ASSERT_EQ(0, oldSegId);
            ASSERT_EQ(1, oldLocalId);
        }
        {
            auto [oldSegId, oldLocalId] = mapper.ReverseMap(3);
            ASSERT_EQ(1, oldSegId);
            ASSERT_EQ(1, oldLocalId);
        }
    }
    {
        vector<SrcSegmentInfo> infos;
        infos.emplace_back(SrcSegmentInfo(0, 2, 0, {1}));
        infos.emplace_back(SrcSegmentInfo(1, 2, 2, {0}));
        FakeDocMapper mapper({}, {});
        mapper.Init(infos, 2, 2, true);
        ASSERT_EQ(2, mapper.GetNewDocCount());
        ASSERT_EQ(0, mapper.GetNewId(0));
        ASSERT_EQ(INVALID_DOCID, mapper.GetNewId(1));
        ASSERT_EQ(INVALID_DOCID, mapper.GetNewId(2));
        ASSERT_EQ(1, mapper.GetNewId(3));
        {
            auto [newSegId, newLocalId] = mapper.Map(0);
            ASSERT_EQ(2, newSegId);
            ASSERT_EQ(0, newLocalId);
        }
        {
            auto [newSegId, newLocalId] = mapper.Map(1);
            ASSERT_EQ(INVALID_SEGMENTID, newSegId);
            ASSERT_EQ(INVALID_DOCID, newLocalId);
        }
        {
            auto [newSegId, newLocalId] = mapper.Map(2);
            ASSERT_EQ(INVALID_SEGMENTID, newSegId);
            ASSERT_EQ(INVALID_DOCID, newLocalId);
        }
        {
            auto [newSegId, newLocalId] = mapper.Map(3);
            ASSERT_EQ(3, newSegId);
            ASSERT_EQ(0, newLocalId);
        }
        {
            auto [oldSegId, oldLocalId] = mapper.ReverseMap(0);
            ASSERT_EQ(0, oldSegId);
            ASSERT_EQ(0, oldLocalId);
        }
        {
            auto [oldSegId, oldLocalId] = mapper.ReverseMap(1);
            ASSERT_EQ(1, oldSegId);
            ASSERT_EQ(1, oldLocalId);
        }
    }
}

}} // namespace indexlibv2::index
