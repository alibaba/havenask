#include "swift/filter/AndMsgFilter.h"

#include <stddef.h>
#include <vector>

#include "swift/common/Common.h"
#include "swift/common/FieldGroupReader.h"
#include "swift/filter/MsgFilter.h"
#include "unittest/unittest.h"

using namespace swift::common;

namespace swift {
namespace filter {

class FakeMsgFilter : public MsgFilter {
public:
    FakeMsgFilter(bool filterMsg) : _filterMsg(filterMsg) {}
    ~FakeMsgFilter() {}
    bool init() override { return true; }
    bool filterMsg(const FieldGroupReader &fieldGroupReader) const override {
        (void)fieldGroupReader;
        return _filterMsg;
    }

private:
    bool _filterMsg;
};

class AndMsgFilterTest : public TESTBASE {};

TEST_F(AndMsgFilterTest, testFilterMsg) {
    {
        AndMsgFilter filter;
        filter.addMsgFilter(NULL);
        filter.addMsgFilter(new FakeMsgFilter(true));
        filter.addMsgFilter(new FakeMsgFilter(true));
        filter.addMsgFilter(new FakeMsgFilter(true));
        filter.addMsgFilter(new FakeMsgFilter(true));
        filter.addMsgFilter(new FakeMsgFilter(false));

        EXPECT_EQ((size_t)5, filter._msgFilterVec.size());
        FieldGroupReader fieldGroupReader;
        EXPECT_FALSE(filter.filterMsg(fieldGroupReader));
    }
    {
        AndMsgFilter filter;
        filter.addMsgFilter(new FakeMsgFilter(false));
        filter.addMsgFilter(new FakeMsgFilter(true));

        EXPECT_EQ((size_t)2, filter._msgFilterVec.size());
        FieldGroupReader fieldGroupReader;
        EXPECT_FALSE(filter.filterMsg(fieldGroupReader));
    }
}

} // namespace filter
} // namespace swift
