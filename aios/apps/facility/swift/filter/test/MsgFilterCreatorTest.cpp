#include "swift/filter/MsgFilterCreator.h"

#include <cstddef>
#include <string>
#include <vector>

#include "swift/common/Common.h"
#include "swift/common/FieldGroupReader.h"
#include "swift/common/FieldGroupWriter.h"
#include "swift/filter/AndMsgFilter.h"
#include "swift/filter/ContainMsgFilter.h"
#include "swift/filter/InMsgFilter.h"
#include "swift/filter/MsgFilter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::common;

namespace swift {
namespace filter {

class MsgFilterCreatorTest : public TESTBASE {};

TEST_F(MsgFilterCreatorTest, testCreateContainMsgFilter) {
    {
        string desc = "name CONTAIN a|b";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter != NULL);
        delete filter;
    }
    {
        string desc = " name     CONTAIN     a|b";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter != NULL);
        delete filter;
    }
    {
        string desc = "adgr?dataType CONTAIN |300||20";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter != NULL);
        ContainMsgFilter *containFilter = dynamic_cast<ContainMsgFilter *>(filter);
        EXPECT_TRUE(containFilter != NULL);
        EXPECT_EQ(string("adgr?dataType"), containFilter->getFieldName());
        EXPECT_EQ(string("|300||20"), containFilter->getValueSearchStr());
        EXPECT_EQ(2, containFilter->getValuesSearch().size());
        EXPECT_EQ(string("300"), containFilter->getValuesSearch()[0]);
        EXPECT_EQ(string("20"), containFilter->getValuesSearch()[1]);
        delete filter;
    }
    {
        string desc = "name NOTCONTAIN a|b";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter == NULL);
    }
    {
        string desc = " CONTAIN ";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter == NULL);
    }
    {
        string desc = " CONTAIN a|b";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter == NULL);
    }
    {
        string desc = "name CONTAIN ";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter == NULL);
    }
}

TEST_F(MsgFilterCreatorTest, testCreateInMsgFilter) {
    {
        string desc = "name IN a|b";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter != NULL);
        delete filter;
    }
    {
        string desc = " name     IN     a|b";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter != NULL);
        delete filter;
    }
    {
        string desc = "adgr?dataType IN 300";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter != NULL);
        InMsgFilter *inFilter = dynamic_cast<InMsgFilter *>(filter);
        EXPECT_TRUE(inFilter != NULL);
        EXPECT_EQ(string("adgr?dataType"), inFilter->getFieldName());
        delete filter;
    }
    {
        string desc = "name NOTIN a|b";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter == NULL);
    }
    {
        string desc = " IN ";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter == NULL);
    }
    {
        string desc = " IN a|b";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter == NULL);
    }
    {
        string desc = "name IN ";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter == NULL);
    }
}

TEST_F(MsgFilterCreatorTest, testCreateAndMsgFilter) {
    FieldGroupWriter fieldGroupWriter;
    string name1 = string("name");
    string value1 = string("a b c");
    fieldGroupWriter.addProductionField(name1, value1, true);
    string name2 = string("type");
    string value2 = string("1 3 5");
    fieldGroupWriter.addProductionField(name2, value2, false);
    string data;
    fieldGroupWriter.toString(data);
    FieldGroupReader fieldGroupReader;
    EXPECT_TRUE(fieldGroupReader.fromProductionString(data));
    {
        string desc = "name IN a|b AND name IN a|c";
        AndMsgFilter *filter = MsgFilterCreator::createAndMsgFilter(desc);
        EXPECT_TRUE(filter);
        EXPECT_EQ((size_t)2, filter->_msgFilterVec.size());
        delete filter;
    }
    {
        string desc = "name CONTAIN a|b AND name IN a|c";
        AndMsgFilter *filter = MsgFilterCreator::createAndMsgFilter(desc);
        EXPECT_TRUE(filter);
        EXPECT_EQ((size_t)2, filter->_msgFilterVec.size());
        EXPECT_TRUE(dynamic_cast<const ContainMsgFilter *>(filter->_msgFilterVec[0]));
        EXPECT_TRUE(dynamic_cast<const InMsgFilter *>(filter->_msgFilterVec[1]));
        delete filter;
    }
    {
        string desc = "name CONTAIN a|d AND name CONTAIN f";
        AndMsgFilter *filter = MsgFilterCreator::createAndMsgFilter(desc);
        EXPECT_TRUE(filter);
        EXPECT_EQ((size_t)2, filter->_msgFilterVec.size());
        EXPECT_TRUE(dynamic_cast<const ContainMsgFilter *>(filter->_msgFilterVec[0]));
        EXPECT_TRUE(dynamic_cast<const ContainMsgFilter *>(filter->_msgFilterVec[1]));
        EXPECT_FALSE(filter->filterMsg(fieldGroupReader));
        delete filter;
    }
    {
        string desc = "name CONTAIN a|d AND type CONTAIN 3|4";
        AndMsgFilter *filter = MsgFilterCreator::createAndMsgFilter(desc);
        EXPECT_TRUE(filter);
        EXPECT_EQ((size_t)2, filter->_msgFilterVec.size());
        EXPECT_TRUE(dynamic_cast<const ContainMsgFilter *>(filter->_msgFilterVec[0]));
        EXPECT_TRUE(dynamic_cast<const ContainMsgFilter *>(filter->_msgFilterVec[1]));
        EXPECT_TRUE(filter->filterMsg(fieldGroupReader));
        delete filter;
    }
    {
        string desc = "name NOT a|b AND name IN a|c";
        AndMsgFilter *filter = MsgFilterCreator::createAndMsgFilter(desc);
        EXPECT_FALSE(filter);
    }
    {
        string desc = "name IN a|b AND name a|c";
        AndMsgFilter *filter = MsgFilterCreator::createAndMsgFilter(desc);
        EXPECT_FALSE(filter);
    }
    {
        string desc = "name IN a|b AND ";
        AndMsgFilter *filter = MsgFilterCreator::createAndMsgFilter(desc);
        EXPECT_TRUE(filter);
        EXPECT_EQ((size_t)1, filter->_msgFilterVec.size());
        delete filter;
    }
}

TEST_F(MsgFilterCreatorTest, testCreateMsgFilter) {
    {
        string desc = " ";
        AndMsgFilter *filter = MsgFilterCreator::createAndMsgFilter(desc);
        EXPECT_FALSE(filter);
    }
    {
        string desc = "name IN a|b AND age NOT-CONTAIN 10|11";
        AndMsgFilter *filter = MsgFilterCreator::createAndMsgFilter(desc);
        EXPECT_FALSE(filter);
    }
    {
        string desc = "name IN a|b";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter);
        EXPECT_TRUE(dynamic_cast<InMsgFilter *>(filter));
        delete filter;
    }
    {
        string desc = "name IN a|b AND name2 IN c";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter);
        EXPECT_TRUE(dynamic_cast<AndMsgFilter *>(filter));
        delete filter;
    }
    {
        string desc = "name CONTAIN a|b AND name IN a|c";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter);
        EXPECT_TRUE(dynamic_cast<AndMsgFilter *>(filter));
        EXPECT_EQ((size_t)2, dynamic_cast<AndMsgFilter *>(filter)->_msgFilterVec.size());
        EXPECT_TRUE(dynamic_cast<const ContainMsgFilter *>(dynamic_cast<AndMsgFilter *>(filter)->_msgFilterVec[0]));
        EXPECT_TRUE(dynamic_cast<const InMsgFilter *>(dynamic_cast<AndMsgFilter *>(filter)->_msgFilterVec[1]));
        delete filter;
    }
    {
        string desc = "name CONTAIN a|d AND name CONTAIN f";
        MsgFilter *filter = MsgFilterCreator::createMsgFilter(desc);
        EXPECT_TRUE(filter);
        EXPECT_TRUE(dynamic_cast<AndMsgFilter *>(filter));
        EXPECT_EQ((size_t)2, dynamic_cast<AndMsgFilter *>(filter)->_msgFilterVec.size());
        EXPECT_TRUE(dynamic_cast<const ContainMsgFilter *>(dynamic_cast<AndMsgFilter *>(filter)->_msgFilterVec[0]));
        EXPECT_TRUE(dynamic_cast<const ContainMsgFilter *>(dynamic_cast<AndMsgFilter *>(filter)->_msgFilterVec[1]));
        delete filter;
    }
}

} // namespace filter
} // namespace swift
