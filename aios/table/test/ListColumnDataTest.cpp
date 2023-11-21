#include "table/ListColumnData.h"

#include "unittest/unittest.h"

namespace table {

class ListColumnDataTest : public TESTBASE {};

TEST_F(ListColumnDataTest, testNonMultiString) {
    auto data =
        std::make_unique<ListColumnData<autil::MultiValueType<uint32_t>>>(std::make_shared<autil::mem_pool::Pool>());
    data->resize(32);

    std::vector<uint32_t> dataVec;
    data->set(0, dataVec.data(), dataVec.size());
    auto v = data->get(0);
    ASSERT_EQ(0, v.size());

    dataVec.push_back(1);
    data->set(1, dataVec.data(), dataVec.size());
    v = data->get(1);
    ASSERT_EQ(1, v.size());
    ASSERT_EQ(1, v[0]);
    ASSERT_EQ(1, data->_dataHolder->_lastChunkPos);

    dataVec.push_back(2);
    dataVec.push_back(3);
    data->set(2, dataVec.data(), dataVec.size());
    v = data->get(2);
    ASSERT_EQ(3, v.size());
    ASSERT_EQ(1, v[0]);
    ASSERT_EQ(2, v[1]);
    ASSERT_EQ(3, v[2]);
    ASSERT_EQ(4, data->_dataHolder->_lastChunkPos);
}

TEST_F(ListColumnDataTest, testMultiString) {
    auto data = std::make_unique<ListColumnData<autil::MultiString>>(std::make_shared<autil::mem_pool::Pool>());
    data->resize(20);

    std::vector<std::string> dataVec;
    data->set(0, dataVec.data(), dataVec.size());
    auto v = data->get(0);
    ASSERT_EQ(0, v.size());
    ASSERT_EQ(2, data->_dataHolder->_lastChunkPos);

    dataVec.emplace_back("abc");
    data->set(1, dataVec.data(), dataVec.size());
    v = data->get(1);
    ASSERT_EQ(1, v.size());
    ASSERT_TRUE(v[0] == std::string("abc"));
    ASSERT_EQ(2 + v.length(), data->_dataHolder->_lastChunkPos);
}

} // namespace table
