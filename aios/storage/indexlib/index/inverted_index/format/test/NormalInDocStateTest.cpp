#include "indexlib/index/inverted_index/format/NormalInDocState.h"

#include "unittest/unittest.h"

namespace indexlib::index {

class NormalInDocStateTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

// TEST_F(NormalInDocStateTest, testSetSectionReader)
// {
//     NormalInDocState normalState;
//     SectionAttributeReaderImpl sectionReader;
//     normalState.SetSectionReader(&sectionReader);
//     ASSERT_EQ(normalState.GetSectionReader(), &sectionReader);
// }

TEST_F(NormalInDocStateTest, testAlignSize) { ASSERT_EQ((size_t)72, sizeof(NormalInDocState)); }

// TEST_F(NormalInDocStateTest, testCopy)
// {
//     NormalInDocState state;
//     SectionAttributeReaderImpl sectionReader;
//     state.SetSectionReader(&sectionReader);
//     util::ObjectPool<NormalInDocState> owner;
//     state.SetOwner(&owner);

//     NormalInDocState copiedState(state);
//     ASSERT_EQ(state._hasSectionReader, copiedState._hasSectionReader);
//     ASSERT_EQ(state._data.sectionReader, copiedState._data.sectionReader);
//     ASSERT_EQ(state._owner, copiedState._owner);
// }

} // namespace indexlib::index
