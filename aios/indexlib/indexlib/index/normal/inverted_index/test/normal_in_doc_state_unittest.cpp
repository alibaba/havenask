#include "indexlib/index/normal/inverted_index/test/normal_in_doc_state_unittest.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, NormalInDocStateTest);

NormalInDocStateTest::NormalInDocStateTest()
{
}

NormalInDocStateTest::~NormalInDocStateTest()
{
}

void NormalInDocStateTest::SetUp()
{
}

void NormalInDocStateTest::TearDown()
{
}

void NormalInDocStateTest::TestSetSectionReader()
{
    NormalInDocState normalState;
    SectionAttributeReaderImpl sectionReader;
    normalState.SetSectionReader(&sectionReader);
    ASSERT_EQ(normalState.GetSectionReader(), &sectionReader);
}

void NormalInDocStateTest::TestAlignSize()
{
    ASSERT_EQ((size_t)64, sizeof(NormalInDocState));
}

void NormalInDocStateTest::TestCopy()
{
    NormalInDocState state;
    SectionAttributeReaderImpl sectionReader;
    state.SetSectionReader(&sectionReader);
    util::ObjectPool<NormalInDocState> owner;
    state.SetOwner(&owner);

    NormalInDocState copiedState(state);
    ASSERT_EQ(state.mHasSectionReader, copiedState.mHasSectionReader);
    ASSERT_EQ(state.mData.sectionReader, copiedState.mData.sectionReader);
    ASSERT_EQ(state.mOwner, copiedState.mOwner);
}

IE_NAMESPACE_END(index);

