#include "matchdoc/Trait.h"

#include "gtest/gtest.h"
#include <stdint.h>
#include <string>
#include <type_traits>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/MultiValueType.h"
#include "matchdoc/MatchDoc.h"

using namespace std;
class TraitTest : public testing::Test {};

namespace matchdoc {
struct Unserializable {
    std::string a;
};

struct Serializable {
    std::string a;
    void serialize(autil::DataBuffer &dataBuffer) { dataBuffer.write(a); }
};

} // namespace matchdoc

class UD2 {};

using namespace matchdoc;

TEST_F(TraitTest, testNeedConstructDestruct) {
#define CD_ASSERT(type, needConstruct, needDestruct)                                                                   \
    ASSERT_EQ(needConstruct, ConstructTypeTraits<type>::NeedConstruct());                                              \
    ASSERT_EQ(needDestruct, DestructTypeTraits<type>::NeedDestruct());

    CD_ASSERT(int8_t, false, false);
    CD_ASSERT(int16_t, false, false);
    CD_ASSERT(int32_t, false, false);
    CD_ASSERT(int64_t, false, false);
    CD_ASSERT(uint8_t, false, false);
    CD_ASSERT(uint16_t, false, false);
    CD_ASSERT(uint32_t, false, false);
    CD_ASSERT(uint64_t, false, false);
    CD_ASSERT(float, false, false);
    CD_ASSERT(double, false, false);
    CD_ASSERT(bool, false, false);

    CD_ASSERT(autil::MultiChar, true, false);
    CD_ASSERT(autil::MultiInt8, true, false);
    CD_ASSERT(autil::MultiUInt8, true, false);
    CD_ASSERT(autil::MultiInt16, true, false);
    CD_ASSERT(autil::MultiUInt16, true, false);
    CD_ASSERT(autil::MultiInt32, true, false);
    CD_ASSERT(autil::MultiUInt32, true, false);
    CD_ASSERT(autil::MultiInt64, true, false);
    CD_ASSERT(autil::MultiUInt64, true, false);
    CD_ASSERT(autil::MultiFloat, true, false);
    CD_ASSERT(autil::MultiDouble, true, false);
    CD_ASSERT(autil::MultiString, true, false);

    CD_ASSERT(UD2, false, false);
    CD_ASSERT(MatchDoc, true, false);
#undef CD_ASSERT
}
