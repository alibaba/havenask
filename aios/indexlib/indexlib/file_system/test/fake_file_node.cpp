#include "indexlib/file_system/test/fake_file_node.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FakeFileNode);

FakeFileNode::FakeFileNode(FSFileType type)
    : mType(type)
{
}

FakeFileNode::~FakeFileNode() 
{
}

IE_NAMESPACE_END(file_system);

