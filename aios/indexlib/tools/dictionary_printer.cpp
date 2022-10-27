#include <iostream>
#include <fstream>
#include <string>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

using namespace std;

static const string usage = "Usage: \n" \
  "\t -h, --help\t\tprint this help message.\n" \
  "\t -f, --file\t\tspecified dictionary index file.\n" \
  "\n -f, --num\t\tspecified top terms to dump.\n";

static const string errinfo = "parameter error, use -h for help";
static const uint32_t DICTIONARY_MAGIC_NUMBER = 0x98765432;
static const uint32_t ITEM_COUNT_PER_BLOCK = 128;

int main(int argc, char** argv) 
{

  char *fileName = NULL;
  uint32_t termNum = INT_MAX;
  if (argc == 1) {
    cout << usage << endl;
    return 0;
  }
  
  for (int i = 1; i < argc; ++i) {
    if (strcmp("-f", argv[i]) == 0 && ++i < argc) {
      fileName = argv[i];
      continue;
    }else if (strcmp("-n", argv[i]) == 0 && ++i < argc) {
      termNum = atoi(argv[i]);
      continue;
    } else if (strcmp("-h", argv[i]) == 0) {
      cout << usage << endl;
      return 0;
    } else {
      cout << errinfo << endl;
      return 0;
    }
  }

  char* addr = NULL;
  int fd = open(fileName, O_RDONLY, 0644);
       
  if (-1 == fd) {
    cerr << "open file " << fileName << " failed : " << strerror(errno) << endl;
    return 1;
  }

  struct stat st;
  if (fstat(fd, &st) == -1) {
    cerr << "get file " << fileName << " stat failed : " << strerror(errno) << endl;
    return 1;
  }

  addr = (char*)mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED) {
    cerr << "mmap file " << fileName << " failed : " << strerror(errno) << endl;
    return 1;
  }

  uint32_t* magicData = (uint32_t*)((uint8_t*)addr + st.st_size - sizeof(uint32_t));
  if (*magicData != DICTIONARY_MAGIC_NUMBER) {
    cerr <<  "Bad dictionary file, magic number doesn't match:" << fileName << endl;
    return 1;
  }
  uint32_t* blockNumData = magicData - 1;
  uint32_t mBlockCount = *blockNumData;
  uint64_t *end = (uint64_t*)((uint8_t*)blockNumData - sizeof(uint64_t) * mBlockCount);


  uint64_t *start = (uint64_t*)addr;
  uint32_t totalSize = (end - start) / 2;
  cout << "total term size : " << totalSize << " block size :" << mBlockCount << endl;
  if (start + termNum * 2 < end) {
    end = start + termNum * 2;
  }
  while (start < end) {
    cout << "key:" << *start << "\toffset:" << *(start+1) << endl;
    start += 2;
  }

  return 0;
}
