import random
from collections import OrderedDict

class DocSet:
    def __init__(self, filename, pk_field, max_pk, docs):
        self.docs = docs
        self.max_pk = max_pk
        self.pk_field = pk_field
        self.doc_sep = "\x1e"
        self.field_sep = "\x1f"
        self.filename = filename
        self.new_filename = filename + ".big"
        
    
    @classmethod
    def parse(self, filename, pk_field):
        with open(filename) as f:
            lines = f.readlines()
            docs = []
            max_pk = -1
            cur_doc = OrderedDict({})
            for index, line in enumerate(lines):
                if line.find("=")!=-1:
                    key, value = line[:line.find("=")], line[line.find("=")+1:]
                    cur_doc[key] = value
                    if key == pk_field:
                        max_pk = max(max_pk, value[:len(value)-2])
                else:
                    docs.append(cur_doc)
                    cur_doc = OrderedDict({})

            return DocSet(filename, pk_field, max_pk, docs)
    
    def write(self):
        with open(self.new_filename, "w") as f:
            for doc in self.docs:
                for k in doc:
                    f.write("%s=%s"%(k, doc[k]))
                f.write(self.doc_sep+"\n")
        
    
    def expandTo(self, size):
        cur_size = len(self.docs)
        if size <= cur_size:
            return 
        diff_size = size - cur_size
        print("diff size %d"%(diff_size))
        count = 0
        while(count < diff_size):
            count += 1
            new_pk = int(self.max_pk) + count
            choose_index = random.randint(0, cur_size-1)
            choose_doc = self.docs[choose_index]
            new_doc = OrderedDict({})
            for k in choose_doc:
                if k == self.pk_field:
                    new_doc[self.pk_field] = str(new_pk) + self.field_sep + "\n"
                else:
                    new_doc[k] = choose_doc[k]
            self.docs.append(new_doc)

    
        
if __name__ == "__main__":
    import sys
    filename = sys.argv[1]
    pk_field = sys.argv[2]
    expand_size = int(sys.argv[3])
    doc_set = DocSet.parse(filename, pk_field)
    doc_set.expandTo(expand_size)
    doc_set.write()
        
            
        