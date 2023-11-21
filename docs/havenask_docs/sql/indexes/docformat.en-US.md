---
toc: content
order: 3
---

# Format of a document that is pushed in real time
Document format example:
```
CMD=add^_
PK=12345321^_
url=http://www.aliyun.com/index.html^_
title=Alibaba Cloud Computing Co., Ltd.^_
body=xxxxxx xxx^_
time=3123423421^_
multi_value_field=1234^]324^]342^_
bidwords=mp3^\price=35.8^Ptime=13867236221^]mp4^\price=32.8^Ptime=13867236221^_
^^
CMD=delete^_
PK=12345321^_
^^
CMD=update_field^_
PK=12345321^_
time=3123423421^_
^^
```
The data file contains three commands: add, delete, and update_field. Each command consists of multiple lines, each of which is a key-value pair.
Commands are separated by `^^\n`. Each pair of key-value pairs is separated by `^_\n`. Multiple values are separated by `^]`.

- cmd: specifies how a document is processed.
   - Add: Add a document. If the document already exists, it will be deleted and then added.
   - update_field: updates a document. Currently, only attribute fields can be updated.
   - delete: deletes a document. To delete a document, you must specify the primary key of the document.

<div class="lake-content" typography="classic"><h2 id="zabBj" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">Precautions</span></h2><ul class="ne-ul" style="margin: 0; padding-left: 23px"><li id="u35b62be1"><span class="ne-text">Do not use built-in characters in the engine. The following table describes the built-in characters.</span></li></ul>

Encoding | Display form in (emacs/vi) | Input method in emacs | Input method in vi
-- | -- | -- | --
"\x1E\n" | ^ ^ (followed by line feed) | C- q C- 6 | C- v C- 6
"\x1F\n" | ^_(followed by a line feed) | C- q C- 7 | C- v C- 7
"\x1D" | ^] | C-q C-5 | C-v C-5
"\x1C" | ^\ | C-q C-4 | C-v C-4
"\x03" | ^C | C-q C-c | C-v C-c

<span class="ne-text">Do not use the '\x1D' ('^]') delimiter in fields of the TEXT type. </span></li></ul></div>
