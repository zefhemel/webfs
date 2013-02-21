import sys
sys.path.append("..")

import block_store
import os

def test_store(store):
    print "Put"
    pic = open("pic.jpg", "r")
    store.put("pic.jpg", pic)
    pic.close()
    print "Get"
    pic = open("pic2.jpg", "w")
    store.get("pic.jpg", pic)
    pic.close()
    assert os.path.getsize("pic.jpg") == os.path.getsize("pic2.jpg")
    print "Delete"
    store.delete("pic.jpg")
    print "Checking if pic exists"
    assert not store.has("pic.jpg")

print "File store"
test_store(block_store.FileStore("store"))
print "S3 store"
test_store(block_store.S3Store("webfs-test"))
