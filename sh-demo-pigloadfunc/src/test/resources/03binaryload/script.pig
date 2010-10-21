REGISTER /Users/cwilkes/.m2/repository/org/seattlehadoop/demo/sh-demo-pigloadfunc/1.0-SNAPSHOT/sh-demo-pigloadfunc-1.0-SNAPSHOT.jar
rawimages = LOAD '$in' USING org.seattlehadoop.demo.pig.loadfunc.ImageSequenceFileLoadFunc() as (fileName: chararray, bytes: bytearray);
onlyblackholes = FILTER rawimages BY  org.seattlehadoop.demo.pig.loadfunc.ExifFilter($0,$1);
STORE onlyblackholes INTO '$out' USING  org.seattlehadoop.demo.pig.loadfunc.ImageStoreFunc;
