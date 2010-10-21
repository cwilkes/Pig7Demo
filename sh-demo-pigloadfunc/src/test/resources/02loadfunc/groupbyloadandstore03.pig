REGISTER /Users/cwilkes/.m2/repository/org/seattlehadoop/demo/sh-demo-pigloadfunc/1.0-SNAPSHOT/sh-demo-pigloadfunc-1.0-SNAPSHOT.jar
userpurchases = LOAD 'purchases.json' USING org.seattlehadoop.demo.pig.loadfunc.JsonWithPushDownLoadFunc() as (userId : long , itemId : long, price : float);
costs         = LOAD 'prices.json'    USING org.seattlehadoop.demo.pig.loadfunc.JsonLoadFunc('itemId', 'cost')            as (itemId : long, cost : float);
profit1 = JOIN userpurchases by itemId, costs by itemId;
profit = FOREACH profit1 GENERATE userpurchases::userId, userpurchases::itemId, costs::itemId - userpurchases::price;
foo = GROUP profit BY userpurchases::userId;
describe foo;
STORE foo INTO '$out' USING org.seattlehadoop.demo.pig.loadfunc.BagJsonStoreFunc('userID', 'purchases', '', 'gameID', 'profit');
