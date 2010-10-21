REGISTER /Users/cwilkes/.m2/repository/org/seattlehadoop/demo/sh-demo-pigloadfunc/1.0-SNAPSHOT/sh-demo-pigloadfunc-1.0-SNAPSHOT.jar
userpurchases = LOAD 'purchases.json' USING org.seattlehadoop.demo.pig.loadfunc.JsonLoadFunc('userId', 'itemId', 'price') as (id : long , itemid : long, price : float);
costs         = LOAD 'prices.json'    USING org.seattlehadoop.demo.pig.loadfunc.JsonLoadFunc('itemId', 'cost')            as (itemid : long, cost : float);
profit1 = JOIN userpurchases by itemid, costs by itemid;
profit = FOREACH profit1 GENERATE userpurchases::id, userpurchases::itemid, costs::itemid - userpurchases::price;
STORE profit INTO '$out';
