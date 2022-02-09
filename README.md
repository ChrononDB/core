# What is this

**ChrononDB** is a tiny in-memory fast timeseries DB. Something like Memcache, but for **timeseries** and **embedded** for now.

API have next methods
- PUT(id, TTL, payload)
- GET(timeFrom, timeTo)
- GET(id)
- REMOVE(id)
- FLUSH(timeFrom, timeTo)
- VACUUM

DB doesn't provide guarantees like relational databases, but most of the operations are non-blocking which give us pretty good performance in multi-threading environment.

I've seen about **2.5M TPS** for read/write of a single item in a single thread. It can scan around **34M records/sec** and read around **17M records / sec** in my tests.

Multi-threading performance see below in [**Performance**](https://github.com/ChrononDB/core#performance) section.

See [**ChrononDB.com**](https://chronondb.com) site, it will be updated in a week or so with more docs and guides.

**Use at your own risk, it's ALFA**

# Where to get

See [Releases](https://github.com/ChrononDB/core/releases/). v1.0.0 just published. Or build on your own from [**master ZIP**](https://github.com/ChrononDB/core/archive/refs/heads/master.zip)

# What's here

- Records Log
- Indexes
- Tombstones
- Garbage collector
- Non-blocking ops except PUT at the edge of the block rotation. Nevertheless it just blocks other PUTs only for a sub-millisec.

# Readiness

**CONS**
- Checkstyle ignored
- No JARs in Maven Central Repository yet, working on it.
- Unit tests are long

**PROS**
- PMD, SpotBugs added
- Build is good and test coverage is ok
- JARs/sources/docs generated
- **BUT CODE WORKS JUST FINE**

See [**Performance**](https://github.com/ChrononDB/core#performance), but IMHO it is ready as **in-memory embedded**.

# Caveats

- No persistence yet
- No garbage collector daemon yet
- Garbage collector is dumb as hell and need to rely on internal statistics and be tunable
- No clustering yet
- No REST API or any form of RMI 
- No stand-alone version yet, just embedded
- Garbage collector is a big deal. I have an idea how to use off-heap to keep this little gremlin under control.
- No partitioning in any form
- Tests coverage still not the best

# History

**January 30, 2022**

We all do mistakes. This particular DB is a result of a little implementation request I misunderstood. Database happens. 

Somebody distracted me, I missed a few important points and decided that functionality requested is very similar to what is usually provided by a regular timeseries DB, but I need something small and fast.

Since I prefer to write a code and documentation rather than read it, I decided to create my own tiny implementation in a few hours rather than read documentation of some existing one.

So, once I finished coding / documenting all the stuff, I realized that I missed these points and , actually, everything is **much-much** more simpler than I expected. So, I decided to open-source everything I did.

Initial version was created in 8 hours, docs, tests and site took another 8 hours. At least, this was a funny weekend.

# Documentation

See [sources](https://github.com/ChrononDB/core) + [design notes](https://github.com/ChrononDB/core/blob/master/docs/ChrononDB%20Notes.pdf). *Yes-yes, my English is terrible sometimes, I know.*

# Performance

## Multi-threaded

I believe something wrong with my test itself, (see [LogTest_Performance_MT](https://github.com/ChrononDB/core/blob/master/src/test/java/com/chronondb/core/memstore/LogTest_Performance_MT.java)), cause DB designed in a way to be not-sensitive to multi-threading at all, but I see clear TPS degradation. 

It is still quite high, but I want to investigate this, I believe test itself is a limiting factor and I would expect performance close to single-threading. 

>**Threads: 200**, chunk size is 10,000 records   
**WRITE** is **76,064 TPS**    
**GET** is **38,372 TPS**   
**RANGE GET** is **181 ms avg.** with **195156 records per sec.**    
**REMOVE** is **55,064 TPS**    
**VACUUM** did 225 operations in 0 ms, collected 0 blocks   

>**Threads: 100**, chunk size is 10,000 records  
**WRITE** is **94,624 TPS**  
**GET** is **180,802 TPS**    
**RANGE GET** is **48 ms avg.** with **475,171 records per sec.**    
**REMOVE** is **204,821 TPMS**   
**VACUUM** did 288 operations in 0 ms, collected 6 blocks   

>**Threads: 10**, chunk size is 10,000 records
**WRITE** is **2,450,549 TPS**    
**GET** is **4,137,291 TPS**   
**RANGE GET** is 2 ms avg. with **6,512,401 records per sec.**    
**REMOVE** is **4,288,461 TPMS**  
**VACUUM** did 223 operations in 0 ms, collected 0 blocks   

>**Threads: 1**, chunk size is 10,000 records    
**WRITE** is **2,053,571 TPS**    
**GET** is **3,150,684 TPS**   
**RANGE GET** is 2 ms avg. with **4,509,803 records per sec.**    
**REMOVE** is **3,484,848 TPMS**  
**VACUUM** did 23 operations in 0 ms, collected 0 blocks    

## Single-threaded

**Note**: **GET RANGE** performance highly depends on VACUUM

>**REMOVE** is **2,000,000 TPS**  
**REMOVE** is **5,595,970 TPS**  
**REMOVE** is **8,305,647 TPS**  
**REMOVE** is **10,752,688 TPS** 

>**PUT** is **4,319,654 TPS**    
**PUT** is **4,854,368 TPS**  
**PUT** is **4,933,399 TPS**     
**PUT** is **4,940,711 TPS** 


>**GET RANGE**, Records scanned / sec 12,019,000    
**GET RANGE**, Records found / sec 12,019,000

>**GET RANGE**, Records scanned / ms 256,410,000    
**GET RANGE**, Records found / sec 0

>**GET RANGE**, Records scanned / sec 322,580,000   
**GET RANGE**, Records found / sec 0

>**GET RANGE**, Records scanned / sec 4,484,000     
**GET RANGE**, Records found / sec 4,484,000

>**GET RANGE**, Records scanned / sec 500,000,000    
**GET RANGE**, Records found / sec 0

>**GET RANGE**, Records scanned / sec 7,939,000     
**GET RANGE**, Records found / sec 7,939,000

>**GET RANGE**, Records scanned / sec 44,742,000    
**GET RANGE**, Records found / sec 22,371,000

>**GET RANGE**, Records scanned / sec 8,628,000     
**GET RANGE**, Records found / sec 8,628,000


# Future

See [design notes](https://github.com/ChrononDB/core/blob/master/docs/ChrononDB%20Notes.pdf) documentation in general, this is the roadmap.

I want to fix [**Caveats**](https://github.com/ChrononDB/core#caveats) listed above first and resolve everything from [**Readiness**](https://github.com/ChrononDB/core#readiness) **_CONS_**. Then I'll think about query planner and coordinator and complex queries.

# License

GPL v3, if you need something else - let me know.