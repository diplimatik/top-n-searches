Initial requirement of the task is to implement the system to perform heavy laoded tasks on reading top N popular searches. Number of clients in WB is huge so my design focuses on using golangs atomics and Sliding window algorithm (the idea is to make buckets responsible for each second of 5 mins span that gonna content ... )

# Backend Task Top-N Searches
### Arslanaliev Agai

# 1.Local Setup:
to run the docker image simply enter:
```
docker-compose up -d --build
```
api enpoints are:
> ```/api/trends?limit=nTopRequests``` *GET*
 
> ```/api/stoplist``` *DELETE* or *POST*
> 
> with body format: ```'{"word": "badword"}'```

> ```/metrics``` *GET*


# 2. Kafka Payload

```aiignore
{
"query": "чехол для iphone",
"user_id": "a1b2c3d4-e5f6",
"timestamp": 1709215000
}
```

\- *query* used for storing search itself  
\- *timestamp* & *user_id* used to avoid parsers and viewboters anomaly  

# 3. Describing App Architecture
Kafka was used:  
--to store logs for other services  
--to be ultrafast on heavy loads  
Atomic Pointers were used:  
--to cached top list without blocking. Performes the operation in O(1)   
--engine has RWMutexes on reading stopwords coz we aren't necessarely modifying it on every queue

# 4.Tradeoffs and business logic
-- Sliding window alggorithm was used instead of on fly calculation to avoid program crashing on high load.
Instead it is calculated each second using buckets to partition large data. ALso we save up on memory coz 
we wipe data from previous 5mins buckets and write to it  
-- business logic is fairly simple and i would've drawbn schema for it but i ain't got time so imma explain it 
on tech review


## 4.1 Problems resolved
### Marketing team
asked for word filtering so they got one (Optional requirement was resolved aswell and bad words update 
withotu restarting service)

### Analytics team
anomaleis were avoided by limiting each user to give out the analytics data only once per minute 
(which is realistically how people be using WB)





